use std::borrow::Cow;
use std::collections::HashSet;

use serde::{Deserialize, Serialize};

use crate::errors::*;
use crate::sql::engine::{Row, Transaction};
use crate::sql::execution::Rows;
use crate::sql::expression::Expression;
use crate::sql::schema::Catalog;
use crate::sql::{Table, Value};
use crate::storage::kv;

/// 一个基于kv的mvcc存储引擎

#[derive(Clone)]
pub struct KV {
    /// The underlying key/value store
    pub(super) kv: kv::MVCC,
}

impl KV {
    /// new一个kv engine
    pub fn new(kv: kv::MVCC) -> Self {
        Self { kv }
    }

    /// 获得元数据
    pub fn get_metadata(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        self.kv.get_metadata(key)
    }

    /// 设置元数据
    pub fn set_metadata(&self, key: &[u8], value: Vec<u8>) -> Result<()> {
        self.kv.set_metadata(key, value)
    }
}

impl super::Engine for KV {
    type Transaction = KvTransaction;

    fn begin(&self, mode: super::Mode) -> Result<Self::Transaction> {
        Ok(Self::Transaction::new(self.kv.begin_with_mode(mode)?))
    }

    fn resume(&self, id: u64) -> Result<Self::Transaction> {
        Ok(Self::Transaction::new(self.kv.resume(id)?))
    }
}

/// An SQL transaction based on an MVCC key/value transaction
pub struct KvTransaction {
    txn: kv::mvcc::MvccTransaction,
}
impl KvTransaction {
    fn new(txn: kv::mvcc::MvccTransaction) -> Self {
        Self { txn }
    }
    /// 保存一个索引
    /// 表名+字段名称+字段值 组成key
    /// hashset为 value
    fn index_save(
        &mut self,
        table: &str,
        column: &str,
        index: &Value,
        values: HashSet<Value>,
    ) -> Result<()> {
        // 构建key
        let key = SqlKey::Index(table.into(), column.into(), Some(index.clone().into())).encode();
        // 设置value
        // 空了就删除，没空就设置
        if values.is_empty() {
            self.txn.delete(&key)
        } else {
            self.txn.set(&key, serialize(&values)?)
        }
    }
}

impl super::Transaction for KvTransaction {
    fn id(&self) -> u64 {
        self.txn.get_id()
    }

    fn mode(&self) -> super::Mode {
        self.txn.mode()
    }

    fn commit(self) -> Result<()> {
        self.txn.commit()
    }

    fn rollback(self) -> Result<()> {
        self.txn.rollback()
    }

    fn create(&mut self, table: &str, row: super::Row) -> Result<()> {
        let table = self.must_read_table(table)?;
        // 检查数据是否正常
        table.check_row(&row,self)?;
        // 查找主键
        let id = table.get_row_key(&row)?;
        self.txn.set(
            &SqlKey::Table(Some(table.name.clone().into())).encode(),
            serialize(&row)?,
        )?;
        // 设置索引
        for (index, column) in table.columns.iter().enumerate().filter(|(_, c)| c.index) {
            let mut entry = self.read_index(&table.name, &column.name, &row[index])?;
            entry.insert(id.clone());
            self.index_save(&table.name, &column.name, &row[index], entry)?;
        }
        Ok(())
    }

    fn delete(&mut self, table: &str, id: &Value) -> Result<()> {
        let table = self.must_read_table(table)?;

        let indexes: Vec<_> = table
            .columns
            .iter()
            .enumerate()
            .filter(|(_, e)| e.index)
            .collect();
        if !indexes.is_empty() {
            if let Some(row) = self.read(&table.name, id)? {
                for (i, column) in indexes {
                    let mut index = self.read_index(&table.name, &column.name, &row[i])?;
                    index.remove(id);
                    self.index_save(&table.name, &column.name, &row[i], index)?;
                }
            }
        }
        self.txn
            .delete(&SqlKey::Row(table.name.into(), Some(id.to_owned().into())).encode())
    }

    fn read(&self, table: &str, id: &Value) -> Result<Option<super::Row>> {
        let r = self
            .txn
            .get(&SqlKey::Row(table.into(), Some(id.clone().into())).encode())?;
        let r: Option<std::result::Result<Row, Error>> = r.map(|val| deserialize(&val));
        return r.transpose();
    }

    fn read_index(&self, table: &str, column: &str, value: &Value) -> Result<HashSet<Value>> {
        let r = self.txn.get(
            &SqlKey::Index(table.into(), column.into(), Some(value.clone().into())).encode(),
        )?;
        let r: Option<Result<HashSet<Value>>> = r.map(|entry| deserialize(&entry));
        r.unwrap_or_else(|| Ok(HashSet::new()))
    }

    fn scan(&self, table: &str, filter: Option<Expression>) -> Result<super::Rows> {
        let r = self
            .txn
            .scan_prefix(&SqlKey::Row(table.into(), None).encode())?;

        let r: Result<Rows> = r
            .map(|res| {
                let (_, r) = res?;
                deserialize(&r)
            })
            .collect();

        // 利用filter进行计算，计算结果是true说明可以展示该数据
        if let Some(filter) = filter {
            if let Ok(rows) = r {
                rows.into_iter()
                    .filter_map(|row| {
                        let rr = filter.evaluate(Some(&row));
                        match rr {
                            Ok(rr) => match rr {
                                Value::Bool(true) => Some(Ok(row)),
                                _ => None,
                            },
                            Err(err) => Some(Err(err)),
                        }
                    })
                    .collect()
            } else {
                r
            }
        } else {
            r
        }
    }

    fn scan_index(&self, table: &str, column: &str) -> Result<super::IndexScan> {
        let table = self.must_read_table(table)?;
        // 检查一下这个是不是索引字段
        table.get_column_index(column)?;

        let scan = self
            .txn
            .scan_prefix(&SqlKey::Index(table.name.clone().into(), column.into(), None).encode())?;

        scan.map(|r| -> Result<(Value, HashSet<Value>)> {
            let r = r?;
            let (key, set): (Value, HashSet<Value>) = (
                match SqlKey::decode(&r.0)? {
                    SqlKey::Index(_, _, key) => key
                        .ok_or(Error::Index("get none index_key".to_string()))?
                        .into_owned(),
                    k => return Err(Error::Index(format!("expect index SqlKey get {:?}", k))),
                },
                deserialize(&r.1)?,
            );
            Ok((key, set))
        })
        .collect()
    }

    fn update(&mut self, table: &str, id: &Value, row: super::Row) -> Result<()> {
        let table = self.must_read_table(table)?;

        // 检查一遍
        table.check_row(&row,self)?;

        // 如果是主键被更新了 那就要删除原数据 并创建一条新的数据
        // 但是这里有关问题 如果是连锁更新 比如 set id=id+1
        // 这就会导致 id+1 这个位置是有可能有数据的
        if id != &table.get_row_key(&row)? {
            self.delete(&table.name, id)?;
            self.create(&table.name, row)?;
            return Ok(());
        }

        // 找到indexes 一旦索引更改了 则需要将索引进行更新
        let indexes: Vec<_> = table
            .columns
            .iter()
            .enumerate()
            .filter(|(_, c)| c.index)
            .collect();

        if indexes.len() > 0 {
            // 我们这里的update 一般是先执行了查询，也就是说肯定是有这个数据的
            // 拿到老数据
            let old_row = self.read(&table.name, id)?.unwrap();
            for (index, column) in indexes {
                if old_row[index] != row[index] {
                    let mut old_entry =
                        self.read_index(&table.name, &column.name, &old_row[index])?;
                    old_entry.remove(id);
                    self.index_save(&table.name, &column.name, &old_row[index], old_entry)?;

                    let mut new_entry = self.read_index(&table.name, &column.name, &row[index])?;
                    new_entry.insert(table.get_row_key(&row)?);
                    self.index_save(&table.name, &column.name, &row[index], new_entry)?;
                }
            }
        };

        // 这个时候执行数据更新
        self.txn.set(
            &SqlKey::Row(table.name.into(), Some(id.into())).encode(),
            serialize(&row)?,
        )
    }
}

impl super::Catalog for KvTransaction {
    fn create_table(&mut self, table: Table) -> Result<()> {
        // 检查是否存在相同的
        if self.must_read_table(&table.name).is_ok() {
            return Err(Error::Table(format!("get same table for {}", table.name)));
        }
        // 检查
        table.validate(self)?;

        // 创建
        self.txn.set(
            &SqlKey::Table(Some(table.name.clone().into())).encode(),
            serialize(&table)?,
        )
    }

    fn delete_table(&mut self, table: &str) -> Result<()> {
        // 删除表之前 先删除表数据

        let table = self.must_read_table(table)?;
        let scan = self.scan(&table.name, None)?;
        for ele in scan.iter() {
            self.delete(&table.name, &table.get_row_key(&ele)?)?;
        }

        self.txn
            .delete(&SqlKey::Table(Some(table.name.into())).encode())
    }

    fn read_table(&self, table: &str) -> Result<Option<Table>> {
        let table = self.txn.get(&SqlKey::Table(Some(table.into())).encode())?;
        if let Some(table) = table {
            deserialize(&table)?
        }
        return Ok(None);
    }

    fn scan_tables(&self) -> Result<Vec<Table>> {
        let tables = self.txn.scan_prefix(&SqlKey::Table(None).encode())?;
        let tables: Result<Vec<Table>> = tables
            .map(|r| -> Result<Table> {
                let (_, table) = r?;
                let table: Table = deserialize(&table)?;
                Ok(table)
            })
            .collect();
        tables
    }
}

/// 用于设置key
#[derive(Debug)]
enum SqlKey<'a> {
    Table(Option<Cow<'a, str>>),
    /// table column key_value
    Index(Cow<'a, str>, Cow<'a, str>, Option<Cow<'a, Value>>),
    Row(Cow<'a, str>, Option<Cow<'a, Value>>),
}

impl<'a> SqlKey<'a> {
    /// 0x01 -> table
    /// 0x02 -> index
    /// 0x03 -> row
    fn encode(self) -> Vec<u8> {
        use kv::encoding::*;
        match self {
            Self::Table(None) => vec![0x01],
            Self::Table(Some(name)) => [&[0x01][..], &encode_string(&name)].concat(),
            Self::Index(table, column, None) => {
                [&[0x02][..], &encode_string(&table), &encode_string(&column)].concat()
            }
            Self::Index(table, column, Some(value)) => [
                &[0x02][..],
                &encode_string(&table),
                &encode_string(&column),
                &encode_value(&value),
            ]
            .concat(),
            Self::Row(table, None) => [&[0x03][..], &encode_string(&table)].concat(),
            Self::Row(table, Some(pk)) => {
                [&[0x03][..], &encode_string(&table), &encode_value(&pk)].concat()
            }
        }
    }

    fn decode(mut bytes: &[u8]) -> Result<Self> {
        use kv::encoding::*;
        let bytes = &mut bytes;
        let key = match take_byte(bytes)? {
            0x01 => Self::Table(Some(take_string(bytes)?.into())),
            0x02 => Self::Index(
                take_string(bytes)?.into(),
                take_string(bytes)?.into(),
                Some(take_value(bytes)?.into()),
            ),
            0x03 => Self::Row(take_string(bytes)?.into(), Some(take_value(bytes)?.into())),
            b => {
                return Err(Error::Encoding(format!(
                    "get unknown sql key prefix {:x}",
                    b
                )))
            }
        };
        if bytes.len() > 0 {
            return Err(Error::Encoding(format!(
                "get expect end of bytes of sqlKey decode"
            )));
        }
        Ok(key)
    }
}

fn serialize<V: Serialize>(value: &V) -> Result<Vec<u8>> {
    Ok(bincode::serialize(value)?)
}

fn deserialize<'a, V: Deserialize<'a>>(bytes: &'a [u8]) -> Result<V> {
    Ok(bincode::deserialize(bytes)?)
}
