use std::collections::HashSet;

/// source文件，最低层的执行器，用于执行扫描文件
use crate::sql::{engine::Transaction, execution::ResultSet, expression::Expression, Value};

use super::Executor;
use crate::errors::*;

pub struct Scan {
    /// 扫描的表
    table: String,
    /// 扫描的filter条件
    filter: Option<Expression>,
}

impl Scan {
    pub fn new(table: String, filter: Option<Expression>) -> Box<Self> {
        Box::new(Self { table, filter })
    }
}

impl<T: Transaction> Executor<T> for Scan {
    fn execute(self: Box<Self>, txn: &mut T) -> Result<super::ResultSet> {
        let rows = txn.scan(&self.table, self.filter)?;
        let columns: Vec<_> = txn
            .must_read_table(&self.table)?
            .columns
            .iter()
            .map(|c| Some(c.name.to_string()))
            .collect();
        let res = ResultSet::Query { columns, rows };
        return Ok(res);
    }
}

pub struct KeyLookUp {
    table: String,
    values: Vec<Value>,
}

impl KeyLookUp {
    pub fn new(table: String, values: Vec<Value>) -> Box<Self> {
        Box::new(Self { table, values })
    }
}

impl<T: Transaction> Executor<T> for KeyLookUp {
    fn execute(self: Box<Self>, txn: &mut T) -> Result<ResultSet> {
        // 这个地方是含有option的 需要进一步转化
        // 这里被转换的都是 or 句子 所以option为none的不显示即可 使用filter_map
        let rows: Result<Vec<_>> = self
            .values
            .iter()
            .filter_map(|v| txn.read(&self.table, v).transpose())
            .collect();
        let rows = rows?;
        let columns: Vec<_> = txn
            .must_read_table(&self.table)?
            .columns
            .iter()
            .map(|c| Some(c.name.clone()))
            .collect();

        Ok(ResultSet::Query { columns, rows })
    }
}

pub struct IndexLookUp {
    table: String,
    /// 索引列
    column: String,
    /// 值
    values: Vec<Value>,
}

impl IndexLookUp {
    pub fn new(table: String, column: String, values: Vec<Value>) -> Box<Self> {
        Box::new(Self {
            table,
            column,
            values,
        })
    }
}

impl<T: Transaction> Executor<T> for IndexLookUp {
    fn execute(self: Box<Self>, txn: &mut T) -> Result<ResultSet> {
        let mut keys = HashSet::new();

        self.values
            .into_iter()
            .map(|v| -> Result<Value> {
                let entrys = txn.read_index(&self.table, &self.column, &v)?;
                keys.extend(entrys.into_iter());
                Ok(v)
            })
            .collect::<Result<Vec<_>>>()?;

        let rows = keys
            .iter()
            .filter_map(|k| txn.read(&self.table, k).transpose())
            .collect::<Result<Vec<_>>>()?;

        let columns: Vec<_> = txn
            .must_read_table(&self.table)?
            .columns
            .iter()
            .map(|c| Some(c.name.clone()))
            .collect();

        Ok(ResultSet::Query { columns, rows })
    }
}

/// An executor that produces a single empty row
pub struct Nothing;

impl Nothing {
    pub fn new() -> Box<Self> {
        Box::new(Self)
    }
}

impl<T: Transaction> Executor<T> for Nothing {
    fn execute(self: Box<Self>, _: &mut T) -> Result<ResultSet> {
        Ok(ResultSet::Query {
            columns: Vec::new(),
            rows: vec![vec![]],
        })
    }
}
