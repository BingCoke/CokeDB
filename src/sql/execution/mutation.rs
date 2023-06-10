/* 设置增删改 对于数据的更改操作
 * */

use std::{collections::HashMap, ops::Index};

use crate::sql::{engine::Transaction, execution::ResultSet, expression::Expression};

use super::Executor;
use crate::errors::*;

pub struct Insert {
    table: String,
    columns: Vec<String>,
    rows: Vec<Vec<Expression>>,
}

impl Insert {
    pub fn new(table: String, columns: Vec<String>, rows: Vec<Vec<Expression>>) -> Box<Self> {
        Box::new(Self {
            table,
            columns,
            rows,
        })
    }
}

impl<T: Transaction> Executor<T> for Insert {
    /// 返回值返回插入的行数
    fn execute(mut self: Box<Self>, txn: &mut T) -> Result<super::ResultSet> {
        let table = txn.must_read_table(&self.table)?;
        let mut count = 0;
        let rows_len = self.rows.len();

        // 如果没有columns 说明是table中的columns
        if self.columns.len() == 0 {
            self.columns
                .extend(table.columns.iter().map(|c| c.name.clone()));
        }
        for expressions in self.rows {
            let row = expressions
                .into_iter()
                .map(|e| e.evaluate(None))
                .collect::<Result<Vec<_>>>()?;

            if self.columns.len() != row.len() {
                return Err(Error::Table(format!(
                    "you want insert columns len is {}. but get {} row value",
                    self.columns.len(),
                    rows_len
                )));
            }

            // 设置一个map 来保存是否已经存储过
            let mut map = HashMap::new();

            for (index, c) in self.columns.iter().enumerate() {
                // 判断是否存在
                table.get_column_index(c)?;
                map.insert(
                    c.clone(),
                    row.get(index)
                        .ok_or(Error::Table(format!("get row index {index} err ")))?,
                );
            }

            let mut row = Vec::new();
            for column in table.columns.iter() {
                // 如果能在刚刚的map中找到，说明是用户自己插入的值
                if let Some(value) = map.get(&column.name).cloned() {
                    row.push(value.clone())
                // 否则是默认值
                } else if let Some(value) = &column.default {
                    row.push(value.clone())
                } else {
                    // 没有默认值报错
                    return Err(Error::Table(format!(
                        "No value given for column {}",
                        column.name
                    )));
                }
            }
            txn.create(&table.name, row)?;
            count = count + 1;
        }

        Ok(super::ResultSet::Create { count })
    }
}

pub struct Update<T: Transaction> {
    table: String,
    source: Box<dyn Executor<T>>,
    expression: Vec<(usize, Expression)>,
}

impl<T: Transaction> Update<T> {
    pub fn new(
        table: String,
        source: Box<dyn Executor<T>>,
        expression: Vec<(usize, Expression)>,
    ) -> Box<Self> {
        Box::new(Self {
            table,
            source,
            expression,
        })
    }
}

impl<T: Transaction> Executor<T> for Update<T> {
    fn execute(self: Box<Self>, txn: &mut T) -> Result<super::ResultSet> {
        let table = txn.must_read_table(&self.table)?;
        let key_index = table.get_key_index()?;

        match self.source.execute(txn)? {
            ResultSet::Query { columns, rows } => {
                let mut count: u64 = 0;

                for mut row in rows {
                    let pk = row.get(key_index).cloned().ok_or(Error::Executor(format!(
                        "try get key in row {:?} index {}",
                        row, key_index
                    )))?;
                    // clone新的row
                    let mut new = row.clone();
                    // 设置新的new
                    for (index, exp) in self.expression.iter() {
                        new[*index] = exp.evaluate(Some(&row))?;
                    }

                    txn.update(&table.name, &pk, new)?;

                    count += 1;
                }

                Ok(ResultSet::Update { count })
            }
            r => Err(Error::Executor(format!(
                "expect get query ersult set but get {:?}",
                r
            ))),
        }
    }
}

pub struct Delete<T: Transaction> {
    table: String,
    source: Box<dyn Executor<T>>,
}

impl<T: Transaction> Delete<T> {
    pub fn new(table: String, source: Box<dyn Executor<T>>) -> Box<Self> {
        Box::new(Self { table, source })
    }
}

impl<T: Transaction> Executor<T> for Delete<T> {
    fn execute(self: Box<Self>, txn: &mut T) -> Result<super::ResultSet> {
        let table = txn.must_read_table(&self.table)?;
        let key_index = table.get_key_index()?;

        match self.source.execute(txn)? {
            ResultSet::Query { columns, rows } => {
                let mut count: u64 = 0;

                for row in rows {
                    let pk = row.get(key_index).ok_or(Error::Executor(format!(
                        "try get key in row {:?} index {}",
                        row, key_index
                    )))?;
                    txn.delete(&table.name, pk);
                    count += 1;
                }

                Ok(ResultSet::Update { count })
            }
            r => Err(Error::Executor(format!(
                "expect get query ersult set but get {:?}",
                r
            ))),
        }
    }
}
