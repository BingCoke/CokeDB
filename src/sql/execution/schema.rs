use std::default;

use super::{Executor, ResultSet};
use crate::errors::*;
use crate::sql::expression::Expression;
use crate::sql::Value;
/// 设置表结构的sql执行
/// 不设置更新表结构
use crate::sql::{engine::Transaction, Table};

pub struct CreateTable {
    table: Table,
    defaults: Vec<Option<Expression>>,
}

impl CreateTable {
    pub fn new(table: Table, defaults: Vec<Option<Expression>>) -> Box<Self> {
        Box::new(Self { table, defaults })
    }
}

impl<T: Transaction> Executor<T> for CreateTable {
    fn execute(mut self: Box<Self>, txn: &mut T) -> Result<ResultSet> {
        let name = self.table.name.clone();
        // 之前default没有计算常量
        let defaults = self
            .defaults
            .into_iter()
            .map(|d| d.map(|de| de.evaluate(None)).transpose())
            .collect::<Result<Vec<Option<Value>>>>()?;
        self.table
            .columns
            .iter_mut()
            .zip(defaults)
            .for_each(|(c, d)| (*c).default = d);

        txn.create_table(self.table)?;
        Ok(ResultSet::CreateTable { name })
    }
}

pub struct DeleteTable {
    table: String,
}

impl DeleteTable {
    pub fn new(table: String) -> Box<Self> {
        Box::new(Self { table })
    }
}

impl<T: Transaction> Executor<T> for DeleteTable {
    fn execute(self: Box<Self>, txn: &mut T) -> Result<ResultSet> {
        txn.delete_table(&self.table)?;
        Ok(ResultSet::DropTable { name: self.table })
    }
}
