use super::{Executor, ResultSet};
use crate::errors::*;
/// 设置表结构的sql执行
/// 不设置更新表结构
use crate::sql::{engine::Transaction, Table};

pub struct CreateTable {
    table: Table,
}

impl<T: Transaction> Executor<T> for CreateTable {
    fn execute(self: Box<Self>, txn: &mut T) -> Result<ResultSet> {
        let name = self.table.name.clone();
        txn.create_table(self.table)?;
        Ok(ResultSet::CreateTable { name })
    }
}

pub struct DeleteTable {
    table: String,
}

impl DeleteTable {
    pub fn new(table: String) -> Self {
        Self { table }
    }
}


impl<T: Transaction> Executor<T> for DeleteTable {
    fn execute(self: Box<Self>, txn: &mut T) -> Result<ResultSet> {
        txn.delete_table(&self.table)?;
        Ok(ResultSet::DropTable { name: self.table })
    }
}

