use super::Table;
use crate::errors::{Error, Result};

/// 对于模式的定义
pub trait Catalog {
    /// 创建一个表
    fn create_table(&mut self, table: Table) -> Result<()>;
    /// 删除一个表
    fn delete_table(&mut self, table: &str) -> Result<()>;
    /// 根据表名称获取
    fn read_table(&self, table: &str) -> Result<Option<Table>>;
    /// 获取所有表
    fn scan_tables(&self) -> Result<Vec<Table>>;

    /// 找到一个table 如果没有就返回错误
    fn must_read_table(&self, table: &str) -> Result<Table> {
        self.read_table(table)?
            .ok_or_else(|| Error::Schema(format!("Table {} does not exist", table)))
    }

}


// 定义tables是一个
pub type Tables = Vec<Table>;
