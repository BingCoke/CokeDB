use std::collections::HashSet;

use super::{schema::Catalog, Value, expression::Expression};
use crate::errors::*;

pub mod kv;


pub type Mode = crate::storage::kv::mvcc::Mode;
pub type Row = Vec<Value>;
pub type Rows = Vec<Row>;
/// value 是 key（索引值）, hashset是索引的对应的主键值
pub type IndexScan = Vec<(Value,HashSet<Value>)>;



/// sql引擎接口
pub trait Engine: Clone {
    /// 设置事务类型
    type Transaction: Transaction;

    /// 开启一个事务
    fn begin(&self, mode: Mode) -> Result<Self::Transaction>;

    /// 开启一个会话
    fn session(&self) -> Result<Session<Self>> {
        Ok(Session { engine: self.clone(), txn: None })
    }

    /// 通过事务id 重新启动一个老事务
    fn resume(&self, id: u64) -> Result<Self::Transaction>;
}

/// 设置一个事务
pub trait Transaction: Catalog {
    /// 事务id
    fn id(&self) -> u64;
    /// 事务模式
    fn mode(&self) -> Mode;
    /// 提交事务
    fn commit(self) -> Result<()>;
    /// 回滚事务
    fn rollback(self) -> Result<()>;
    /// 创建一个行
    fn create(&mut self, table: &str, row: Row) -> Result<()>;
    /// 删除行
    fn delete(&mut self, table: &str, id: &Value) -> Result<()>;
    /// 通过主键返回一个row
    fn read(&self, table: &str, id: &Value) -> Result<Option<Row>>;
    /// 得到column=value的行主键  column应是索引
    fn read_index(&self, table: &str, column: &str, value: &Value) -> Result<HashSet<Value>>;
    /// scan table
    fn scan(&self, table: &str, filter: Option<Expression>) -> Result<Rows>;
    /// 得到索引entry 就是set集合， 里面有对应的主键
    fn scan_index(&self, table: &str, column: &str) -> Result<IndexScan>;
    /// 更新一个表行
    fn update(&mut self, table: &str, id: &Value, row: Row) -> Result<()>;
}

/// sql session 处理事务和表的请求
pub struct Session<E: Engine> {
    /// 底层引擎
    engine: E,
    /// 当前的事务
    txn: Option<E::Transaction>,
}

pub type  SqlScan = Box<dyn DoubleEndedIterator<Item = Result<Row>> + Send>;
pub type  SqlIndexScan = Box<dyn DoubleEndedIterator<Item = Result<(Value,HashSet<Value>)>> + Send>;
