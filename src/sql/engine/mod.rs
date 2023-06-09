use super::{execution::ResultSet, expression::Expression, schema::Catalog, Value};
use crate::errors::Error;
use crate::sql::plan::planner::Planner;
use crate::sql::plan::Plan;
use crate::storage::kv::mvcc::Mode;
use crate::{errors::*, sql::parser::Parser};
use futures_util::poll;
use log::debug;
use serde_derive::{Deserialize, Serialize};
use std::collections::HashSet;

pub mod kv;
pub mod raft;

pub type Row = Vec<Value>;
pub type Rows = Vec<Row>;
/// value 是 key（索引值）, hashset是索引的对应的主键值
pub type IndexScan = Vec<(Value, HashSet<Value>)>;

/// sql引擎接口
pub trait Engine: Clone {
    /// 设置事务类型
    type Transaction: Transaction;

    /// 开启一个事务
    fn begin(&self, mode: Mode) -> Result<Self::Transaction>;

    /// 开启一个会话
    fn session(&self) -> Result<SqlSession<Self>> {
        Ok(SqlSession {
            engine: self.clone(),
            txn: None,
        })
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
pub struct SqlSession<E: Engine> {
    /// 底层引擎
    engine: E,
    /// 当前的事务
    txn: Option<E::Transaction>,
}

impl<E: Engine + 'static> SqlSession<E> {
    /// Runs a closure in the session's transaction, or a new transaction if none is active.
    pub fn with_txn<R, F>(&mut self, mode: Mode, f: F) -> Result<R>
    where
        F: FnOnce(&mut E::Transaction) -> Result<R>,
    {
        if let Some(ref mut txn) = self.txn {
            if !txn.mode().satisfies(&mode) {
                return Err(Error::Executor(
                    "The operation cannot run in the current transaction".into(),
                ));
            }
            return f(txn);
        }
        let mut txn: <E as Engine>::Transaction = self.engine.begin(mode)?;
        let result = f(&mut txn);
        txn.commit()?;
        result
    }

    pub fn execute(&mut self, sql: &str) -> Result<ResultSet> {
        debug!("execute sql : {}",sql);
        let r: Result<ResultSet> = match Parser::new(sql).parse()? {
            // begin 分为几种情况
            crate::sql::parser::ast::Statement::Begin { .. } if self.txn.is_some() => Err(
                Error::Executor("there already has a transaction".to_string()),
            ),
            // 没问题的话就是 开启一个事务
            crate::sql::parser::ast::Statement::Begin {
                readonly: false,
                version: None,
            } => {
                let txn = self.engine.begin(Mode::ReadWrite)?;
                let result = ResultSet::Begin {
                    id: txn.id(),
                    mode: txn.mode(),
                };
                self.txn = Some(txn);
                Ok(result)
            }
            // TODO: 目前是想要server启动的时候去检查 ，这样就不需要进行事务恢复了
            // 所以这里暂时不写了
            // 本来是考虑重启之后之前的事务可能没有commit 这样就导致一些数据一直被锁住了
            // 但是还需要去考虑 raft每个raft节点的问题
            crate::sql::parser::ast::Statement::Begin { readonly, version } => todo!(),
            crate::sql::parser::ast::Statement::Commit if self.txn.is_none() => {
                Err(Error::Executor("not transaction to commit".into()))
            }
            // 执行commit操作
            crate::sql::parser::ast::Statement::Commit => {
                let txn = self.txn.take().unwrap();
                let id = txn.id();
                if let Err(err) = txn.commit() {
                    // 如果commit失败了 将事务恢复
                    if let Ok(t) = self.engine.resume(id) {
                        self.txn = Some(t);
                    }
                    return Err(err);
                }
                Ok(ResultSet::Commit { id })
            }
            crate::sql::parser::ast::Statement::Rollback if self.txn.is_none() => {
                Err(Error::Executor("not transaction to rollback".into()))
            }
            crate::sql::parser::ast::Statement::Rollback => {
                let txn = self.txn.take().unwrap();
                let id = txn.id();
                txn.rollback()?;
                Ok(ResultSet::Rollback { id })
            }
            crate::sql::parser::ast::Statement::Explain(state) => {
                let txn = self.txn.take().unwrap();
                let mut planner = Planner::new(&txn);
                //let plan = planner.build_plan(*state);
                let node = planner.build_node(*state)?;
                Ok(ResultSet::Explain(node))
            }

            // 如果当前有一个事务在进行
            statement if self.txn.is_some() => {
                //let mut txn = self.txn.as_mut().unwrap();
                let mut txn = self.txn.take().unwrap();
                Planner::new(&txn)
                    .build_plan(statement)?
                    .optimize(&txn)?
                    .execute(&mut txn)
            }
            // 没有事务在进行
            statement => {
                let mut txn = self.engine.begin(Mode::ReadWrite)?;
                let r = Planner::new(&txn)
                    .build_plan(statement)?
                    .optimize(&txn)?
                    .execute(&mut txn);
                txn.commit()?;
                r
            }
        };
        r
    }
}

/// status
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Status {
    pub mvcc: crate::storage::kv::mvcc::Status,
}
pub type SqlScan = Box<dyn DoubleEndedIterator<Item = Result<Row>> + Send>;
pub type SqlIndexScan = Box<dyn DoubleEndedIterator<Item = Result<(Value, HashSet<Value>)>> + Send>;
