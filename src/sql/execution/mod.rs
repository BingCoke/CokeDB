pub mod source;
pub mod schema;
pub mod mutation;

use crate::storage::kv::mvcc::Mode;

use super::{engine::Transaction, Value, plan::Node};

use crate::errors::*;


/// 执行器
pub trait Executor<T: Transaction> {
    /// 执行器执行方法
    fn execute(self: Box<Self>, txn: &mut T) -> Result<ResultSet>;
}


/// 执行器的结果
//#[derive(Derivative, Serialize, Deserialize)]
//#[derivative(Debug, PartialEq)]
pub enum ResultSet {
    // 事务开始
    Begin {
        id: u64,
        mode: Mode,
    },
    // 事务提交
    Commit {
        id: u64,
    },
    // 事务回滚
    Rollback {
        id: u64,
    },
    // 创建行
    Create {
        count: u64,
    },
    // 删除行
    Delete {
        count: u64,
    },
    // 更新行 返回更新的几行
    Update {
        count: u64,
    },
    // 创建table
    CreateTable {
        name: String,
    },
    // 删除table
    DropTable {
        name: String,
    },
    // 查询结果
    Query {
        columns: Vec<Option<String>>,
        /* #[derivative(Debug = "ignore")]
        #[derivative(PartialEq = "ignore")]
        #[serde(skip, default = "ResultSet::empty_rows")] */
        rows: Rows,
    },
    // explain 结果
    Explain(Node),
}


pub type Row = Vec<Value>;
pub type Rows = Vec<Row>;
#[derive(Clone, Debug, PartialEq,)]
pub struct Column {
    pub name: Option<String>,
}

pub type Columns = Vec<Column>;
