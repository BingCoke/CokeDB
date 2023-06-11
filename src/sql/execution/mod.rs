pub mod aggregation;
pub mod join;
pub mod mutation;
pub mod query;
pub mod schema;
pub mod source;

use serde_derive::{Deserialize, Serialize};

use crate::storage::kv::mvcc::Mode;

use self::{
    aggregation::Aggregation,
    join::{HashJoin, NestedLoopJoin},
    mutation::{Delete, Insert, Update},
    query::{Filter, Limit, Offset, Order, Projection},
    schema::{CreateTable, DeleteTable},
    source::{Nothing, Scan, IndexLookUp, KeyLookUp},
};

use super::{engine::Transaction, plan::Node, Value};

use crate::errors::*;

/// 执行器
pub trait Executor<T: Transaction> {
    /// 执行器执行方法
    fn execute(self: Box<Self>, txn: &mut T) -> Result<ResultSet>;
}

impl<T: Transaction + 'static> dyn Executor<T> {
    /// 构建一个执行器
    pub fn build(node: Node) -> Box<dyn Executor<T>> {
        match node {
            Node::Aggregation { source, aggregates } => {
                Aggregation::new(Self::build(*source), aggregates)
            }
            Node::CreateTable { table, defaults } => CreateTable::new(table,defaults),
            Node::Delete { table, source } => Delete::new(table, Self::build(*source)),
            Node::DropTable { table } => DeleteTable::new(table),
            Node::Filter { source, predicate } => Filter::new(Self::build(*source), predicate),
            Node::HashJoin {
                left,
                left_field,
                right,
                right_field,
                outer,
            } => HashJoin::new(
                Self::build(*left),
                left_field.0,
                Self::build(*right),
                right_field.0,
                outer,
            ),
            Node::IndexLookup {
                table,
                alias: _,
                column,
                values,
            } => IndexLookUp::new(table, column, values),
            Node::Insert {
                table,
                columns,
                expressions,
            } => Insert::new(table, columns, expressions),
            Node::KeyLookup {
                table,
                alias: _,
                keys,
            } => KeyLookUp::new(table, keys),
            Node::Limit { source, limit } => Limit::new(Self::build(*source), limit),
            Node::NestedLoopJoin {
                left,
                left_size: _,
                right,
                predicate,
                outer,
            } => NestedLoopJoin::new(Self::build(*left), Self::build(*right), predicate, outer),
            Node::Nothing => Nothing::new(),
            Node::Offset { source, offset } => Offset::new(Self::build(*source), offset),
            Node::Order { source, orders } => Order::new(Self::build(*source), orders),
            Node::Projection {
                source,
                expressions,
            } => Projection::new(Self::build(*source), expressions),
            Node::Scan {
                table,
                filter,
                alias: _,
            } => Scan::new(table, filter),
            Node::Update {
                table,
                source,
                set,
            } => Update::new(
                table,
                Self::build(*source),
                set,
            ),
        }
    }
}

/// 执行器的结果
#[derive(Serialize, Deserialize, Debug, PartialEq)]
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
        rows: Rows,
    },
    // explain 结果
    Explain(Node),
}

pub type Row = Vec<Value>;
pub type Rows = Vec<Row>;
#[derive(Clone, Debug, PartialEq)]
pub struct Column {
    pub name: Option<String>,
}

pub type Columns = Vec<Column>;
