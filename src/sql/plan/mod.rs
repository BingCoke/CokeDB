pub mod optimizer;
pub mod planner;

use core::fmt;
use std::fmt::Display;

use serde_derive::{Deserialize, Serialize};

use super::{
    engine::Transaction,
    execution::{Executor, ResultSet},
    expression::Expression,
    schema::Catalog,
    OrderType, Table, Value,
};
use crate::{
    errors::{Error, Result},
    sql::plan::{optimizer::Optimizer, planner::Planner},
};

/// 执行节点
#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub enum Node {
    CreateTable {
        table: Table,
        defaults: Vec<Option<Expression>>,
    },
    DropTable {
        table: String,
    },
    Insert {
        table: String,
        columns: Vec<String>,
        expressions: Vec<Vec<Expression>>,
    },
    Update {
        table: String,
        source: Box<Node>,
        set: Vec<(usize, Expression)>,
    },
    Delete {
        table: String,
        source: Box<Node>,
    },
    Scan {
        table: String,
        alias: Option<String>,
        filter: Option<Expression>,
    },
    NestedLoopJoin {
        left: Box<Node>,
        right: Box<Node>,
        left_size: usize,
        predicate: Option<Expression>,
        outer: bool,
    },
    Filter {
        source: Box<Node>,
        predicate: Expression,
    },
    /// 投影
    Projection {
        source: Box<Node>,
        /// Expression 是 Expression::Field
        /// 后面的option是label 如果是None则使用上层传来的column label
        expressions: Vec<(Expression, Option<String>)>,
    },
    /// 聚合
    Aggregation {
        source: Box<Node>,
        aggregates: Vec<Aggregate>,
    },
    Order {
        source: Box<Node>,
        orders: Vec<(Expression, OrderType)>,
    },
    Limit {
        source: Box<Node>,
        limit: Expression,
    },
    Offset {
        source: Box<Node>,
        offset: Expression,
    },
    HashJoin {
        left: Box<Node>,
        left_field: (usize, Option<(Option<String>, String)>),
        right: Box<Node>,
        right_field: (usize, Option<(Option<String>, String)>),
        outer: bool,
    },
    IndexLookup {
        table: String,
        alias: Option<String>,
        column: String,
        values: Vec<Value>,
    },
    KeyLookup {
        table: String,
        alias: Option<String>,
        keys: Vec<Value>,
    },
    Nothing,
}
impl Node {
    /// 将node转化为另一个node
    pub fn transform<B, A>(mut self, before: &B, after: &A) -> Result<Self>
    where
        B: Fn(Self) -> Result<Self>,
        A: Fn(Self) -> Result<Self>,
    {
        self = before(self)?;
        self = match self {
            Self::Update { table, source, set } => Self::Update {
                table,
                source: source.transform(before, after)?.into(),
                set,
            },
            Self::Delete { table, source } => Self::Delete {
                table,
                source: source.transform(before, after)?.into(),
            },

            Self::NestedLoopJoin {
                left,
                right,
                left_size,
                predicate,
                outer,
            } => Self::NestedLoopJoin {
                left: left.transform(before, after)?.into(),
                right: right.transform(before, after)?.into(),
                predicate,
                outer,
                left_size,
            },
            Self::Filter { source, predicate } => Self::Filter {
                source: source.transform(before, after)?.into(),
                predicate,
            },
            Self::Aggregation { source, aggregates } => Self::Aggregation {
                source: source.transform(before, after)?.into(),
                aggregates,
            },
            Self::HashJoin {
                left,
                left_field,
                right,
                right_field,
                outer,
            } => Self::HashJoin {
                left: left.transform(before, after)?.into(),
                left_field,
                right: right.transform(before, after)?.into(),
                right_field,
                outer,
            },
            Self::Limit { source, limit } => Self::Limit {
                source: source.transform(before, after)?.into(),
                limit,
            },
            Self::Offset { source, offset } => Self::Offset {
                source: source.transform(before, after)?.into(),
                offset,
            },
            Self::Order { source, orders } => Self::Order {
                source: source.transform(before, after)?.into(),
                orders,
            },
            Self::Projection {
                source,
                expressions,
            } => Self::Projection {
                source: source.transform(before, after)?.into(),
                expressions,
            },

            // 最低层的操作就不转换了
            n @ Self::CreateTable { .. }
            | n @ Self::DropTable { .. }
            | n @ Self::IndexLookup { .. }
            | n @ Self::Insert { .. }
            | n @ Self::KeyLookup { .. }
            | n @ Self::Nothing
            | n @ Self::Scan { .. } => n,
        };
        after(self)
    }

    /// 转换node中的expression
    pub fn transform_expressions<B, A>(self, before: &B, after: &A) -> Result<Self>
    where
        B: Fn(Expression) -> Result<Expression>,
        A: Fn(Expression) -> Result<Expression>,
    {
        Ok(match self {
            n @ Self::Aggregation { .. }
            | n @ Self::CreateTable { .. }
            | n @ Self::Delete { .. }
            | n @ Self::DropTable { .. }
            | n @ Self::HashJoin { .. }
            | n @ Self::IndexLookup { .. }
            | n @ Self::KeyLookup { .. }
            | n @ Self::Limit { .. }
            | n @ Self::NestedLoopJoin {
                predicate: None, ..
            }
            | n @ Self::Nothing
            | n @ Self::Offset { .. }
            | n @ Self::Scan { filter: None, .. } => n,

            Self::Filter { source, predicate } => Self::Filter {
                source,
                predicate: predicate.transform(before, after)?,
            },

            Self::Insert {
                table,
                columns,
                expressions,
            } => Self::Insert {
                table,
                columns,
                expressions: expressions
                    .into_iter()
                    .map(|exprs| {
                        exprs
                            .into_iter()
                            .map(|e| e.transform(before, after))
                            .collect()
                    })
                    .collect::<Result<_>>()?,
            },

            Self::Order { source, orders } => Self::Order {
                source,
                orders: orders
                    .into_iter()
                    .map(|(e, o)| e.transform(before, after).map(|e| (e, o)))
                    .collect::<Result<_>>()?,
            },

            Self::NestedLoopJoin {
                left,
                right,
                predicate: Some(predicate),
                outer,
                left_size,
            } => Self::NestedLoopJoin {
                left,
                right,
                predicate: Some(predicate.transform(before, after)?),
                outer,
                left_size,
            },

            Self::Projection {
                source,
                expressions,
            } => Self::Projection {
                source,
                expressions: expressions
                    .into_iter()
                    .map(|(e, l)| Ok((e.transform(before, after)?, l)))
                    .collect::<Result<_>>()?,
            },

            Self::Scan {
                table,
                alias,
                filter: Some(filter),
            } => Self::Scan {
                table,
                alias,
                filter: Some(filter.transform(before, after)?),
            },

            Self::Update { table, source, set } => Self::Update {
                table,
                source,
                set: set
                    .into_iter()
                    .map(|(i, e)| e.transform(before, after).map(|e| (i, e)))
                    .collect::<Result<_>>()?,
            },
        })
    }

    // Displays the node, where prefix gives the node prefix.
    pub fn format(&self, mut indent: String, root: bool, last: bool) -> String {
        let mut s = indent.clone();
        if !last {
            s += "├─ ";
            indent += "│  "
        } else if !root {
            s += "└─ ";
            indent += "   ";
        }
        match self {
            Self::Aggregation { source, aggregates } => {
                s += &format!(
                    "Aggregation: {}\n",
                    aggregates
                        .iter()
                        .map(|a| a.to_string())
                        .collect::<Vec<_>>()
                        .join(", ")
                );
                s += &source.format(indent, false, true);
            }
            Self::CreateTable { table, defaults } => {
                s += &format!("CreateTable: {}\n", table.name);
            }
            Self::Delete { source, table } => {
                s += &format!("Delete: {}\n", table);
                s += &source.format(indent, false, true);
            }
            Self::DropTable { table } => {
                s += &format!("DropTable: {}\n", table);
            }
            Self::Filter { source, predicate } => {
                s += &format!("Filter: {}\n", predicate);
                s += &source.format(indent, false, true);
            }
            Self::HashJoin {
                left,
                left_field,
                right,
                right_field,
                outer,
            } => {
                s += &format!(
                    "HashJoin: {} on {} = {}\n",
                    if *outer { "outer" } else { "inner" },
                    match left_field {
                        (_, Some((Some(t), n))) => format!("{}.{}", t, n),
                        (_, Some((None, n))) => n.clone(),
                        (i, None) => format!("left #{}", i),
                    },
                    match right_field {
                        (_, Some((Some(t), n))) => format!("{}.{}", t, n),
                        (_, Some((None, n))) => n.clone(),
                        (i, None) => format!("right #{}", i),
                    },
                );
                s += &left.format(indent.clone(), false, false);
                s += &right.format(indent, false, true);
            }
            Self::IndexLookup {
                table,
                column,
                alias,
                values,
            } => {
                s += &format!("IndexLookup: {}", table);
                if let Some(alias) = alias {
                    s += &format!(" as {}", alias);
                }
                s += &format!(" column {}", column);
                if !values.is_empty() && values.len() < 10 {
                    s += &format!(
                        " ({})",
                        values
                            .iter()
                            .map(|k| k.to_string())
                            .collect::<Vec<_>>()
                            .join(", ")
                    );
                } else {
                    s += &format!(" ({} values)", values.len());
                }
                s += "\n";
            }
            Self::Insert {
                table,
                columns: _,
                expressions,
            } => {
                s += &format!("Insert: {} ({} rows)\n", table, expressions.len());
            }
            Self::KeyLookup { table, alias, keys } => {
                s += &format!("KeyLookup: {}", table);
                if let Some(alias) = alias {
                    s += &format!(" as {}", alias);
                }
                if !keys.is_empty() && keys.len() < 10 {
                    s += &format!(
                        " ({})",
                        keys.iter()
                            .map(|k| k.to_string())
                            .collect::<Vec<_>>()
                            .join(", ")
                    );
                } else {
                    s += &format!(" ({} keys)", keys.len());
                }
                s += "\n";
            }
            Self::Limit { source, limit } => {
                s += &format!("Limit: {}\n", limit);
                s += &source.format(indent, false, true);
            }
            Self::NestedLoopJoin {
                left,
                right,
                predicate,
                outer,
                left_size: _,
            } => {
                s += &format!("NestedLoopJoin: {}", if *outer { "outer" } else { "inner" });
                if let Some(expr) = predicate {
                    s += &format!(" on {}", expr);
                }
                s += "\n";
                s += &left.format(indent.clone(), false, false);
                s += &right.format(indent, false, true);
            }
            Self::Nothing {} => {
                s += "Nothing\n";
            }
            Self::Offset { source, offset } => {
                s += &format!("Offset: {}\n", offset);
                s += &source.format(indent, false, true);
            }
            Self::Order { source, orders } => {
                s += &format!(
                    "Order: {}\n",
                    orders
                        .iter()
                        .map(|(expr, dir)| format!("{} {}", expr, dir))
                        .collect::<Vec<_>>()
                        .join(", ")
                );
                s += &source.format(indent, false, true);
            }
            Self::Projection {
                source,
                expressions,
            } => {
                s += &format!(
                    "Projection: {}\n",
                    expressions
                        .iter()
                        .map(|(expr, _)| expr.to_string())
                        .collect::<Vec<_>>()
                        .join(", ")
                );
                s += &source.format(indent, false, true);
            }
            Self::Scan {
                table,
                alias,
                filter,
            } => {
                s += &format!("Scan: {}", table);
                if let Some(alias) = alias {
                    s += &format!(" as {}", alias);
                }
                if let Some(expr) = filter {
                    s += &format!(" ({})", expr);
                }
                s += "\n";
            }
            Self::Update { source, table, set } => {
                s += &format!(
                    "Update: {} ({})\n",
                    table,
                    set.iter()
                        .map(|(i, e)| format!(
                            "{}={}",
                            // l.clone().unwrap_or_else(|| format!("#{}", i)),
                            format!("#{}", i),
                            e
                        ))
                        .collect::<Vec<_>>()
                        .join(",")
                );
                s += &source.format(indent, false, true);
            }
        };
        if root {
            s = s.trim_end().to_string()
        }
        s
    }
}
impl Display for Node {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.format("".into(), true, true))
    }
}

pub struct Plan {
    pub node: Node,
}

impl Plan {
    pub fn new(node: Node) -> Self {
        Self { node }
    }

    pub(crate) fn build(
        state: super::parser::ast::Statement,
        catalog: &'static dyn Catalog,
    ) -> Result<Self> {
        let node = Planner::new(catalog).build_node(state)?;
        Ok(Self { node })
    }

    /// 进行节点优化
    pub fn optimize(self, catalog: &dyn Catalog) -> Result<Self> {
        let mut root = self.node;
        //root = optimizer::ConstantFolder.optimize(root)?;
        root = optimizer::FilterPushdown.optimize(root)?;
        root = optimizer::IndexLookup::new(catalog).optimize(root)?;
        //root = optimizer::JoinType.optimize(root)?;
        //root = optimizer::NoopCleaner.optimize(root)?;
        Ok(Plan::new(root))
    }
    pub fn execute<T: Transaction + 'static>(self, txn: &mut T) -> Result<ResultSet> {
        <dyn Executor<T>>::build(self.node).execute(txn)
    }
}

/// 聚合函数
#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub enum Aggregate {
    /// 求和
    Sum,
    /// 平均
    Average,
    /// 计数
    Count,
    /// 求最大值
    Max,
    /// 最小值
    Min,
}

impl Display for Aggregate {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}",
            match self {
                Aggregate::Sum => "Sum",
                Aggregate::Average => "Average",
                Aggregate::Count => "Count",
                Aggregate::Max => "Max",
                Aggregate::Min => "Min",
            }
        )
    }
}

impl Aggregate {
    pub fn from_str(f: &str) -> Result<Aggregate> {
        match f.to_uppercase().as_str() {
            "MAX" => Ok(Self::Max),
            "MIN" => Ok(Self::Min),
            "SUM" => Ok(Self::Sum),
            "COUNT" => Ok(Self::Count),
            "AVERAGE" => Ok(Self::Average),
            _ => Err(Error::Plan(format!("not support for aggregate: {}", f))),
        }
    }
    pub fn to_string(&self) -> String {
        match self {
            Aggregate::Sum => "Sum".to_string(),
            Aggregate::Average => "Average".to_string(),
            Aggregate::Count => "Count".to_string(),
            Aggregate::Max => "Max".to_string(),
            Aggregate::Min => "Min".to_string(),
        }
    }
}
