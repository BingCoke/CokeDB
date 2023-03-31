pub mod planner;

use std::fmt::Display;

use super::{expression::Expression, OrderType, Table, Value};
use crate::errors::{Error, Result};

/// 聚合函数
#[derive(Debug, PartialEq)]
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
}

/// 执行节点
#[derive(Debug, PartialEq)]
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

pub struct Plan {
    node: Node,
}

impl Plan {
    pub fn new(node: Node) -> Self {
        Self { node }
    }
}
