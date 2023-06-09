use serde::de::Unexpected;

use crate::sql::execution::Column;
use crate::sql::{engine::Transaction, expression::Expression, OrderType};

use super::Executor;
use super::ResultSet;
use crate::errors::*;
use crate::sql::Value;

pub struct Filter<T: Transaction> {
    source: Box<dyn Executor<T>>,
    predicate: Expression,
}

impl<T: Transaction> Filter<T> {
    pub fn new(source: Box<dyn Executor<T>>, predicate: Expression) -> Box<Self> {
        Box::new(Self { source, predicate })
    }
}

/// filter 只需要执行row 然后返回值是否是true即可
/// 但是必须要保证返回值是true或者false
/// 如果不是 布尔返回值就是错误的
impl<T: Transaction> Executor<T> for Filter<T> {
    fn execute(self: Box<Self>, txn: &mut T) -> Result<super::ResultSet> {
        match self.source.execute(txn)? {
            ResultSet::Query { columns, rows } => {
                let rows = rows
                    .into_iter()
                    .filter_map(|row| {
                        let result = self.predicate.evaluate(Some(&row));
                        match result {
                            Ok(r) => match r {
                                Value::Null => None,
                                Value::Bool(false) => None,
                                Value::Bool(true) => Some(Ok(row)),
                                other => Some(Err(Error::Executor(format!(
                                    "filter execution expect get bool but get {:?}",
                                    other
                                )))),
                            },
                            Err(e) => Some(Err(e)),
                        }
                    })
                    .collect::<Result<Vec<_>>>()?;
                Ok(ResultSet::Query { columns, rows })
            }
            r => Err(Error::Executor(format!(
                "expect get resultset::query but get {:?}",
                r
            ))),
        }
    }
}

pub struct Projection<T: Transaction> {
    source: Box<dyn Executor<T>>,
    expressions: Vec<(Expression, Option<String>)>,
}

impl<T: Transaction> Projection<T> {
    pub fn new(
        source: Box<dyn Executor<T>>,
        expressions: Vec<(Expression, Option<String>)>,
    ) -> Box<Self> {
        Box::new(Self {
            source,
            expressions,
        })
    }
}

impl<T: Transaction> Executor<T> for Projection<T> {
    fn execute(self: Box<Self>, txn: &mut T) -> Result<super::ResultSet> {
        match self.source.execute(txn)? {
            ResultSet::Query { columns, rows } => {
                // 设置一下column 的label 没有就看看是不是filed 改成filed名字
                let (expressions, labels): (Vec<Expression>, Vec<Option<String>>) =
                    self.expressions.into_iter().unzip();

                let columns: Vec<_> = expressions
                    .iter()
                    .enumerate()
                    .map(|(i, e)| {
                        if let Some(Some(label)) = labels.get(i) {
                            Some(label.clone())
                        } else if let Expression::Field(i, _) = e {
                            columns.get(*i).cloned().unwrap_or(None)
                        } else {
                            None
                        }
                    })
                    .collect();

                let rows: Result<Vec<_>> = rows
                    .iter()
                    .map(|r| {
                        expressions
                            .iter()
                            .map(|e| e.evaluate(Some(&r)))
                            .collect::<Result<Vec<_>>>()
                    })
                    .collect();

                Ok(ResultSet::Query {
                    columns,
                    rows: rows?,
                })
            }
            r => Err(Error::Executor(format!(
                "expect get resultset::query but get {:?}",
                r
            ))),
        }
    }
}

pub struct Order<T: Transaction> {
    source: Box<dyn Executor<T>>,
    order: Vec<(Expression, OrderType)>,
}

impl<T: Transaction> Order<T> {
    pub fn new(source: Box<dyn Executor<T>>, order: Vec<(Expression, OrderType)>) -> Box<Self> {
        Box::new(Self { source, order })
    }
}

impl<T: Transaction> Executor<T> for Order<T> {
    fn execute(self: Box<Self>, txn: &mut T) -> Result<super::ResultSet> {
        match self.source.execute(txn)? {
            ResultSet::Query { columns, rows } => {
                struct Item {
                    /// 这个是要存储的
                    row: Vec<Value>,
                    /// 这是个要排序的
                    values: Vec<Value>,
                }
                let mut items = Vec::new();
                for row in rows {
                    let mut values = Vec::new();
                    // 把需要排序的值进行计算
                    for (expr, _) in self.order.iter() {
                        values.push(expr.evaluate(Some(&row))?);
                    }
                    items.push(Item { row, values })
                }

                let order = &self.order;
                items.sort_by(|a, b| {
                    for (i, (_, order)) in order.iter().enumerate() {
                        let value_a = &a.values[i];
                        let value_b = &b.values[i];
                        match value_a.partial_cmp(value_b) {
                            Some(std::cmp::Ordering::Equal) => {}
                            // 要么大 要么 小于
                            Some(o) => {
                                // 如果是 decs 需要反向排序
                                return if *order == OrderType::ASC {
                                    o
                                } else {
                                    o.reverse()
                                };
                            }
                            None => {}
                        }
                    }
                    std::cmp::Ordering::Equal
                });

                Ok(ResultSet::Query {
                    columns,
                    rows: items.into_iter().map(|i| i.row).collect(),
                })
            }
            r => Err(Error::Executor(format!(
                "expect get resultset::query but get {:?}",
                r
            ))),
        }
    }
}

pub struct Limit<T: Transaction> {
    source: Box<dyn Executor<T>>,
    limit: Expression,
}

impl<T: Transaction> Limit<T> {
    pub fn new(source: Box<dyn Executor<T>>, limit: Expression) -> Box<Self> {
        Box::new(Self { source, limit })
    }
}

impl<T: Transaction> Executor<T> for Limit<T> {
    fn execute(self: Box<Self>, txn: &mut T) -> Result<super::ResultSet> {
        // 先计算出来limit的value
        let limit = self.limit.evaluate(None)?;
        match limit {
            Value::Integer(i) => match self.source.execute(txn)? {
                ResultSet::Query { columns, rows } => Ok(ResultSet::Query {
                    columns,
                    rows: rows.into_iter().take(i as usize).collect(),
                }),
                r => Err(Error::Executor(format!(
                    "expect get resultset::query but get {:?}",
                    r
                ))),
            },
            unexpect => Err(Error::Executor(format!(
                "get unexpect limit value {}",
                unexpect
            ))),
        }
    }
}
pub struct Offset<T: Transaction> {
    source: Box<dyn Executor<T>>,
    offset: Expression,
}

impl<T: Transaction> Offset<T> {
    pub fn new(source: Box<dyn Executor<T>>, offset: Expression) -> Box<Self> {
        Box::new(Self { source, offset })
    }
}
impl<T: Transaction> Executor<T> for Offset<T> {
    fn execute(self: Box<Self>, txn: &mut T) -> Result<super::ResultSet> {
        let offset = self.offset.evaluate(None)?;
        match offset {
            Value::Integer(i) => match self.source.execute(txn)? {
                ResultSet::Query { columns, rows } => Ok(ResultSet::Query {
                    columns,
                    rows: rows.into_iter().skip(i as usize).collect(),
                }),
                r => Err(Error::Executor(format!(
                    "expect get resultset::query but get {:?}",
                    r
                ))),
            },
            unexpect => Err(Error::Executor(format!(
                "get unexpect offset value {}",
                unexpect
            ))),
        }
    }
}
