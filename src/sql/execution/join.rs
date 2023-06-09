use std::{collections::HashMap, todo};

use crate::sql::{
    engine::{Row, Transaction},
    execution::ResultSet,
    expression::Expression,
    Column, Value,
};

use super::Executor;

use crate::errors::*;
/// 连接join的执行器 检查一下左表是否和右表能够连接
pub struct NestedLoopJoin<T: Transaction> {
    left: Box<dyn Executor<T>>,
    right: Box<dyn Executor<T>>,
    predicate: Option<Expression>,
    outer: bool,
}

impl<T: Transaction> NestedLoopJoin<T> {
    pub fn new(
        left: Box<dyn Executor<T>>,
        right: Box<dyn Executor<T>>,
        predicate: Option<Expression>,
        outer: bool,
    ) -> Box<Self> {
        Box::new(Self {
            left,
            right,
            predicate,
            outer,
        })
    }

    pub fn generate_row(
        left: Vec<Row>,
        right: Vec<Row>,
        predicate: Option<Expression>,
        outer: bool,
    ) -> Result<Vec<Row>> {
        let mut res: Vec<Row> = Vec::new();
        let empty: Vec<_> = std::iter::repeat(Value::Null).take(right.len()).collect();
        for lrow in left {
            let mut base_res: Vec<Row> = Vec::new();
            for rrow in right.clone() {
                let mut row = lrow.clone();
                row.extend(rrow);
                if let Some(predicate) = &predicate {
                    if predicate.evaluate(Some(&row))?.is_visiable()? {
                        base_res.push(row)
                    } else if outer {
                    }
                } else {
                    base_res.push(row)
                }
            }
            // 没有找到 并且 是个外连接
            if res.len() == 0 && outer {
                let mut row = lrow.clone();
                row.extend(empty.clone());
                res.push(row);
            }
        }

        Ok(res)
    }
}

impl<T: Transaction> Executor<T> for NestedLoopJoin<T> {
    fn execute(self: Box<Self>, txn: &mut T) -> crate::errors::Result<super::ResultSet> {
        let (column, lrow, rrow): (Vec<Option<String>>, Vec<Row>, Vec<Row>) =
            match self.left.execute(txn)? {
                ResultSet::Query { mut columns, rows } => match self.right.execute(txn)? {
                    ResultSet::Query {
                        columns: rcolumns,
                        rows: rrows,
                    } => {
                        columns.extend(rcolumns);
                        Ok((columns, rows, rrows))
                    }
                    r => Err(Error::Executor(format!(
                        "expect query ResultSet get {:?}",
                        r
                    ))),
                },
                r => Err(Error::Executor(format!(
                    "expect query ResultSet get {:?}",
                    r
                ))),
            }?;

        let rows = Self::generate_row(lrow, rrow, self.predicate, self.outer)?;
        Ok(ResultSet::Query {
            columns: column,
            rows,
        })
    }
}

/// HashJoin 这里的执行比较简单
/// 就是直接用右表构建成为一个hashmap 然后左表对应寻找
pub struct HashJoin<T: Transaction> {
    left: Box<dyn Executor<T>>,
    left_field: usize,
    right: Box<dyn Executor<T>>,
    right_field: usize,
    outer: bool,
}

impl<T: Transaction> HashJoin<T> {
    pub fn new(
        left: Box<dyn Executor<T>>,
        left_field: usize,
        right: Box<dyn Executor<T>>,
        right_field: usize,
        outer: bool,
    ) -> Box<Self> {
        Box::new(Self {
            left,
            left_field,
            right,
            right_field,
            outer,
        })
    }
}

impl<T: Transaction> Executor<T> for HashJoin<T> {
    fn execute(self: Box<Self>, txn: &mut T) -> Result<ResultSet> {
        match self.left.execute(txn)? {
            ResultSet::Query { mut columns, rows } => match self.right.execute(txn)? {
                ResultSet::Query {
                    columns: rcolumns,
                    rows: rrows,
                } => {
                    let mut res: Vec<Row> = Vec::new();
                    // 将右表形成hashmap
                    let rmap: HashMap<_, _> = rrows
                        .into_iter()
                        .map(|row| {
                            if row.len() < self.right_field {
                                // 越界
                                Err(Error::Executor(format!(
                                    "out of bounds at right list with index {}",
                                    self.right_field
                                )))
                            } else {
                                Ok((row[self.right_field].clone(), row))
                            }
                        })
                        .collect::<Result<_>>()?;

                    let empty: Vec<_> = std::iter::repeat(Value::Null)
                        .take(rcolumns.len())
                        .collect();

                    columns.extend(rcolumns);

                    for mut lrow in rows {
                        if lrow.len() < self.left_field {
                            return Err(Error::Executor(format!(
                                "out of bounds at left list with index {}",
                                self.right_field
                            )));
                        }
                        let rrow = rmap.get(&lrow[self.left_field]);
                        match rrow {
                            Some(row) => lrow.extend(row.clone()),
                            None if self.outer => lrow.extend(empty.clone()),
                            None => {}
                        }
                        if lrow.len() == columns.len() {
                            res.push(lrow);
                        }
                    }

                    Ok(ResultSet::Query { columns, rows: res })
                }
                r => Err(Error::Executor(format!(
                    "expect query ResultSet get {:?}",
                    r
                ))),
            },
            r => Err(Error::Executor(format!(
                "expect query ResultSet get {:?}",
                r
            ))),
        }
    }
}
