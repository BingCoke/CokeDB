use std::cmp::Ordering;
use std::collections::HashMap;

use super::{Executor, ResultSet};
use crate::errors::*;
use crate::sql::execution::source;
use crate::sql::{engine::Transaction, plan::Aggregate, Value};

pub struct Aggregation<T: Transaction> {
    source: Box<dyn Executor<T>>,
    aggregates: Vec<Aggregate>,
    // 记录group by的字段
    accumulators: HashMap<Vec<Value>, Vec<Box<dyn Accumulator>>>,
}
impl<T: Transaction> Executor<T> for Aggregation<T> {
    fn execute(mut self: Box<Self>, txn: &mut T) -> Result<ResultSet> {
        let aggre_size = self.aggregates.len();
        match self.source.execute(txn)? {
            ResultSet::Query { columns, rows } => {
                let mut rows = rows.into_iter();
                while let Some(mut row) = rows.next() {
                    // 为group by的字段设置为key value是其计算器
                    // 例如
                    // group by name
                    // 那么key 有可能是 xiaoming  或者 xiaohong
                    // 如果我们需要count  那么value就会有count计算器
                    // xiaoming对应的value就会记录一共有多少个name=xiaoming
                    let accumulators = self
                        .accumulators
                        .entry(row.split_off(aggre_size))
                        .or_insert(
                            self.aggregates
                                .iter()
                                .map(|v| <dyn Accumulator>::new(v))
                                .collect(),
                        );
                    // 我们在执行 aggregation 之前 已经做过映射了 所以 数据情况应该是
                    // count      sum   group by (1)
                    // xiaoming   age   xiaoming
                    // 也就说 第n个计算器 去row中的第n个数据拿取计算即可
                    accumulators
                        .iter_mut()
                        .zip(row)
                        .try_for_each(|(a, v)| a.accumulate(&v))?;
                }
                // 考虑数据有可能为空
                // 例如 select count(*) from some where 1=2;
                // 或者本身没有group by的情况
                if self.accumulators.is_empty() && self.aggregates.len() == columns.len() {
                    self.accumulators.insert(
                        Vec::new(),
                        self.aggregates
                            .iter()
                            .map(|agg| <dyn Accumulator>::new(agg))
                            .collect(),
                    );
                }

                let columns: Vec<Option<String>> = columns
                    .into_iter()
                    .enumerate()
                    // 聚合操作column是null, group_by保持原来的标签
                    .map(|(i, c)| {
                        if i < aggre_size {
                            self.aggregates.get(i).map(|m| m.to_string())
                        } else {
                            c
                        }
                    })
                    .collect();
                let rows = self
                    .accumulators
                    .into_iter()
                    .map(|(gb, ac)| {
                        let mut row = Vec::new();
                        let r1 = ac.into_iter().map(|a| a.aggregate()).collect::<Vec<_>>();
                        row.extend(r1);
                        row.extend(gb);
                        row
                    })
                    .collect::<Vec<_>>();

                Ok(ResultSet::Query { columns, rows })
            }
            e => {
                return Err(Error::Executor(format!(
                    "expect get a query set but get {:#?}",
                    e
                )))
            }
        }
    }
}

impl<T: Transaction> Aggregation<T> {
    pub fn new(source: Box<dyn Executor<T>>, aggregates: Vec<Aggregate>) -> Box<Self> {
        Box::new(Self {
            source,
            aggregates,
            accumulators: HashMap::new(),
        })
    }
}

// 计算器
pub trait Accumulator: std::fmt::Debug + Send {
    // 放入一个值
    fn accumulate(&mut self, value: &Value) -> Result<()>;

    // 最终值结果的计算
    fn aggregate(&self) -> Value;
}

impl dyn Accumulator {
    fn new(aggregate: &Aggregate) -> Box<dyn Accumulator> {
        match aggregate {
            Aggregate::Average => Box::new(Average::new()),
            Aggregate::Count => Box::new(Count::new()),
            Aggregate::Max => Box::new(Max::new()),
            Aggregate::Min => Box::new(Min::new()),
            Aggregate::Sum => Box::new(Sum::new()),
        }
    }
}

/// counter 计算
/// 计算不是null的数值
#[derive(Debug)]
pub struct Count {
    count: u64,
}

impl Count {
    pub fn new() -> Self {
        Self { count: 0 }
    }
}

impl Accumulator for Count {
    fn accumulate(&mut self, value: &Value) -> Result<()> {
        // 只要不是null 那就+1
        match value {
            Value::Null => {}
            _ => self.count += 1,
        }
        Ok(())
    }

    fn aggregate(&self) -> Value {
        Value::Integer(self.count as i64)
    }
}

// 计算平均值
#[derive(Debug)]
pub struct Average {
    count: Count,
    sum: Sum,
}

impl Average {
    pub fn new() -> Self {
        Self {
            count: Count::new(),
            // 计算sum先
            sum: Sum::new(),
        }
    }
}

impl Accumulator for Average {
    fn accumulate(&mut self, value: &Value) -> Result<()> {
        self.count.accumulate(value)?;
        self.sum.accumulate(value)?;
        Ok(())
    }

    fn aggregate(&self) -> Value {
        match (self.sum.aggregate(), self.count.aggregate()) {
            (Value::Integer(s), Value::Integer(c)) => Value::Integer(s / c),
            (Value::Float(s), Value::Integer(c)) => Value::Float(s / c as f64),
            _ => Value::Null,
        }
    }
}

// 计算max值
#[derive(Debug)]
pub struct Max {
    max: Option<Value>,
}

impl Max {
    pub fn new() -> Self {
        Self { max: None }
    }
}

impl Accumulator for Max {
    fn accumulate(&mut self, value: &Value) -> Result<()> {
        if let Some(max) = &mut self.max {
            if max.datatype() != value.datatype() {
                return Ok(());
            }
            match value.partial_cmp(max) {
                // 防止所有的值是Null
                None => *max = Value::Null,
                Some(Ordering::Greater) => *max = value.clone(),
                Some(Ordering::Equal) | Some(Ordering::Less) => {}
            };
        } else {
            self.max = Some(value.clone())
        }
        Ok(())
    }

    fn aggregate(&self) -> Value {
        match &self.max {
            Some(value) => value.clone(),
            None => Value::Null,
        }
    }
}

// 计算最小值
#[derive(Debug)]
pub struct Min {
    min: Option<Value>,
}

impl Min {
    pub fn new() -> Self {
        Self { min: None }
    }
}

impl Accumulator for Min {
    fn accumulate(&mut self, value: &Value) -> Result<()> {
        if let Some(min) = &mut self.min {
            if min.datatype() != value.datatype() {
                return Ok(());
            }
            match value.partial_cmp(min) {
                None => *min = Value::Null,
                Some(Ordering::Less) => *min = value.clone(),
                Some(Ordering::Equal) | Some(Ordering::Greater) => {}
            };
        } else {
            self.min = Some(value.clone())
        }
        Ok(())
    }

    fn aggregate(&self) -> Value {
        match &self.min {
            Some(value) => value.clone(),
            None => Value::Null,
        }
    }
}

/// 计算总计值
#[derive(Debug)]
pub struct Sum {
    sum: Option<Value>,
}

impl Sum {
    pub fn new() -> Self {
        Self { sum: None }
    }
}

impl Accumulator for Sum {
    fn accumulate(&mut self, value: &Value) -> Result<()> {
        self.sum = match (&self.sum, value) {
            (Some(Value::Integer(s)), Value::Integer(i)) => Some(Value::Integer(s + i)),
            (Some(Value::Float(s)), Value::Float(f)) => Some(Value::Float(s + f)),
            (None, Value::Integer(i)) => Some(Value::Integer(*i)),
            (None, Value::Float(f)) => Some(Value::Float(*f)),
            _ => Some(Value::Null),
        };
        Ok(())
    }

    fn aggregate(&self) -> Value {
        match &self.sum {
            Some(value) => value.clone(),
            None => Value::Null,
        }
    }
}
