use std::fmt::{self, Display};


use regex::Regex;

use super::Value;
use crate::errors::{Error, Result};

#[derive(Debug, PartialEq, Clone)]
pub enum Expression {
    /// 常量
    Constant(Value),
    /// 查询字段 usize 表示从上一个节点的结果中 获取第几列数据
    /// option 用于列的命名，表名+ 字段名 或者就直接 列名
    Field(usize, Option<(Option<String>, String)>),
    // 逻辑
    And(Box<Expression>, Box<Expression>),

    Or(Box<Expression>, Box<Expression>),
    Not(Box<Expression>),
    IsNull(Box<Expression>),

    /// 比大小 大于等于会变成Or(LessThan,Equal)
    Equal(Box<Expression>, Box<Expression>),
    GreaterThan(Box<Expression>, Box<Expression>),
    LessThan(Box<Expression>, Box<Expression>),

    ///  数学运算 加减乘除 乘方
    Add(Box<Expression>, Box<Expression>),
    Subtract(Box<Expression>, Box<Expression>),
    Multiply(Box<Expression>, Box<Expression>),
    Divide(Box<Expression>, Box<Expression>),
    Exponentiate(Box<Expression>, Box<Expression>),

    /// 正负号
    Plus(Box<Expression>),
    Negative(Box<Expression>),

    /// 模糊匹配 待定
    Like(Box<Expression>, Box<Expression>),
}

impl Expression {
    pub fn evaluate(&self, row: Option<&Vec<Value>>, constant: bool) -> Result<Value> {
        use Value::*;
        Ok(match self {
            // 常量计算
            Self::Constant(c) => c.clone(),
            
            Self::Field(i, _) => {
                row.and_then(|row| row.get(*i).cloned()).unwrap_or(Null)
            },

            // 逻辑运算
            Self::And(lhs, rhs) => match (lhs.evaluate(row,constant)?, rhs.evaluate(row,constant)?) {
                (Bool(lhs), Bool(rhs)) => Bool(lhs && rhs),
                (Bool(lhs), Value::Null) if !lhs => Bool(false),
                (Bool(_), Value::Null) => Null,
                (Value::Null, Bool(rhs)) if !rhs => Bool(false),
                (Null, Bool(_)) => Null,
                (Null, Null) => Null,
                (lhs, rhs) => {
                    return Err(Error::Evaluate(format!("Can't and {} and {}", lhs, rhs)))
                }
            },
            Self::Not(expr) => match expr.evaluate(row, constant)? {
                Bool(b) => Bool(!b),
                Null => Null,
                value => return Err(Error::Evaluate(format!("Can't negate {}", value))),
            },
            Self::Or(lhs, rhs) => match (lhs.evaluate(row, constant)?, rhs.evaluate(row, constant)?) {
                (Bool(lhs), Bool(rhs)) => Bool(lhs || rhs),
                (Bool(lhs), Null) if lhs => Bool(true),
                (Bool(_), Null) => Null,
                (Null, Bool(rhs)) if rhs => Bool(true),
                (Null, Bool(_)) => Null,
                (Null, Null) => Null,
                (lhs, rhs) => return Err(Error::Evaluate(format!("Can't or {} and {}", lhs, rhs))),
            },

            // 比较
            Self::Equal(lhs, rhs) => match (lhs.evaluate(row, constant)?, rhs.evaluate(row, constant)?) {
                (Bool(lhs), Bool(rhs)) => Bool(lhs == rhs),
                (Integer(lhs), Integer(rhs)) => Bool(lhs == rhs),
                (Integer(lhs), Float(rhs)) => Bool(lhs as f64 == rhs),
                (Float(lhs), Integer(rhs)) => Bool(lhs == rhs as f64),
                (Float(lhs), Float(rhs)) => Bool(lhs == rhs),
                (String(lhs), String(rhs)) => Bool(lhs == rhs),
                (Null, _) | (_, Null) => Null,
                (lhs, rhs) => {
                    return Err(Error::Evaluate(format!(
                        "Can't compare {} and {}",
                        lhs, rhs
                    )))
                }
            },
            Self::GreaterThan(lhs, rhs) => match (lhs.evaluate(row,constant)?, rhs.evaluate(row,constant)?) {
                (Bool(lhs), Bool(rhs)) => Bool(lhs > rhs),
                (Integer(lhs), Integer(rhs)) => Bool(lhs > rhs),
                (Integer(lhs), Float(rhs)) => Bool(lhs as f64 > rhs),
                (Float(lhs), Integer(rhs)) => Bool(lhs > rhs as f64),
                (Float(lhs), Float(rhs)) => Bool(lhs > rhs),
                (String(lhs), String(rhs)) => Bool(lhs > rhs),
                (Value::Null, _) | (_, Value::Null) => Value::Null,
                (lhs, rhs) => {
                    return Err(Error::Evaluate(format!(
                        "Can't compare {} and {}",
                        lhs, rhs
                    )))
                }
            },
            Self::LessThan(lhs, rhs) => match (lhs.evaluate(row, constant)?, rhs.evaluate(row, constant)?) {
                (Bool(lhs), Bool(rhs)) => Bool(lhs < rhs),
                (Integer(lhs), Integer(rhs)) => Bool(lhs < rhs),
                (Integer(lhs), Float(rhs)) => Bool((lhs as f64) < rhs),
                (Float(lhs), Integer(rhs)) => Bool(lhs < rhs as f64),
                (Float(lhs), Float(rhs)) => Bool(lhs < rhs),
                (String(lhs), String(rhs)) => Bool(lhs < rhs),
                (Value::Null, _) | (_, Value::Null) => Value::Null,
                (lhs, rhs) => {
                    return Err(Error::Evaluate(format!(
                        "Can't compare {} and {}",
                        lhs, rhs
                    )))
                }
            },
            Self::IsNull(expr) => match expr.evaluate(row, constant)? {
                Null => Bool(true),
                _ => Bool(false),
            },

            // 数学运算
            Self::Negative(expr) => match expr.evaluate(row ,constant)? {
                Integer(i) => Integer(-i),
                Float(f) => Float(-f),
                Null => Null,
                value => return Err(Error::Evaluate(format!("Can't negate {}", value))),
            },
            Self::Plus(expr) => match expr.evaluate(row, constant)? {
                Float(f) => Float(f),
                Integer(i) => Integer(i),
                Null => Null,
                expr => {
                    return Err(Error::Evaluate(format!(
                        "Can't take the positive of {}",
                        expr
                    )))
                }
            },
            Self::Add(lhs, rhs) => match (lhs.evaluate(row, constant)?, rhs.evaluate(row, constant)?) {
                (Integer(lhs), Integer(rhs)) => Integer(
                    lhs.checked_add(rhs)
                        .ok_or_else(|| Error::Evaluate("Integer overflow".into()))?,
                ),
                (Integer(lhs), Float(rhs)) => Float(lhs as f64 + rhs),
                (Integer(_), Null) => Null,
                (Float(lhs), Float(rhs)) => Float(lhs + rhs),
                (Float(lhs), Integer(rhs)) => Float(lhs + rhs as f64),
                (Float(_), Null) => Null,
                (Null, Float(_)) => Null,
                (Null, Integer(_)) => Null,
                (Null, Value::Null) => Null,
                (lhs, rhs) => {
                    return Err(Error::Evaluate(format!("Can't add {} and {}", lhs, rhs)))
                }
            },
            Self::Divide(lhs, rhs) => match (lhs.evaluate(row, constant)?, rhs.evaluate(row, constant)?) {
                (Integer(_), Integer(rhs)) if rhs == 0 => {
                    return Err(Error::Evaluate("Can't divide by zero".into()))
                }
                (Integer(lhs), Integer(rhs)) => Integer(lhs / rhs),
                (Integer(lhs), Float(rhs)) => Float(lhs as f64 / rhs),
                (Integer(_), Null) => Null,
                (Float(lhs), Integer(rhs)) => Float(lhs / rhs as f64),
                (Float(lhs), Float(rhs)) => Float(lhs / rhs),
                (Float(_), Null) => Null,
                (Null, Float(_)) => Null,
                (Null, Integer(_)) => Null,
                (Null, Value::Null) => Null,
                (lhs, rhs) => {
                    return Err(Error::Evaluate(format!("Can't divide {} and {}", lhs, rhs)))
                }
            },
            Self::Multiply(lhs, rhs) => match (lhs.evaluate(row, constant)?, rhs.evaluate(row,constant)?) {
                (Integer(lhs), Integer(rhs)) => Integer(
                    lhs.checked_mul(rhs)
                        .ok_or_else(|| Error::Evaluate("Integer overflow".into()))?,
                ),
                (Integer(lhs), Float(rhs)) => Float(lhs as f64 * rhs),
                (Integer(_), Null) => Null,
                (Float(lhs), Integer(rhs)) => Float(lhs * rhs as f64),
                (Float(lhs), Float(rhs)) => Float(lhs * rhs),
                (Float(_), Null) => Null,
                (Null, Float(_)) => Null,
                (Null, Integer(_)) => Null,
                (Null, Value::Null) => Null,
                (lhs, rhs) => {
                    return Err(Error::Evaluate(format!(
                        "Can't multiply {} and {}",
                        lhs, rhs
                    )))
                }
            },
            Self::Subtract(lhs, rhs) => match (lhs.evaluate(row,constant)?, rhs.evaluate(row,constant)?) {
                (Integer(lhs), Integer(rhs)) => Integer(
                    lhs.checked_sub(rhs)
                        .ok_or_else(|| Error::Evaluate("Integer overflow".into()))?,
                ),
                (Integer(lhs), Float(rhs)) => Float(lhs as f64 - rhs),
                (Integer(_), Null) => Null,
                (Float(lhs), Integer(rhs)) => Float(lhs - rhs as f64),
                (Float(lhs), Float(rhs)) => Float(lhs - rhs),
                (Float(_), Null) => Null,
                (Null, Float(_)) => Null,
                (Null, Integer(_)) => Null,
                (Null, Value::Null) => Null,
                (lhs, rhs) => {
                    return Err(Error::Evaluate(format!(
                        "Can't subtract {} and {}",
                        lhs, rhs
                    )))
                }
            },

            Self::Exponentiate(lhs, rhs) => match (lhs.evaluate(row,constant)?, rhs.evaluate(row,constant)?) {
                (Integer(lhs), Integer(rhs)) if rhs >= 0 => Integer(
                    lhs.checked_pow(rhs as u32)
                        .ok_or_else(|| Error::Evaluate("Integer overflow".into()))?,
                ),
                (Integer(lhs), Integer(rhs)) => Float((lhs as f64).powf(rhs as f64)),
                (Integer(lhs), Float(rhs)) => Float((lhs as f64).powf(rhs)),
                (Integer(_), Null) => Null,
                (Float(lhs), Integer(rhs)) => Float((lhs).powi(rhs as i32)),
                (Float(lhs), Float(rhs)) => Float((lhs).powf(rhs)),
                (Float(_), Null) => Null,
                (Null, Float(_)) => Null,
                (Null, Integer(_)) => Null,
                (Null, Value::Null) => Null,
                (lhs, rhs) => {
                    return Err(Error::Evaluate(format!(
                        "Can't exponentiate {} and {}",
                        lhs, rhs
                    )))
                }
            },
            // 字符串操作
            Self::Like(lhs, rhs) => match (lhs.evaluate(row,constant)?, rhs.evaluate(row,constant)?) {
                (String(lhs), String(rhs)) => Bool(
                    Regex::new(&format!(
                        "^{}$",
                        regex::escape(&rhs)
                            .replace("%", ".*")
                            .replace(".*.*", "%")
                            .replace("_", ".")
                            .replace("..", "_")
                    ))?
                    .is_match(&lhs),
                ),
                (String(_), Null) => Null,
                (Null, String(_)) => Null,
                (lhs, rhs) => {
                    return Err(Error::Evaluate(format!("Can't LIKE {} and {}", lhs, rhs)))
                }
            },
        })
    }
}

impl Display for Expression {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let s = match self {
            Self::Constant(v) => v.to_string(),
            Self::Field(i, None) => format!("#{}", i),
            Self::Field(_, Some((None, name))) => name.to_string(),
            Self::Field(_, Some((Some(table), name))) => format!("{}.{}", table, name),

            Self::And(lhs, rhs) => format!("{} AND {}", lhs, rhs),
            Self::Or(lhs, rhs) => format!("{} OR {}", lhs, rhs),
            Self::Not(expr) => format!("NOT {}", expr),

            Self::Equal(lhs, rhs) => format!("{} = {}", lhs, rhs),
            Self::GreaterThan(lhs, rhs) => format!("{} > {}", lhs, rhs),
            Self::LessThan(lhs, rhs) => format!("{} < {}", lhs, rhs),
            Self::IsNull(expr) => format!("{} IS NULL", expr),

            Self::Add(lhs, rhs) => format!("{} + {}", lhs, rhs),
            Self::Plus(expr) => expr.to_string(),
            Self::Divide(lhs, rhs) => format!("{} / {}", lhs, rhs),
            Self::Exponentiate(lhs, rhs) => format!("{} ^ {}", lhs, rhs),
            Self::Multiply(lhs, rhs) => format!("{} * {}", lhs, rhs),
            Self::Negative(expr) => format!("-{}", expr),
            Self::Subtract(lhs, rhs) => format!("{} - {}", lhs, rhs),

            Self::Like(lhs, rhs) => format!("{} LIKE {}", lhs, rhs),
        };
        write!(f, "{}", s)
    }
}
