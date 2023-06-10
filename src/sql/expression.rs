use std::fmt::{self, Display};

use log::debug;
use regex::Regex;
use serde_derive::{Serialize, Deserialize};

use super::Value;
use crate::errors::{Error, Result};
use std::convert::Into;

#[derive(Debug, PartialEq, Clone, Serialize, Deserialize)]
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
    /// expression进行转换 和BaseExpression一样
    pub fn transform<B, A>(mut self, before: &A, after: &B) -> Result<Self>
    where
        A: Fn(Self) -> Result<Self>,
        B: Fn(Self) -> Result<Self>,
    {
        self = before(self)?;
        match &mut self {
            Self::Add(lhs, rhs)
            | Self::And(lhs, rhs)
            | Self::Divide(lhs, rhs)
            | Self::Equal(lhs, rhs)
            | Self::Exponentiate(lhs, rhs)
            | Self::GreaterThan(lhs, rhs)
            | Self::LessThan(lhs, rhs)
            | Self::Like(lhs, rhs)
            | Self::Multiply(lhs, rhs)
            | Self::Or(lhs, rhs)
            | Self::Subtract(lhs, rhs) => {
                lhs.transform_ref(before, after)?;
                rhs.transform_ref(before, after)?;
            }

            Self::Plus(expr) | Self::Negative(expr) | Self::IsNull(expr) | Self::Not(expr) => {
                expr.transform_ref(before, after)?
            }

            Self::Constant(_) | Self::Field(_, _) => {}
        };
        after(self)
    }
        
    /// 借用 进行转换
    pub fn transform_ref<A, B>(&mut self, before: &A, after: &B) -> Result<()>
    where
        A: Fn(Self) -> Result<Self>,
        B: Fn(Self) -> Result<Self>,
    {
        // 直接内存转换
        let tmp = std::mem::replace(self, Expression::Constant(Value::Null));
        // 这样就拿到所有权了
        *self = tmp.transform(before, after)?;
        Ok(())
    }

    pub fn evaluate(&self, row: Option<&Vec<Value>>) -> Result<Value> {
        use Value::*;
        Ok(match self {
            // 常量计算
            Self::Constant(c) => c.clone(),

            Self::Field(i, _) => {
               let r = row.and_then(|row| row.get(*i).cloned()).unwrap_or(Null);
               debug!("row {} get {}",i,r);
               r
            },

            // 逻辑运算
            Self::And(lhs, rhs) => match (lhs.evaluate(row)?, rhs.evaluate(row)?) {
                (Bool(lhs), Bool(rhs)) => Bool(lhs && rhs),
                (Bool(lhs), Value::Null) if !lhs => Bool(false),
                (Bool(_), Value::Null) => Null,
                (Value::Null, Bool(rhs)) if !rhs => Bool(false),
                (Value::Null, Bool(_)) => Value::Null,
                (Null, Null) => Null,
                (lhs, rhs) => {
                    return Err(Error::Evaluate(format!("Can't and {} and {}", lhs, rhs)))
                }
            },
            Self::Not(expr) => match expr.evaluate(row)? {
                Bool(b) => Bool(!b),
                Null => Null,
                value => return Err(Error::Evaluate(format!("Can't negate {}", value))),
            },
            Self::Or(lhs, rhs) => match (lhs.evaluate(row)?, rhs.evaluate(row)?) {
                (Bool(lhs), Bool(rhs)) => Bool(lhs || rhs),
                (Bool(lhs), Null) if lhs => Bool(true),
                (Bool(_), Null) => Null,
                (Null, Bool(rhs)) if rhs => Bool(true),
                (Null, Bool(_)) => Null,
                (Null, Null) => Null,
                (lhs, rhs) => return Err(Error::Evaluate(format!("Can't or {} and {}", lhs, rhs))),
            },

            // 比较
            Self::Equal(lhs, rhs) => match (lhs.evaluate(row)?, rhs.evaluate(row)?) {
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
            Self::GreaterThan(lhs, rhs) => match (lhs.evaluate(row)?, rhs.evaluate(row)?) {
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
            Self::LessThan(lhs, rhs) => match (lhs.evaluate(row)?, rhs.evaluate(row)?) {
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
            Self::IsNull(expr) => match expr.evaluate(row)? {
                Null => Bool(true),
                _ => Bool(false),
            },

            // 数学运算
            Self::Negative(expr) => match expr.evaluate(row)? {
                Integer(i) => Integer(-i),
                Float(f) => Float(-f),
                Null => Null,
                value => return Err(Error::Evaluate(format!("Can't negate {}", value))),
            },
            Self::Plus(expr) => match expr.evaluate(row)? {
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
            Self::Add(lhs, rhs) => match (lhs.evaluate(row)?, rhs.evaluate(row)?) {
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
            Self::Divide(lhs, rhs) => match (lhs.evaluate(row)?, rhs.evaluate(row)?) {
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
            Self::Multiply(lhs, rhs) => match (lhs.evaluate(row)?, rhs.evaluate(row)?) {
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
            Self::Subtract(lhs, rhs) => match (lhs.evaluate(row)?, rhs.evaluate(row)?) {
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

            Self::Exponentiate(lhs, rhs) => match (lhs.evaluate(row)?, rhs.evaluate(row)?) {
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
            Self::Like(lhs, rhs) => match (lhs.evaluate(row)?, rhs.evaluate(row)?) {
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

    pub fn contains<F>(&self, predicate: &F) -> bool
    where
        F: Fn(&Expression) -> bool,
    {
        predicate(self)
            || match self {
                Self::Add(lhs, rhs)
                | Self::And(lhs, rhs)
                | Self::Divide(lhs, rhs)
                | Self::Equal(lhs, rhs)
                | Self::Exponentiate(lhs, rhs)
                | Self::GreaterThan(lhs, rhs)
                | Self::LessThan(lhs, rhs)
                | Self::Like(lhs, rhs)
                | Self::Multiply(lhs, rhs)
                | Self::Or(lhs, rhs)
                | Self::Subtract(lhs, rhs) => lhs.contains(predicate) && rhs.contains(predicate),

                Self::Plus(expr) | Self::Negative(expr) | Self::IsNull(expr) | Self::Not(expr) => {
                    expr.contains(predicate)
                }
                // 如果visiter就是针对这两个，那么就会在最开始进行判断
                Self::Constant(_) | Self::Field(_, _) => false,
            }
    }

    /// 就是将expression句子 先全部转变成由and连接的子句，再拆分
    pub fn to_cnf_vec(&mut self) -> Result<Vec<Expression>> {
        // not(and(e1,e1)) => or(not(e1),not(e2))
        self.transform_ref(
            &|e| match e {
                Expression::Not(expr) => match *expr {
                    Expression::And(e1, e2) => {
                        Ok(Self::Or(Box::new(Self::Not(e1)), Box::new(Self::Not(e2))))
                    }
                    Expression::Or(e1, e2) => {
                        Ok(Self::And(Box::new(Self::Not(e1)), Box::new(Self::Not(e2))))
                    }
                    Expression::Not(n) => Ok(*n),
                    e => Ok(e),
                },
                _ => Ok(e),
            },
            &|e| Ok(e),
        )?;
        // 之后将 Or(And(e1,e2),e3) => And(Or(e1,e3),Or(e2,e3))
        self.transform_ref(
            &|e| match e {
                Self::Or(lhs, rhs) => match (*lhs, *rhs) {
                    (Self::And(e1, e2), e3) => {
                        Ok(Self::And(Box::new(Self::Or(e1, e2)), Box::new(e3)))
                    }
                    (e3, Self::And(e1, e2)) => {
                        Ok(Self::And(Box::new(Self::Or(e1, e2)), Box::new(e3)))
                    }
                    (e1, e2) => Ok(Self::Or(Box::new(e1), Box::new(e2))),
                },
                _ => Ok(e),
            },
            &|e| Ok(e),
        )?;
        // 之后切分每一个子句
        let mut res = Vec::new();
        let mut stack = Vec::new();
        stack.push(self);
        while let Some(e) = stack.pop() {
            match e {
                Self::And(e1, e2) => {
                    stack.push(e1);
                    stack.push(e2);
                }
                _ => {
                    res.push(e.clone());
                }
            }
        }
        Ok(res)
    }

    pub fn from_cnf_vec(mut cnf: Vec<Expression>) -> Option<Expression> {
        if cnf.is_empty() {
            return None;
        }
        let mut expr = cnf.remove(0);
        while !cnf.is_empty() {
            expr = Expression::And(Box::new(expr), Box::new(cnf.remove(0)));
        }
        return Some(expr);
    }

    /// 查找expression中包含filed=xxx的，
    /// 此时expression应该不包含And子句
    pub fn look_up(&self, filed_index: usize) -> Option<Vec<Value>> {
        use Expression::*;
        match &*self {
            Equal(lhs, rhs) => match (&**lhs, &**rhs) {
                (Field(i, _), Constant(v)) if i == &filed_index => Some(vec![v.clone()]),
                (Constant(v), Field(i, _)) if i == &filed_index => Some(vec![v.clone()]),
                (_, _) => None,
            },
            IsNull(e) => match &**e {
                Field(i, _) if i == &filed_index => Some(vec![Value::Null]),
                _ => None,
            },
            Or(lhs, rhs) => match (lhs.look_up(filed_index), rhs.look_up(filed_index)) {
                (Some(mut lvalues), Some(mut rvalues)) => {
                    lvalues.append(&mut rvalues);
                    Some(lvalues)
                }
                _ => None,
            },
            _ => None,
        }
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
