use std::{
    borrow::Cow,
    cmp::Ordering,
    default,
    fmt::{self, format, Display},
    hash::Hasher,
};

use crate::errors::{Error, Result};
use core::hash::Hash;
use serde_derive::{Deserialize, Serialize};

use self::engine::Transaction;

pub mod engine;
pub mod execution;
pub mod expression;
pub mod parser;
mod plan;
pub mod schema;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum Value {
    Null,
    Integer(i64),
    Float(f64),
    String(String),
    Bool(bool),
}

impl Value {
    fn is_visiable(&self) -> Result<bool> {
        match self {
            Value::Null => Ok(false),
            Value::Bool(b) => Ok(*b),
            r => Err(Error::Evaluate(format!("expected boolean get {}", r))),
        }
    }
}

impl Value {
    fn datatype(&self) -> Option<ColumnType> {
        match &self {
            Value::Null => None,
            Value::Integer(_) => Some(ColumnType::Integer),
            Value::Float(_) => Some(ColumnType::Float),
            Value::String(_) => Some(ColumnType::String),
            Value::Bool(_) => Some(ColumnType::Bool),
        }
    }
}
impl std::cmp::Eq for Value {}

impl Hash for Value {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.datatype().hash(state);
        match self {
            Value::Null => self.hash(state),
            Value::Bool(v) => v.hash(state),
            Value::Integer(v) => v.hash(state),
            Value::Float(v) => v.to_be_bytes().hash(state),
            Value::String(v) => v.hash(state),
        }
    }
}

impl std::fmt::Display for Value {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        f.write_str(
            match self {
                Self::Null => "NULL".to_string(),
                Self::Bool(b) if *b => "TRUE".to_string(),
                Self::Bool(_) => "FALSE".to_string(),
                Self::Integer(i) => i.to_string(),
                Self::Float(f) => f.to_string(),
                Self::String(s) => s.clone(),
            }
            .as_ref(),
        )
    }
}

impl<'a> From<Value> for Cow<'a, Value> {
    fn from(v: Value) -> Self {
        Cow::Owned(v)
    }
}

impl<'a> From<&'a Value> for Cow<'a, Value> {
    fn from(v: &'a Value) -> Self {
        Cow::Borrowed(v)
    }
}

impl PartialOrd for Value {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        match (self, other) {
            (Self::Null, Self::Null) => Some(Ordering::Equal),
            (Self::Null, _) => Some(Ordering::Less),
            (_, Self::Null) => Some(Ordering::Greater),
            (Self::Bool(a), Self::Bool(b)) => a.partial_cmp(b),
            (Self::Float(a), Self::Float(b)) => a.partial_cmp(b),
            (Self::Float(a), Self::Integer(b)) => a.partial_cmp(&(*b as f64)),
            (Self::Integer(a), Self::Float(b)) => (*a as f64).partial_cmp(b),
            (Self::Integer(a), Self::Integer(b)) => a.partial_cmp(b),
            (Self::String(a), Self::String(b)) => a.partial_cmp(b),
            (_, _) => None,
        }
    }
}

impl From<bool> for Value {
    fn from(v: bool) -> Self {
        Value::Bool(v)
    }
}

impl From<f64> for Value {
    fn from(v: f64) -> Self {
        Value::Float(v)
    }
}

impl From<i64> for Value {
    fn from(v: i64) -> Self {
        Value::Integer(v)
    }
}

impl From<String> for Value {
    fn from(v: String) -> Self {
        Value::String(v)
    }
}

impl From<&str> for Value {
    fn from(v: &str) -> Self {
        Value::String(v.to_owned())
    }
}

/// 列
#[derive(Clone, Debug, PartialEq, Deserialize, Serialize)]
pub struct Column {
    /// 字段名称
    pub name: String,
    /// 字段类型
    pub column_type: ColumnType,
    /// 主键
    pub primary_key: bool,
    /// 是否可以为null
    pub nullable: bool,
    /// 默认值
    pub default: Option<Value>,
    /// 是否是唯一
    pub unique: bool,
    /// 是否是索引
    pub index: bool,
}

impl Column {
    // 检查一个数据是否正常
    pub fn validate_value(
        &self,
        table: &Table,
        pk: &Value,
        val: &Value,
        txn: &mut dyn Transaction,
    ) -> Result<()> {
        // 检查数据类型
        match val.datatype() {
            None => {
                if self.nullable {
                    Ok(())
                } else {
                    Err(Error::Row(format!(
                        "get unexpected null value in column {}",
                        self.name
                    )))
                }
            }
            Some(val) => {
                if val != self.column_type {
                    Err(Error::Row(format!(
                        "invalid column type {} for {} column expect type : {}",
                        val, self.name, self.column_type
                    )))
                } else {
                    Ok(())
                }
            }
        }?;
        // 校验唯一值
        // 如果不是主键的话，而且不是null(主键在之后会校验)
        if self.unique && !self.primary_key && val != &Value::Null {
            let index = table.get_column_index(&self.name)?;
            // 如果是index（索引）
            if self.index {
                let entry = txn.read_index(&table.name, &self.name, val)?;
                if !entry.is_empty() {
                    return Err(Error::Row(format!(
                        "Unique value {} already exists for index column {}",
                        val, self.name
                    )));
                }
            } else {
                //得到这个字段是表中的第几个字段
                let scan = txn.scan(&table.name, None)?;
                for item in scan.iter() {
                    if item.get(index).unwrap_or(&Value::Null) == val
                        && &table.get_row_key(&item)? != pk
                    {
                        return Err(Error::Row(format!(
                            "Unique value {} already exists for column {}",
                            val, self.name
                        )));
                    }
                }
            }
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Hash, Serialize, Deserialize)]
pub enum ColumnType {
    Integer,
    Float,
    String,
    Bool,
}

impl std::fmt::Display for ColumnType {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        f.write_str(match self {
            Self::Bool => "BOOLEAN",
            Self::Integer => "INTEGER",
            Self::Float => "FLOAT",
            Self::String => "STRING",
        })
    }
}

/// 表
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Table {
    pub name: String,
    pub columns: Vec<Column>,
}
impl Table {
    pub fn check_row(&self, row: &[Value], txn: &mut dyn Transaction) -> Result<()> {
        // 先判断行数
        if self.columns.len() != row.len() {
            return Err(Error::Table(format!(
                "need columns len is {} get len is {}",
                self.columns.len(),
                row.len()
            )));
        }

        let pk = self.get_row_key(row)?;

        for (column, value) in self.columns.iter().zip(row.iter()) {
            column.validate_value(self, &pk, value, txn)?;
        }

        return Ok(());
    }

    fn get_row_key(&self, row: &[Value]) -> Result<Value> {
        row.get(
            self.columns
                .iter()
                .position(|r| r.primary_key)
                .ok_or_else(|| {
                    Error::Table(format!("the table {} cannot find primary key", self.name))
                })?,
        )
        .cloned()
        .ok_or_else(|| Error::Row("cannt find primary key in this row".to_string()))
    }

    fn get_column_index(&self, name: &str) -> Result<usize> {
        self.columns
            .iter()
            .position(|c| c.name == name)
            .ok_or(Error::Table(format!("cannt get column {}", name)))
    }

    fn validate(&self, arg: &mut engine::kv::KvTransaction) -> Result<()> {
        if self.columns.iter().filter(|c| c.primary_key).count() != 1 {
            return Err(Error::Table(
                "database currently only supports single index ".to_string(),
            ));
        }
        for ele in self.columns.iter() {
            // 主键不可以是null
            if ele.primary_key && ele.nullable {
                return Err(Error::Table(format!("primary_key cannot be nullable")));
            }

            // 看一下默认值
            if let Some(default) = &ele.default {
                if let Some(datatype) = default.datatype() {
                    if datatype != ele.column_type {
                        return Err(Error::Table(format!(
                            "datatype of default value is {}, but datatype of column is {}",
                            datatype, ele.column_type
                        )));
                    }
                } else if !ele.nullable {
                    return Err(Error::Table(format!(
                        "cannot use null default value with not nullable column"
                    )));
                }
            } else if ele.nullable {
                // 如果没有默认值 并且is not null
                return Err(Error::Table(format!(
                    "the table {} of column {} is nullable but hasn't a default value",
                    self.name, ele.name
                )));
            }
        }
        Ok(())
    }

    fn get_key_index(&self) -> Result<usize> {
        self.columns
            .iter()
            .position(|c| c.primary_key)
            .ok_or(Error::Table(format!(
                "error get table key index {}",
                self.name
            )))
    }
}

/// 排序类型
#[derive(Clone, Debug, PartialEq, Deserialize, Serialize)]
pub enum OrderType {
    ASC,
    DES,
}

impl Display for OrderType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}",
            match self {
                Self::ASC => "asc",
                Self::DES => "desc",
            }
        )
    }
}
