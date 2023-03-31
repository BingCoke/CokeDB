use std::fmt::{Display, self};

mod parser;
mod plan;
pub mod expression;
pub mod schema;

#[derive(Clone, Debug, PartialEq)]
pub enum Value {
    Null,
    Integer(i64),
    Float(f64),
    String(String),
    Bool(bool),
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



/// 列
#[derive(Clone, Debug, PartialEq)]
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

#[derive(Clone, Debug, PartialEq)]
pub enum ColumnType {
    Integer,
    Float,
    String,
    Bool,
}


/// 表
#[derive(Clone, Debug, PartialEq)]
pub struct Table {
    pub name: String,
    pub columns: Vec<Column>,
}

/// 排序类型
#[derive(Clone, Debug, PartialEq)]
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





