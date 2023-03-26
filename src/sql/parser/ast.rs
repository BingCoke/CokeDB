use std::collections::BTreeMap;

/// Statements
#[derive(Clone, Debug, PartialEq)]
#[allow(clippy::large_enum_variant)]
pub enum Statement {
    Begin {
        readonly: bool,
        version: Option<u64>,
    },
    Commit,
    Rollback,
    Explain(Box<Statement>),

    CreateTable {
        name: String,
        columns: Vec<Column>,
    },
    DropTable(String),

    Delete {
        table: String,
        filter: Option<BaseExpression>,
    },
    Insert {
        table: String,
        columns: Option<Vec<String>>,
        values: Vec<Vec<BaseExpression>>,
    },
    Update {
        table: String,
        set: BTreeMap<String, BaseExpression>,
        filter: Option<BaseExpression>,
    },

    Select {
        select: Vec<(BaseExpression, Option<String>)>,
        from: Vec<FromItem>,
        filter: Option<BaseExpression>,
        group_by: Vec<BaseExpression>,
        having: Option<BaseExpression>,
        order: Vec<(BaseExpression, OrderType)>,
        offset: Option<BaseExpression>,
        limit: Option<BaseExpression>,
    },
}

/// A FROM item
#[derive(Clone, Debug, PartialEq)]
pub enum FromItem {
    Table {
        name: String,
        alias: Option<String>,
    },
    Join {
        left: Box<FromItem>,
        right: Box<FromItem>,
        join_type: JoinType,
        predicate: Option<BaseExpression>,
    },
}

/// A JOIN type
#[derive(Clone, Debug, PartialEq)]
pub enum JoinType {
    Cross,
    Inner,
    Left,
    Right,
}

/// A column
#[derive(Clone, Debug, PartialEq)]
pub struct Column {
    pub name: String,
    pub column_type: ColumnType,
    pub primary_key: bool,
    pub nullable: Option<bool>,
    pub default: Option<BaseExpression>,
    pub unique: bool,
    pub index: bool,
}

#[derive(Clone, Debug, PartialEq)]
pub enum ColumnType {
    Integer,
    Float,
    String,
    Bool
}


/// Sort orders
#[derive(Clone, Debug, PartialEq)]
pub enum OrderType {
    ASC,
    DES,
}

/// Expressions
#[derive(Clone, Debug, PartialEq)]
pub enum BaseExpression {
    Field(Option<String>, String),
    Value(Value),
    Function(String, Vec<BaseExpression>),
    Operation(Operation),
}

#[derive(Clone, Debug, PartialEq)]
pub enum Operation {

}

#[derive(Clone, Debug, PartialEq)]
pub enum Value {
    None,
    Integer(i64),
    Float(f64),
    String(String),
    Bool(bool)
}





