use std::collections::BTreeMap;

use crate::errors::Result;

use crate::sql::{ColumnType, OrderType, Value};
/// Statements
#[derive(Clone, Debug, PartialEq)]
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
        from: Option<FromItem>,
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

/// Expressions
#[derive(Clone, Debug, PartialEq)]
#[allow(unconditional_recursion)]
pub enum BaseExpression {
    Field(Option<String>, String),
    Column(usize),
    Value(Value),
    Function(String, Box<BaseExpression>),
    Operation(Operation),
}

#[derive(Clone, Debug, PartialEq)]
pub enum Operation {
    // 负
    Negative(Box<BaseExpression>),
    // 正
    Plus(Box<BaseExpression>),

    And(Box<BaseExpression>, Box<BaseExpression>),
    Or(Box<BaseExpression>, Box<BaseExpression>),

    Like(Box<BaseExpression>, Box<BaseExpression>),

    Equal(Box<BaseExpression>, Box<BaseExpression>),
    NotEqual(Box<BaseExpression>, Box<BaseExpression>),
    GreaterThan(Box<BaseExpression>, Box<BaseExpression>),
    GreaterThanOrEqual(Box<BaseExpression>, Box<BaseExpression>),
    LessThan(Box<BaseExpression>, Box<BaseExpression>),
    LessThanOrEqual(Box<BaseExpression>, Box<BaseExpression>),

    Add(Box<BaseExpression>, Box<BaseExpression>),
    Subtract(Box<BaseExpression>, Box<BaseExpression>),
    Multiply(Box<BaseExpression>, Box<BaseExpression>),
    Divide(Box<BaseExpression>, Box<BaseExpression>),
    Exponentiate(Box<BaseExpression>, Box<BaseExpression>),

    Not(Box<BaseExpression>),

    IsNull(Box<BaseExpression>),
}

impl BaseExpression {
    /// 所有者进行转换 类比于二叉树，这里就是对二叉树某个节点进行转换
    /// before就是前序遍历转换 after就是后序遍历转换
    /// 这里的闭包需要传递多次 所以需要借用
    pub fn transform<A, B>(mut self, before: &mut A, after: &mut B) -> Result<Self>
    where
        A: FnMut(Self) -> Result<Self>,
        B: FnMut(Self) -> Result<Self>,
    {
        self = before(self)?;
        match &mut self {
            BaseExpression::Operation(Operation::Add(lhs, rhs))
            | Self::Operation(Operation::And(lhs, rhs))
            | Self::Operation(Operation::Divide(lhs, rhs))
            | Self::Operation(Operation::Equal(lhs, rhs))
            | Self::Operation(Operation::Exponentiate(lhs, rhs))
            | Self::Operation(Operation::GreaterThan(lhs, rhs))
            | Self::Operation(Operation::GreaterThanOrEqual(lhs, rhs))
            | Self::Operation(Operation::LessThan(lhs, rhs))
            | Self::Operation(Operation::LessThanOrEqual(lhs, rhs))
            | Self::Operation(Operation::Like(lhs, rhs))
            | Self::Operation(Operation::Multiply(lhs, rhs))
            | Self::Operation(Operation::Or(lhs, rhs))
            | Self::Operation(Operation::NotEqual(lhs, rhs))
            | Self::Operation(Operation::Subtract(lhs, rhs)) => {
                lhs.transform_ref(before, after)?;
                rhs.transform_ref(before, after)?;
            }
            Self::Operation(Operation::Plus(expr))
            | Self::Operation(Operation::Negative(expr))
            | Self::Operation(Operation::IsNull(expr))
            | Self::Function(_, expr)
            | Self::Operation(Operation::Not(expr)) => {
                expr.transform_ref(before, after)?;
            }
            Self::Value(_) | Self::Field(_, _) | Self::Column(_) => {}
        };
        after(self)
    }

    /// 借用 进行转换
    pub fn transform_ref<A, B>(&mut self, before: &mut A, after: &mut B) -> Result<()>
    where
        A: FnMut(Self) -> Result<Self>,
        B: FnMut(Self) -> Result<Self>,
    {
        // 直接内存转换
        let tmp = std::mem::replace(self, BaseExpression::Value(Value::Null));
        // 这样就拿到所有权了
        *self = tmp.transform(before, after)?;
        Ok(())
    }

    pub fn contains<F>(&self, predicate: &F) -> bool
    where
        F: Fn(&BaseExpression) -> bool,
    {
        use Operation::*;
        predicate(self)
            || match self {
                Self::Operation(Add(lhs, rhs))
                | Self::Operation(And(lhs, rhs))
                | Self::Operation(Divide(lhs, rhs))
                | Self::Operation(Equal(lhs, rhs))
                | Self::Operation(Exponentiate(lhs, rhs))
                | Self::Operation(GreaterThan(lhs, rhs))
                | Self::Operation(GreaterThanOrEqual(lhs, rhs))
                | Self::Operation(LessThan(lhs, rhs))
                | Self::Operation(LessThanOrEqual(lhs, rhs))
                | Self::Operation(Like(lhs, rhs))
                | Self::Operation(Multiply(lhs, rhs))
                | Self::Operation(NotEqual(lhs, rhs))
                | Self::Operation(Or(lhs, rhs))
                | Self::Operation(Subtract(lhs, rhs)) => {
                    lhs.contains(predicate) || rhs.contains(predicate)
                },
                Self::Function(_, expr)
                | Self::Operation(Plus(expr))
                | Self::Operation(Negative(expr))
                | Self::Operation(IsNull(expr))
                | Self::Operation(Not(expr)) => expr.contains(predicate),
                // 如果上面的predicate失败 这里也就是false
                Self::Value(_) | Self::Field(_, _) | Self::Column(_) => false,
            }
    }

    pub fn contains_aggreate(&self) -> bool {
        self.contains(&|e|{
            match e {
                BaseExpression::Function(_,_ ) => true,
                _ => false,
            }
        })
    }
}
