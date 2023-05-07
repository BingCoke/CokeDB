/* 设置增删改 对于数据的更改操作
 * */

use crate::sql::{expression::Expression, engine::Transaction};

use super::Executor;

pub struct Insert {
    table: String,
    columns: Vec<String>,
    rows: Vec<Vec<Expression>>,
}

pub struct Update<T: Transaction> {
    table: String,
    source: Box<dyn Executor<T>>
}

pub struct Delete<T: Transaction> {
    table: String,
    source: Box<dyn Executor<T>>
}
