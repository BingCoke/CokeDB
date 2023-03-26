use std::collections::BTreeMap;
use std::f32::consts::E;
use std::iter::Peekable;

use crate::sql::parser::ast::ColumnType;
use crate::sql::parser::laxer::{Keyword, Token};

use self::ast::{BaseExpression, Column, FromItem, JoinType, OrderType};
use self::{ast::Statement, laxer::Laxer};
use crate::errors::Error;
use crate::errors::Result;

pub mod ast;
pub mod laxer;

pub struct Parser<'a> {
    laxer: Peekable<Laxer<'a>>,
}

impl<'a> Parser<'a> {
    pub fn new(input: &'a str) -> Self {
        let laxer = Laxer::new(input).peekable();
        Parser { laxer }
    }
    pub fn parse(&mut self) -> Result<ast::Statement> {
        let statement = self.get_statement()?;
        self.next_token_expect(Token::Semicolon)?;
        self.next_token_expect_none()?;
        Ok(statement)
    }
    pub fn get_statement(&mut self) -> Result<Statement> {
        match self.laxer.peek() {
            Some(token) => match token {
                Ok(Token::Keyword(Keyword::Create)) => self.parse_create_statement(),
                Ok(Token::Keyword(Keyword::Drop)) => self.parse_drop_statement(),
                Ok(Token::Keyword(Keyword::Select)) => self.parse_select_statement(),
                Ok(Token::Keyword(Keyword::Update)) => self.parse_update_statement(),
                Ok(Token::Keyword(Keyword::Delete)) => self.parse_delete_statement(),
                Ok(Token::Keyword(Keyword::Insert)) => self.parse_insert_statement(),
                Ok(t) => Err(Error::Parse(format!("get unexpected token: {}", t))),
                Err(e) => Err(e.clone()),
            },
            None => Err(Error::Parse("not fount token".to_string())),
        }
    }

    fn parse_explain(&mut self) -> Result<Statement> {
        self.next_token_expect(Token::Keyword(Keyword::Explain))?;
        Ok(Statement::Explain(Box::new(self.get_statement()?)))
    }

    fn parse_create_statement(&mut self) -> Result<Statement> {
        // CREATE TABLE 表名称 (
        // 列名称1 数据类型,
        // 列名称2 数据类型,
        // 列名称3 数据类型
        // )

        self.next_token_expect(Token::Keyword(Keyword::Create))?;
        self.next_token_expect(Token::Keyword(Keyword::Table))?;
        let name = self.next_ident()?;
        self.next_token_expect(Token::OpenParen)?;
        let mut columns: Vec<Column> = vec![];
        loop {
            columns.push(self.parse_column()?);
            // 下一个不是逗号的时候表示结束
            if self.next_token_expect(Token::Comma).is_err() {
                break;
            }
        }
        if columns.is_empty() {
            return Err(Error::Parse(
                "can't create table with zero column".to_string(),
            ));
        }
        self.next_token_expect(Token::CloseParen)?;
        Ok(Statement::CreateTable { name, columns })
    }

    /*
     * name type other(比如是否是主键，是否是索引，是否唯一，是否可以是null, 是有default,)
     *
     */
    fn parse_column(&mut self) -> Result<Column> {
        // get cloumn name
        let name = self.next_ident()?;
        // get column_type
        let column_type = match self.next_keyword()? {
            Keyword::Bool => ColumnType::Bool,
            Keyword::Boolean => ColumnType::Bool,
            Keyword::Char => ColumnType::String,
            Keyword::Double => ColumnType::Float,
            Keyword::Float => ColumnType::Float,
            Keyword::Int => ColumnType::Integer,
            Keyword::Integer => ColumnType::Integer,
            Keyword::String => ColumnType::String,
            Keyword::Text => ColumnType::String,
            Keyword::Varchar => ColumnType::String,
            other => return Err(Error::Parse(format!("Unexpected keyword {}", other))),
        };
        let mut column = Column {
            name,
            column_type,
            primary_key: false,
            nullable: None,
            default: None,
            unique: false,
            index: false,
        };
        while let Ok(keyword) = self.next_keyword() {
            match keyword {
                Keyword::Primary => {
                    self.next_token_expect(Keyword::Key.into())?;
                    column.primary_key = true;
                }
                Keyword::Null => {
                    if let Some(false) = column.nullable {
                        return Err(Error::Parse(format!(
                            "Column {} can't be both not nullable and nullable",
                            column.name
                        )));
                    }
                    column.nullable = Some(true)
                }
                Keyword::Not => {
                    self.next_token_expect(Keyword::Null.into())?;
                    if let Some(true) = column.nullable {
                        return Err(Error::Parse(format!(
                            "Column {} can't be both not nullable and nullable",
                            column.name
                        )));
                    }
                    column.nullable = Some(false)
                }
                Keyword::Default => {
                    let expression = self.parse_expression(0)?;
                    column.default = Some(expression)
                }
                Keyword::Unique => column.unique = true,
                Keyword::Index => column.index = true,
                other => return Err(Error::Parse(format!("unexpected keyword: {}", other))),
            }
        }
        Ok(column)
    }

    fn parse_drop_statement(&mut self) -> Result<Statement> {
        //  drop table table_name;
        self.next_token_expect(Token::Keyword(Keyword::Drop))?;
        self.next_token_expect(Token::Keyword(Keyword::Table))?;
        let table_name = self.next_ident()?;
        Ok(Statement::DropTable(table_name))
    }

    fn parse_update_statement(&mut self) -> Result<Statement> {
        // UPDATE 表名称 SET 列名称 = 新值 WHERE 列名称 = 某值
        // update table_ set name="xiaoming", age=19+1 where expr
        self.next_token_expect(Token::Keyword(Keyword::Update))?;
        let table_name = self.next_ident()?;
        self.next_token_expect(Token::Keyword(Keyword::Set))?;
        let set_expression = self.parse_set_expression()?;

        let mut filter = None;
        if self
            .next_token_expect(Token::Keyword(Keyword::Where))
            .is_ok()
        {
            filter = Some(self.parse_expression(0)?);
        };

        Ok(Statement::Update {
            table: table_name,
            set: set_expression,
            filter,
        })
    }

    fn parse_set_expression(&mut self) -> Result<BTreeMap<String, BaseExpression>> {
        // set column1="hah" , column2=12+2 , column3=-12
        let mut res = BTreeMap::new();
        loop {
            let column = self.next_ident()?;
            let expression = self.parse_expression(0)?;
            res.insert(column, expression);
            if self.next_token_expect(Token::Comma).is_err() {
                break;
            }
        }
        Ok(res)
    }

    fn parse_delete_statement(&mut self) -> Result<Statement> {
        // delete from table_name where (expression)
        self.next_token_expect(Token::Keyword(Keyword::Delete))?;
        self.next_token_expect(Token::Keyword(Keyword::From))?;
        let table_name = self.next_ident()?;
        let mut filter = None;
        if self
            .next_token_expect(Token::Keyword(Keyword::Where))
            .is_ok()
        {
            filter = Some(self.parse_expression(0)?);
        };
        Ok(Statement::Delete {
            table: table_name,
            filter,
        })
    }
    fn parse_insert_statement(&mut self) -> Result<Statement> {
        // INSERT INTO table_name (列1, 列2,...) VALUES (值1, 值2,....),(值1,值2....)
        self.next_token_expect(Token::Keyword(Keyword::Insert))?;
        self.next_token_expect(Token::Keyword(Keyword::Into))?;
        let table_name = self.next_ident()?;
        // 如果下一个是括号 就有columns
        let mut columns = None;
        if self.next_token_expect(Token::OpenParen).is_ok() {
            let mut columnss = Vec::new();
            while let Ok(name) = self.next_ident() {
                columnss.push(name);
                // 逗号分割
                self.next_token_expect(Token::Comma)?;
            }
            self.next_token_expect(Token::CloseParen)?;
            columns = Some(columnss);
        }
        // values关键字必须要有
        self.next_token_expect(Token::Keyword(Keyword::Values))?;
        let mut values = Vec::new();
        loop {
            // 需要括号包裹
            self.next_token_expect(Token::OpenParen)?;
            let mut value = Vec::new();
            loop {
                let expression = self.parse_expression(0)?;
                value.push(expression);
                // 每个value逗号分割
                if self.next_token_expect(Token::Comma).is_err() {
                    break;
                }
            }
            values.push(value);
            self.next_token_expect(Token::CloseParen)?;
            // 如果下一个不是逗号就说明结束了
            if self.next_token_expect(Token::Comma).is_err() {
                break;
            }
        }
        Ok(Statement::Insert {
            table: table_name,
            columns,
            values,
        })
    }

    fn parse_select_statement(&mut self) -> Result<Statement> {
        // 分为多种解析 解析select列，解析 from 解析 wheer 解析 groupby 解析 having 解析orderby
        // 解析 offset 解析 limit
        self.next_token_expect(Keyword::Select.into())?;
        Ok(Statement::Select {
            select: self.parse_select_clause()?,
            from: self.parse_from_claues()?,
            filter: self.parse_where_claues()?,
            group_by: self.parse_grouby_clause()?,
            having: self.parse_having_claues()?,
            order: self.parse_order_claues()?,
            offset: if self.next_token_expect(Keyword::Offset.into()).is_ok() {
                Some(self.parse_expression(0)?)
            } else {
                None
            },
            limit: if self.next_token_expect(Keyword::Limit.into()).is_ok() {
                Some(self.parse_expression(0)?)
            } else {
                None
            },
        })
    }

    fn parse_select_clause(&mut self) -> Result<Vec<(BaseExpression, Option<String>)>> {
        let mut select: Vec<(BaseExpression, Option<String>)> = Vec::new();
        // 首先判断一下是不是select * ...
        if self.next_token_expect(Token::Asterisk).is_ok() {
            return Ok(select);
        }
        // 解析一个expression 然后看一下有没有别名 逗号分割
        loop {
            let expression = self.parse_expression(0)?;
            let mut label = None;
            if let Ok(Keyword::As) = self.next_keyword() {
                // 别名 如果有as 就必须有别名
                label = Some(self.next_ident()?);
            } else if let Ok(label_) = self.next_ident() {
                // 没有as就看一下 下一个token是不是ident
                label = Some(label_);
            }
            select.push((expression, label));
            if self.next_token_expect(Token::Comma).is_err() {
                break;
            }
        }
        Ok(select)
    }

    fn parse_from_claues(&mut self) -> Result<Vec<FromItem>> {
        let mut from = Vec::new();
        // 没有from关键字就返回就好了
        if self
            .next_token_expect(Token::Keyword(Keyword::From))
            .is_err()
        {
            return Ok(from);
        }
        // 首先拿到第一个fromItem
        loop {
            let table = self.parse_join_from(None)?;
            from.push(table);
            if self.next_token_expect(Token::Comma).is_err() {
                break;
            }
        }

        Ok(from)
    }

    fn parse_join_from(&mut self, left: Option<FromItem>) -> Result<FromItem> {
        // from users AS u
        // INNER JOIN addresses AS a ON u.id = a.user_id
        // INNER JOIN orders AS o ON u.id = o.user_id
        // INNER JOIN order_items AS oi ON o.id = oi.order_id
        // INNER JOIN products AS p ON oi.product_id = p.id
        // INNER JOIN categories AS c ON p.category_id = c.id
        match left {
            Some(left) => {
                let join_type = self.parse_join_type()?;
                // 有join type的话说明有和右表连接的情况
                match join_type {
                    Some(join_type) => {
                        let left = Box::new(left);
                        // 拿到右表
                        let right = Box::new(self.parse_table()?);
                        // 看一下是否有on表达式
                        let predicate = match join_type {
                            // 笛卡尔积没有表达式
                            JoinType::Cross => None,
                            _ => {
                                // 有on就说明有 后面要跟一个表达式
                                if self.next_token_expect(Keyword::On.into()).is_ok() {
                                    Some(self.parse_expression(0)?)
                                } else {
                                    // 没有就predicate为none
                                    None
                                }
                            }
                        };
                        let left = ast::FromItem::Join {
                            left,
                            right,
                            join_type,
                            predicate,
                        };
                        self.parse_join_from(Some(left))
                    }
                    // 没有后续就结束了
                    None => Ok(left),
                }
            }
            // 如果是None说明是第一次调用，先拿出一个left table
            None => {
                let left = self.parse_table()?;
                self.parse_join_from(Some(left))
            }
        }
    }

    fn parse_table(&mut self) -> Result<FromItem> {
        let name = self.next_ident()?;
        let alias = if self.next_token_expect(Keyword::As.into()).is_ok() {
            Some(self.next_ident()?)
        } else if let Some(Ok(Token::Ident(_))) = self.laxer.peek() {
            Some(self.next_ident()?)
        } else {
            None
        };
        Ok(FromItem::Table { name, alias })
    }
    /// 解析一个join type 后续没有jointype就返回null
    fn parse_join_type(&mut self) -> Result<Option<JoinType>> {
        if self.next_token_expect(Keyword::Cross.into()).is_ok() {
            self.next_token_expect(Keyword::Join.into())?;
            Ok(Some(ast::JoinType::Cross))
        } else if self.next_token_expect(Keyword::Inner.into()).is_ok() {
            self.next_token_expect(Keyword::Join.into())?;
            Ok(Some(ast::JoinType::Inner))
        } else if self.next_token_expect(Keyword::Join.into()).is_ok() {
            Ok(Some(ast::JoinType::Inner))
        } else if self.next_token_expect(Keyword::Left.into()).is_ok() {
            // 这个outer可有可无 有就消耗掉
            let _ = self.next_token_expect(Keyword::Outer.into());
            self.next_token_expect(Keyword::Join.into())?;
            Ok(Some(ast::JoinType::Left))
        } else if self.next_token_expect(Keyword::Right.into()).is_ok() {
            // 这个outer可有可无 有就消耗掉
            let _ = self.next_token_expect(Keyword::Outer.into());
            self.next_token_expect(Keyword::Join.into())?;
            Ok(Some(ast::JoinType::Right))
        } else {
            Ok(None)
        }
    }

    fn parse_where_claues(&mut self) -> Result<Option<BaseExpression>> {
        // 如果没有where就结束了 返回 Ok(None)
        if self.next_token_expect(Keyword::Where.into()).is_ok() {
            return Ok(None);
        }
        // 有的话就进行解析
        Ok(Some(self.parse_expression(0)?))
    }

    fn parse_grouby_clause(&mut self) -> Result<Vec<BaseExpression>> {
        let mut expressions = Vec::new();
        // group by 没有就返回
        if self.next_token_expect(Keyword::Group.into()).is_ok() {
            return Ok(expressions);
        }
        // 接下来必须要有into
        self.next_token_expect(Keyword::By.into())?;
        // 解析group_by 字段 以逗号分割， 没有就结束了
        loop {
            //记得push进去 cloumn解析
            expressions.push(self.parse_expression(0)?);
            // 碰到逗号退出
            if self.next_token_expect(Token::Comma).is_ok() {
                break;
            }
        }

        todo!()
    }

    fn parse_having_claues(&mut self) -> Result<Option<BaseExpression>> {
        // 老规矩 判断有没有having
        if self.next_token_expect(Keyword::Having.into()).is_err() {
            return Ok(None);
        }
        // 有having就解析表达式
        Ok(Some(self.parse_expression(0)?))
    }

    fn parse_order_claues(&mut self) -> Result<Vec<(BaseExpression, OrderType)>> {
        // order by xxx DES , xxx ASC, age/10
        let mut orders = Vec::new();
        // 判断
        if self.next_token_expect(Keyword::Order.into()).is_err() {
            return Ok(orders);
        }
        // 有order了 那接下来必须是by
        self.next_token_expect(Keyword::By.into())?;
        // 循环获取
        loop {
            // 获得表达式
            let expression = self.parse_expression(0)?;
            // 获得ordertype
            let order_type = if self.next_token_expect(Keyword::Desc.into()).is_ok() {
                OrderType::DES
            } else if self.next_token_expect(Keyword::Asc.into()).is_ok() {
                OrderType::ASC
            } else {
                OrderType::DES
            };
            orders.push((expression,order_type));
            // 直到没有逗号分割表示结束
            if self.next_token_expect(Token::Comma).is_err() {
                break;
            }
        }
        Ok(orders)
    }

    fn parse_expression(&self, min: u8) -> Result<BaseExpression> {
        todo!()
    }

    /// 传入闭包判断，如果返回ok则调用next,并返回token err就返回err
    fn next_token_judge<F>(&mut self, judge: F) -> Result<Token>
    where
        F: Fn(&Token) -> Result<Token>,
    {
        match self.laxer.peek() {
            Some(t) => match t {
                Ok(token) => {
                    let r = judge(token)?;
                    self.laxer.next();
                    Ok(r)
                }
                Err(e) => Err(e.clone()),
            },
            None => Err(Error::Parse(format!("failed to get a token but get:None"))),
        }
    }

    fn next_token_expect_none(&mut self) -> Result<()> {
        if let Some(token) = self.laxer.peek() {
            match token {
                Ok(t) => Err(Error::Parse(format!("expect token:None get:{}", t))),
                Err(e) => Err(e.clone()),
            }
        } else {
            Ok(())
        }
    }
    /// 检查下一个token是否与我的匹配，如果不匹配返回不匹配的err,如果匹配无需返回
    fn next_token_expect(&mut self, judge_token: Token) -> Result<()> {
        if let Some(token) = self.laxer.peek() {
            match token {
                Ok(t) => match t {
                    token if token == &judge_token => {
                        self.laxer.next();
                        Ok(())
                    }
                    _ => Err(Error::Parse(format!(
                        "expect token:{} get:{}",
                        judge_token, t
                    ))),
                },
                Err(e) => return Err(e.clone()),
            }
        } else {
            Err(Error::Parse(format!(
                "expect token:{} get:None",
                judge_token
            )))
        }
    }
    fn next_string(&mut self) -> Result<String> {
        Ok(self
            .next_token_judge(|token| match token {
                Token::String(s) => Ok(Token::String(s.to_string())),
                other => Err(Error::Parse(format!("expect a string get {}", other))),
            })?
            .to_string())
    }
    // 下一个token是keyword 否则报错
    fn next_keyword(&mut self) -> Result<Keyword> {
        match self.laxer.peek() {
            Some(t) => match t {
                Ok(token) => match token {
                    Token::Keyword(keyword) => {
                        let k = keyword.clone();
                        self.laxer.next();
                        Ok(k)
                    }
                    other => Err(Error::Parse(format!("unexpected token {}", other))),
                },
                Err(e) => Err(e.clone()),
            },
            None => Err(Error::Parse("unexpected none".to_string())),
        }
    }
    /// 获得下一个token并且是ident返回string,否则报错
    fn next_ident(&mut self) -> Result<String> {
        Ok(self
            .next_token_judge(|token| match token {
                Token::Ident(s) => Ok(Token::Ident(s.to_string())),
                other => Err(Error::Parse(format!("expect a ident get {}", other))),
            })?
            .to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn create_test() {
        let mut parser = Parser::new("create table test ( name char primary key null, hah int );");
        let statement = parser.parse();
        assert!(statement.is_ok());
        println!("statement {:?}", statement);
    }
}
