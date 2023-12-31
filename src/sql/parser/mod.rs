use std::collections::BTreeMap;
use std::iter::Peekable;

use crate::sql::parser::laxer::{Keyword, Token};

use self::ast::{BaseExpression, FromItem, JoinType, SqlClumn};
use self::{ast::Statement, laxer::Laxer};
use crate::errors::Error;
use crate::errors::Result;

use super::{ColumnType, OrderType, Value};

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
                Ok(Token::Keyword(Keyword::Begin))
                | Ok(Token::Keyword(Keyword::Commit))
                | Ok(Token::Keyword(Keyword::Rollback)) => self.parse_transaction(),
                Ok(Token::Keyword(Keyword::Create)) => self.parse_create_statement(),
                Ok(Token::Keyword(Keyword::Drop)) => self.parse_drop_statement(),
                Ok(Token::Keyword(Keyword::Select)) => self.parse_select_statement(),
                Ok(Token::Keyword(Keyword::Update)) => self.parse_update_statement(),
                Ok(Token::Keyword(Keyword::Delete)) => self.parse_delete_statement(),
                Ok(Token::Keyword(Keyword::Insert)) => self.parse_insert_statement(),
                Ok(Token::Keyword(Keyword::Explain)) => self.parse_explain(),
                Ok(t) => Err(Error::Parse(format!("get unexpected token: {}", t))),
                Err(e) => Err(e.clone()),
            },
            None => Err(Error::Parse("not fount token".to_string())),
        }
    }

    /// Parses a transaction statement
    fn parse_transaction(&mut self) -> Result<ast::Statement> {
        match self.next()? {
            Token::Keyword(Keyword::Begin) => {
                let mut readonly = false;
                let mut version = None;
                self.next_token_expect(Keyword::Transaction.into())?;
                if self.next_token_expect(Keyword::Read.into()).is_ok() {
                    match self.next()? {
                        Token::Keyword(Keyword::Only) => readonly = true,
                        Token::Keyword(Keyword::Write) => readonly = false,
                        token => return Err(Error::Parse(format!("unexpected token {}", token))),
                    }
                }
                if self.next_token_expect(Keyword::As.into()).is_ok() {
                    match self.next()? {
                        Token::Number(n) => version = Some(n.parse::<u64>()?),
                        token => return Err(Error::Parse(format!("unexpected token {}", token))),
                    }
                }
                Ok(ast::Statement::Begin { readonly, version })
            }
            Token::Keyword(Keyword::Commit) => Ok(ast::Statement::Commit),
            Token::Keyword(Keyword::Rollback) => Ok(ast::Statement::Rollback),
            token => Err(Error::Parse(format!("Unexpected token {}", token))),
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
        let mut columns: Vec<SqlClumn> = vec![];
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
    fn parse_column(&mut self) -> Result<SqlClumn> {
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
        let mut column = SqlClumn {
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
            self.next_token_expect(Token::Equal)?;
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
                if self.next_token_expect(Token::Comma).is_err() {
                    break;
                };
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

        let select = self.parse_select_clause()?;
        let from = self.parse_from_claues()?;
        let filter = self.parse_where_claues()?;
        let group_by = self.parse_grouby_clause()?;
        let having = self.parse_having_claues()?;
        let order = self.parse_order_claues()?;
        let (offset, limit) = self.parse_limit_offset()?;

        Ok(Statement::Select {
            select,
            from,
            filter,
            group_by,
            having,
            order,
            offset,
            limit,
        })
    }

    fn parse_limit_offset(&mut self) -> Result<(Option<BaseExpression>, Option<BaseExpression>)> {
        // 有可能是 limit 在前 或者 offset 在前 或者就是直接 limit 1,2
        let mut offset = None;
        let mut limit = None;
        // limit 在前
        if self.next_token_expect(Keyword::Limit.into()).is_ok() {
            let expr = self.parse_expression(0)?;
            if self.next_token_expect(Keyword::Offset.into()).is_ok() {
                // 下一个是offset 说明expr是limit
                limit = Some(expr);
                // 再下一个是offset
                offset = Some(self.parse_expression(0)?);
            } else if self.next_token_expect(Token::Comma).is_ok() {
                // 下一个是逗号 说明expr是offset
                offset = Some(expr);
                // 再接下来是limit
                limit = Some(self.parse_expression(0)?);
            } else {
                // 什么也没有了
                limit = Some(expr);
            }
        } else if self.next_token_expect(Keyword::Offset.into()).is_ok() {
            // offset 在前
            offset = Some(self.parse_expression(0)?);
            // 接下来如果还有limit的话
            if self.next_token_expect(Keyword::Limit.into()).is_ok() {
                limit = Some(self.parse_expression(0)?);
            }
        }
        Ok((offset, limit))
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
            if self.next_token_expect(Keyword::As.into()).is_ok() {
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

    fn parse_from_claues(&mut self) -> Result<Option<FromItem>> {
        // 没有from关键字就返回就好了
        if self
            .next_token_expect(Token::Keyword(Keyword::From))
            .is_err()
        {
            return Ok(None);
        }
        // 首先拿到第一个fromItem
        let mut base_table = self.parse_join_from(None)?;
        loop {
            if self.next_token_expect(Token::Comma).is_err() {
                break;
            }
            let table = self.parse_join_from(None)?;

            // 逗号连接的多个表连接其实就是一个内连接
            base_table = ast::FromItem::Join {
                left: Box::new(base_table),
                right: Box::new(table),
                join_type: JoinType::Inner,
                predicate: None,
            };
        }

        Ok(Some(base_table))
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
        if self.next_token_expect(Keyword::Where.into()).is_err() {
            return Ok(None);
        }
        // 有的话就进行解析
        Ok(Some(self.parse_expression(0)?))
    }

    fn parse_grouby_clause(&mut self) -> Result<Vec<BaseExpression>> {
        let mut expressions = Vec::new();
        // group by 没有就返回
        if self.next_token_expect(Keyword::Group.into()).is_err() {
            return Ok(expressions);
        }
        // 接下来必须要有by
        self.next_token_expect(Keyword::By.into())?;
        // 解析group_by 字段 以逗号分割， 没有就结束了
        loop {
            //记得push进去 cloumn解析
            expressions.push(self.parse_expression(0)?);
            // 没有碰到逗号退出
            if self.next_token_expect(Token::Comma).is_err() {
                break;
            }
        }
        Ok(expressions)
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
            orders.push((expression, order_type));
            // 直到没有逗号分割表示结束
            if self.next_token_expect(Token::Comma).is_err() {
                break;
            }
        }
        Ok(orders)
    }

    /// 获得表达式， min表示当前expr中最小的优先级，如果小于min则return
    fn parse_expression(&mut self, min: u8) -> Result<BaseExpression> {
        // 查看有没有前缀运算符
        let mut expr = if let Some(operation) = PrefixOperation::get_operation(self, min)? {
            // 看到前缀之后递归比如 -(1+3)
            operation.build_expresion(
                self.parse_expression(operation.get_assoc() + operation.get_prec())?,
            )
        } else {
            self.get_atom_expression()?
        };

        if let Some(operation) = PostfixOperator::get_operation(self, min)? {
            expr = operation.build_expresion(expr);
        };

        while let Some(operation) = InfixOperator::get_operation(self, min)? {
            expr = operation.build_expresion(
                expr,
                self.parse_expression(operation.get_prec() + operation.get_assoc())?,
            );
        }

        Ok(expr)
    }

    /// function filed 常量(数字，字符串) 包括被括号包裹起来的可以将整体看作atom
    fn get_atom_expression(&mut self) -> Result<BaseExpression> {
        match self.next()? {
            // 先解析常量
            Token::Number(num) => {
                // 判断一下是整型还是浮点性
                if let Ok(i) = num.parse::<i64>() {
                    Ok(BaseExpression::Value(Value::Integer(i)))
                } else if let Ok(f) = num.parse::<f64>() {
                    Ok(BaseExpression::Value(Value::Float(f)))
                } else {
                    Err(Error::Parse(format!("expect a number get {}!", num)))
                }
            }
            Token::String(string) => Ok(BaseExpression::Value(Value::String(string))),
            Token::Keyword(Keyword::Null) => Ok(BaseExpression::Value(Value::Null)),
            Token::Keyword(Keyword::True) => Ok(BaseExpression::Value(Value::Bool(true))),
            Token::Keyword(Keyword::False) => Ok(BaseExpression::Value(Value::Bool(false))),
            // 补上
            Token::Keyword(Keyword::Infinity) => {
                Ok(BaseExpression::Value(Value::Float(f64::INFINITY)))
            }

            Token::Keyword(Keyword::NaN) => Ok(BaseExpression::Value(Value::Float(f64::NAN))),

            // 碰到括号包围的
            Token::OpenParen => {
                let expr = self.parse_expression(0)?;
                self.next_token_expect(Token::CloseParen)?;
                Ok(expr)
            }
            // function 列名
            Token::Ident(ident) => {
                // 看一下下一个是不是括号，如果是括号就是函数
                if self.next_token_expect(Token::OpenParen).is_ok() {
                    // 计划中函数只需要单属性就好了
                    // 可能是count *
                    let arg = if ident.to_uppercase() == "COUNT"
                        && self.next_token_expect(Token::Asterisk).is_ok()
                    {
                        BaseExpression::Value(Value::Bool(true))
                    } else {
                        self.parse_expression(0)?
                    };
                    self.next_token_expect(Token::CloseParen)?;
                    Ok(BaseExpression::Function(ident, Box::new(arg)))
                } else {
                    // 不是函数就是字段
                    let mut table = None;
                    let mut filed = ident;
                    // 有 点 说明是 table.filed
                    if self.next_token_expect(Token::Period).is_ok() {
                        table = Some(filed);
                        filed = self.next_ident()?;
                    }
                    Ok(BaseExpression::Field(table, filed))
                }
            }
            t => Err(Error::Parse(format!("expect an atom get:{}", t))),
        }
    }

    fn next(&mut self) -> Result<Token> {
        self.laxer
            .next()
            .unwrap_or_else(|| Err(Error::Parse("unexpected end".into())))
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
    fn peek(&mut self) -> Result<Token> {
        match self.laxer.peek() {
            Some(t) => match t {
                Ok(token) => Ok(token.clone()),
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

const LEFT_ASSOC: u8 = 1;
const RIGHT_ASSOC: u8 = 0;

trait Operation: Sized {
    // 通过paser获得operation
    fn get_operation(parser: &mut Parser, min: u8) -> Result<Option<Self>>;
    // 获得优先级
    fn get_prec(&self) -> u8;
    // 获得左右结合性
    fn get_assoc(&self) -> u8;
}

pub enum PrefixOperation {
    // 负号
    Negative,
    // 正号
    Plus,
    // 非
    Not,
}

impl PrefixOperation {
    fn build_expresion(&self, expr: BaseExpression) -> BaseExpression {
        match self {
            PrefixOperation::Negative => {
                BaseExpression::Operation(ast::Operation::Negative(Box::new(expr)))
            }
            PrefixOperation::Plus => {
                BaseExpression::Operation(ast::Operation::Plus(Box::new(expr)))
            }
            PrefixOperation::Not => BaseExpression::Operation(ast::Operation::Not(Box::new(expr))),
        }
    }
}

impl Operation for PrefixOperation {
    fn get_operation(parser: &mut Parser, min: u8) -> Result<Option<Self>> {
        if min > 9 {
            return Ok(None);
        }
        if parser.next_token_expect(Token::Plus).is_ok() {
            Ok(Some(PrefixOperation::Plus))
        } else if parser.next_token_expect(Token::Minus).is_ok() {
            Ok(Some(PrefixOperation::Negative))
        } else if parser.next_token_expect(Keyword::Not.into()).is_ok() {
            Ok(Some(PrefixOperation::Not))
        } else if parser.next_token_expect(Token::Exclamation).is_ok() {
            Ok(Some(PrefixOperation::Not))
        } else {
            Ok(None)
        }
    }

    fn get_prec(&self) -> u8 {
        9
    }

    fn get_assoc(&self) -> u8 {
        LEFT_ASSOC
    }
}

enum InfixOperator {
    // 逻辑
    And,
    Or,

    // 比较
    Equal,
    GreaterThan,
    GreaterThanOrEqual,
    LessThan,
    LessThanOrEqual,
    NotEqual,

    // 加减乘除
    Add,
    Subtract,
    Multiply,
    Divide,
    // 次方
    Exponentiate,

    Like,
}

impl InfixOperator {
    /// 将操作和其他expression结合
    fn build_expresion(&self, expr1: BaseExpression, expr2: BaseExpression) -> BaseExpression {
        match self {
            InfixOperator::And => {
                BaseExpression::Operation(ast::Operation::And(Box::new(expr1), Box::new(expr2)))
            }
            InfixOperator::Or => {
                BaseExpression::Operation(ast::Operation::Or(Box::new(expr1), Box::new(expr2)))
            }
            InfixOperator::Equal => {
                BaseExpression::Operation(ast::Operation::Equal(Box::new(expr1), Box::new(expr2)))
            }
            InfixOperator::GreaterThan => BaseExpression::Operation(ast::Operation::GreaterThan(
                Box::new(expr1),
                Box::new(expr2),
            )),
            InfixOperator::GreaterThanOrEqual => BaseExpression::Operation(
                ast::Operation::GreaterThanOrEqual(Box::new(expr1), Box::new(expr2)),
            ),
            InfixOperator::LessThan => BaseExpression::Operation(ast::Operation::LessThan(
                Box::new(expr1),
                Box::new(expr2),
            )),
            InfixOperator::LessThanOrEqual => BaseExpression::Operation(
                ast::Operation::LessThanOrEqual(Box::new(expr1), Box::new(expr2)),
            ),
            InfixOperator::NotEqual => BaseExpression::Operation(ast::Operation::NotEqual(
                Box::new(expr1),
                Box::new(expr2),
            )),
            InfixOperator::Add => {
                BaseExpression::Operation(ast::Operation::Add(Box::new(expr1), Box::new(expr2)))
            }
            InfixOperator::Subtract => BaseExpression::Operation(ast::Operation::Subtract(
                Box::new(expr1),
                Box::new(expr2),
            )),
            InfixOperator::Multiply => BaseExpression::Operation(ast::Operation::Multiply(
                Box::new(expr1),
                Box::new(expr2),
            )),
            InfixOperator::Divide => {
                BaseExpression::Operation(ast::Operation::Divide(Box::new(expr1), Box::new(expr2)))
            }
            InfixOperator::Exponentiate => BaseExpression::Operation(ast::Operation::Exponentiate(
                Box::new(expr1),
                Box::new(expr2),
            )),
            InfixOperator::Like => {
                BaseExpression::Operation(ast::Operation::Like(Box::new(expr1), Box::new(expr2)))
            }
        }
    }
}

impl Operation for InfixOperator {
    fn get_operation(parser: &mut Parser, min: u8) -> Result<Option<Self>> {
        let r = match parser.peek()? {
            Token::Keyword(Keyword::And) => Some(Self::And),
            Token::Keyword(Keyword::Or) => Some(Self::Or),

            Token::Keyword(Keyword::Like) => Some(Self::Like),

            Token::GreaterThan => Some(Self::GreaterThan),
            Token::GreaterThanOrEqual => Some(Self::GreaterThanOrEqual),
            Token::LessThan => Some(Self::LessThan),
            Token::LessOrGreaterThan => Some(Self::NotEqual),
            Token::LessThanOrEqual => Some(Self::LessThanOrEqual),
            Token::NotEqual => Some(Self::NotEqual),

            Token::Plus => Some(Self::Add),
            Token::Minus => Some(Self::Subtract),
            Token::Asterisk => Some(Self::Multiply),
            Token::Slash => Some(Self::Divide),
            Token::Caret => Some(Self::Exponentiate),
            Token::Equal => Some(Self::Equal),
            _ => None,
        };
        match r {
            Some(op) => {
                if op.get_prec() >= min {
                    parser.next()?;
                    Ok(Some(op))
                } else {
                    Ok(None)
                }
            }
            None => Ok(None),
        }
    }

    fn get_prec(&self) -> u8 {
        match self {
            InfixOperator::And => 2,
            InfixOperator::Or => 1,
            InfixOperator::Equal | InfixOperator::NotEqual | InfixOperator::Like => 3,
            InfixOperator::GreaterThan
            | InfixOperator::GreaterThanOrEqual
            | InfixOperator::LessThan
            | InfixOperator::LessThanOrEqual => 4,
            InfixOperator::Add | InfixOperator::Subtract => 5,
            InfixOperator::Multiply | InfixOperator::Divide => 6,
            InfixOperator::Exponentiate => 7,
        }
    }

    fn get_assoc(&self) -> u8 {
        match self {
            InfixOperator::Exponentiate => RIGHT_ASSOC,
            _ => LEFT_ASSOC,
        }
    }
}

// 后缀操作
enum PostfixOperator {
    IsNull,
    IsNotNull,
}

impl PostfixOperator {
    fn build_expresion(&self, expr: BaseExpression) -> BaseExpression {
        match self {
            PostfixOperator::IsNull => {
                BaseExpression::Operation(ast::Operation::IsNull(Box::new(expr)))
            }
            PostfixOperator::IsNotNull => BaseExpression::Operation(ast::Operation::Not(Box::new(
                BaseExpression::Operation(ast::Operation::IsNull(Box::new(expr))),
            ))),
        }
    }
}

impl Operation for PostfixOperator {
    fn get_operation(parser: &mut Parser, min: u8) -> Result<Option<Self>> {
        if min > 9 {
            return Ok(None);
        }
        if parser
            .next_token_expect(Token::Keyword(Keyword::Is))
            .is_ok()
        {
            let r = if parser.next_token_expect(Keyword::Not.into()).is_ok() {
                PostfixOperator::IsNotNull
            } else {
                PostfixOperator::IsNull
            };
            parser.next_token_expect(Keyword::Null.into())?;
            Ok(Some(r))
        } else {
            Ok(None)
        }
    }

    fn get_prec(&self) -> u8 {
        9
    }

    fn get_assoc(&self) -> u8 {
        LEFT_ASSOC
    }
}

#[cfg(test)]
mod tests {
    use crate::sql::Value;

    use super::*;

    #[test]
    fn create_test() {
        let mut parser = Parser::new("create table test ( name char primary key null, hah int );");
        let statement = parser.parse();
        assert!(statement.is_ok());
        println!("statement {:?}", statement);
    }
    #[test]
    fn select_test() {
        let mut parser = Parser::new(
            "SELECT customers.customer_id, customers.customer_name, COUNT(orders.order_id) AS num_of_orders, SUM(orders.order_total) AS total_spent 
            FROM customers LEFT JOIN orders ON customers.customer_id = orders.customer_id 
            WHERE orders.order_status = \"completed\" AND customers.customer_country = \"USA\" 
            GROUP BY customers.customer_id 
            HAVING num_of_orders > 5 
            order BY total_spent DESC OFFSET 10 LIMIT 5;",
        );
        let statement = parser.parse();

        //println!("statement {:#?}", statement);
        assert!(statement.is_ok());
    }
    #[test]
    fn select1_test() {
        let input = "SELECT -1 * (4.5 + 2) / 3.7 AS result ,-(4+3+2) + 5 
        FROM table1
        JOIN table2 ON table1.id = table2.id
        WHERE table1.value > 10
        GROUP BY table1.id
        HAVING COUNT(*) > 3
        ORDER BY table2.value DESC
         OFFSET 5 LIMIT 10;";
        let mut parser = Parser::new(input);
        let statement = parser.parse().unwrap();
        match &statement {
            Statement::Select {
                select,
                from,
                filter,
                group_by,
                having,
                order,
                offset,
                limit,
            } => {
                let expr = &select[0].0;
                if expr.contains(&|e| match e {
                    BaseExpression::Operation(ast::Operation::Negative(_)) => true,
                    _ => false,
                }) {
                    println!("contain negative");
                };
                if expr.contains(&|e| match e {
                    BaseExpression::Operation(ast::Operation::Multiply(_, _)) => true,
                    _ => false,
                }) {
                    println!("contain multiple");
                };
            }
            _ => {}
        };
        //println!("statement {:#?}", statement);
    }
    #[test]
    fn select_offset_limit_test() {
        let input = "SELECT *
        FROM table1
         OFFSET 5 LIMIT 10;";
        let mut parser = Parser::new(input);
        let statement = parser.parse();
        println!("statement {:#?}", statement);
        match statement {
            Ok(n) => match n {
                Statement::Select {
                    select: _,
                    from: _,
                    filter: _,
                    group_by: _,
                    having: _,
                    order: _,
                    offset,
                    limit,
                } => {
                    assert_eq!(offset, Some(BaseExpression::Value(Value::Integer(5))));
                    assert_eq!(limit, Some(BaseExpression::Value(Value::Integer(10))));
                }
                _ => {
                    panic!("expect select");
                }
            },
            Err(_) => {}
        };
        let input = "SELECT *
        FROM table1
          LIMIT 10 OFFSET 5;";
        let mut parser = Parser::new(input);
        let statement = parser.parse();
        println!("statement {:#?}", statement);
        match statement {
            Ok(n) => match n {
                Statement::Select {
                    select: _,
                    from: _,
                    filter: _,
                    group_by: _,
                    having: _,
                    order: _,
                    offset,
                    limit,
                } => {
                    assert_eq!(offset, Some(BaseExpression::Value(Value::Integer(5))));
                    assert_eq!(limit, Some(BaseExpression::Value(Value::Integer(10))));
                }
                _ => {
                    panic!("expect select");
                }
            },
            Err(_) => {}
        };
        let input = "SELECT *
        FROM table1
          LIMIT 5,10;";
        let mut parser = Parser::new(input);
        let statement = parser.parse();
        println!("statement {:#?}", statement);
        match statement {
            Ok(n) => match n {
                Statement::Select {
                    select: _,
                    from: _,
                    filter: _,
                    group_by: _,
                    having: _,
                    order: _,
                    offset,
                    limit,
                } => {
                    assert_eq!(offset, Some(BaseExpression::Value(Value::Integer(5))));
                    assert_eq!(limit, Some(BaseExpression::Value(Value::Integer(10))));
                }
                _ => {
                    panic!("expect select");
                }
            },
            Err(_) => {}
        };
    }
}
