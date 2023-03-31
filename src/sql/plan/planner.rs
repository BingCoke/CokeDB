use std::collections::{HashMap, HashSet};

use crate::sql::{
    expression::{self, Expression},
    parser::ast::{BaseExpression, FromItem, JoinType, Operation, Statement},
    plan::Aggregate,
    schema::Catalog,
    Column, Table, OrderType,
};

use super::{Node, Plan};
use crate::errors::{Error, Result};

pub struct Planner {
    catalog: Box<dyn Catalog>,
}

impl Planner {
    pub fn new(catalog: Box<dyn Catalog>) -> Self {
        Self { catalog }
    }

    pub fn build_plan(&mut self, statement: Statement) -> Result<Plan> {
        let node = self.build_node(statement)?;
        Ok(Plan::new(node))
    }

    fn build_node(&mut self, statement: Statement) -> Result<Node> {
        match statement {
            Statement::Begin { .. }
            | Statement::Commit
            | Statement::Rollback
            | Statement::Explain(_) => {
                return Err(Error::Plan(format!(
                    "get unexpected statement: {:?}",
                    statement
                )));
            }

            Statement::CreateTable { name, columns } => {
                // 在 column 中有default字段类型是BaseExpression，其实default字段应该是一个Constant常量
                let mut set = HashSet::new();
                // 首先分离出<column,Expression>
                // expression后续会进行常量计算，所以先提取出来，Cloumn中先置为None
                let columns = columns
                    .into_iter()
                    .map(|c| {
                        let default = c
                            .default
                            .clone()
                            .map(|de| Result::Ok(self.build_expresion(&Scope::constant(), de)?))
                            .transpose()?;
                        if !set.insert(c.name.clone()) {
                            return Err(Error::Plan(format!(
                                "try to create table that has repeat column name: {}",
                                c.name
                            )));
                        }
                        let column = Column {
                            name: c.name,
                            column_type: c.column_type,
                            primary_key: c.primary_key,
                            nullable: c.nullable.unwrap_or_else(|| false),
                            default: None,
                            unique: c.unique,
                            index: c.index,
                        };
                        Result::Ok((column, default))
                    })
                    .collect::<Result<Vec<(Column, Option<Expression>)>>>()?;
                // 将columns和defaults进行分离
                let (columns, defaults): (Vec<Column>, Vec<Option<Expression>>) =
                    columns.into_iter().unzip();
                let table = Table { name, columns };
                Ok(Node::CreateTable { table, defaults })
            }

            Statement::DropTable(table_name) => Ok(Node::DropTable { table: table_name }),

            Statement::Insert {
                table,
                columns,
                values,
            } => {
                let table_name = table.clone();
                // 得到table
                let table = self.catalog.must_read_table(table.as_str())?;
                // 作一下转换 如果是空说明是全部字段，不为空就是指定字段
                let columns = match columns {
                    Some(cs) => cs,
                    None => table
                        .columns
                        .iter()
                        .map(|c| c.name.clone())
                        .collect::<Vec<String>>(),
                };
                let mut scope = Scope::new();
                scope.register_table(table)?;
                if values.len() != columns.len() {
                    return Err(Error::Plan(format!(
                        "unexpected values.len not equal columns.len"
                    )));
                }

                // 检查一下这些column是否存在
                for ele in columns.iter() {
                    scope.get_column_index(Some(table_name.to_string()), ele.clone())?;
                }
                // 包括 insert的数据必须都是常量，这里需要进行判断 同时转换一下
                let values = values
                    .into_iter()
                    .map(|vs| {
                        Result::Ok(
                            vs.into_iter()
                                .map(|expr| {
                                    Result::Ok(self.build_expresion(&Scope::constant(), expr)?)
                                })
                                .collect::<Result<Vec<Expression>>>()?,
                        )
                    })
                    .collect::<Result<Vec<Vec<Expression>>>>()?;
                // 后续会对常量统一进行计算，这里就不进行了
                Ok(Node::Insert {
                    table: table_name,
                    columns,
                    expressions: values,
                })
            }
            Statement::Delete { table, filter } => {
                let mut scope = Scope::new();
                scope.register_table(self.catalog.must_read_table(table.as_str())?)?;
                let filter = match filter {
                    Some(expr) => Some(self.build_expresion(&scope, expr)?),
                    None => None,
                };
                Ok(Node::Delete {
                    table: table.clone(),
                    source: Box::new(Node::Scan {
                        table,
                        alias: None,
                        filter,
                    }),
                })
            }
            Statement::Update { table, set, filter } => {
                let mut scope = Scope::new();
                scope.register_table(self.catalog.must_read_table(table.as_str())?)?;
                let filter = match filter {
                    Some(expr) => Some(self.build_expresion(&scope, expr)?),
                    None => None,
                };

                let set = set
                    .into_iter()
                    .map(|(k, v)| {
                        let index = scope.get_column_index(Some(table.clone()), k)?.to_owned();
                        Result::Ok((index, self.build_expresion(&Scope::constant(), v)?))
                    })
                    .collect::<Result<Vec<_>>>()?;
                Ok(Node::Update {
                    table: table.clone(),
                    source: Box::new(Node::Scan {
                        table,
                        alias: None,
                        filter,
                    }),
                    set,
                })
            }
            Statement::Select {
                mut select,
                from,
                filter,
                group_by,
                mut having,
                mut order,
                offset,
                limit,
            } => {
                // 从from中获取from
                let (mut node, mut scope) = if let Some(from) = from {
                    let mut scope = Scope::new();
                    (self.build_from_table(&mut scope, from)?, scope)
                } else if !select.is_empty() {
                    // 如果from是none ，但是select不是就返回noting
                    (Node::Nothing, Scope::new())
                } else {
                    // 啥也没有就报错
                    return Err(Error::Plan(format!("get select and from empty")));
                };

                // 构建where filter
                if let Some(filter) = filter {
                    node = Node::Filter {
                        source: Box::new(node),
                        predicate: self.build_expresion(&scope, filter)?,
                    };
                }

                // select * where .... group_by ...
                // 这种情况不允许出现
                if select.is_empty() && !group_by.is_empty() {
                    return Err(Error::Plan(
                        "is not support for using 'select *' and 'group_by' at same time".into(),
                    ));
                }

                // 设置需要隐藏的数目
                let mut hidden = 0;

                // 开始解析select
                if !select.is_empty() {
                    // having orderby 需要
                    if let Some(ref mut expr) = having {
                        hidden += self.transform_and_inject_hidden(expr, &mut select)?;
                    }
                    for (expr, _) in order.iter_mut() {
                        hidden += self.transform_and_inject_hidden(expr, &mut select)?;
                    }

                    // 将函数和group by提取出来 这两个需要单独生成node节点
                    let aggregates = self.extract_aggreates(&mut select)?;
                    let gourps = self.extract_group_by(aggregates.len(), &mut select, group_by)?;

                    // 如果有group_by aggregates 则需要构建聚合函数的node
                    if aggregates.len() > 0 || gourps.len() > 0 {
                        node = self.build_aggregates(&mut scope, aggregates, gourps, node)?;
                    }

                    // 最后终于可以构建select了 就是建立一个投影
                    let expressions: Vec<(Expression, Option<String>)> = select
                        .into_iter()
                        .map(|(e, l)| Ok((self.build_expresion(&scope, e)?, l)))
                        .collect::<Result<_>>()?;
                    scope.project(&expressions)?;
                    node = Node::Projection {
                        source: Box::new(node),
                        expressions,
                    };
                }
                if let Some(having) = having {
                    node = Node::Filter {
                        source: Box::new(node),
                        predicate: self.build_expresion(&scope, having)?,
                    }
                }

                if order.len() > 0 {
                    node = Node::Order {
                        source: Box::new(node),
                        orders: order
                            .into_iter()
                            .map(|(expr, order_type)| {
                               Result::Ok((self.build_expresion(&scope, expr)?, order_type))
                            })
                            .collect::<Result<Vec<(Expression, OrderType)>>>()?,
                    }
                }

                if let Some(offset) = offset {
                    node = Node::Offset {
                        source: Box::new(node),
                        offset: self.build_expresion(&Scope::constant(), offset)?,
                    }
                }

                if let Some(limit) = limit {
                    node = Node::Offset {
                        source: Box::new(node),
                        offset: self.build_expresion(&Scope::constant(), limit)?,
                    }
                }

                // 这里进行投影把后面hidden删除
                if hidden > 0 {
                    node = Node::Projection {
                        source: Box::new(node),
                        expressions: (0..scope.get_column_size() - hidden)
                            .into_iter()
                            .map(|index| (Expression::Field(index, None), None))
                            .collect(),
                    }
                }

                Ok(node)
            }
        }
    }

    /// 构建 聚合操作执行节点
    fn build_aggregates(
        &self,
        scope: &mut Scope,
        aggregate: Vec<(Aggregate, BaseExpression)>,
        group_by: Vec<(BaseExpression, Option<String>)>,
        source: Node,
    ) -> Result<Node> {
        // 按照顺序记录聚合操作
        let mut aggregates = Vec::new();
        // 记录列 作为投影 前面是需要被聚合的列 后面是groupby的列
        let mut expressions = Vec::new();

        for (agg, expr) in aggregate {
            aggregates.push(agg);
            expressions.push((self.build_expresion(scope, expr)?, None));
        }

        for (expr, label) in group_by {
            expressions.push((self.build_expresion(scope, expr)?, label));
        }

        // 建立映射 保证上层节点正常拿取数据
        scope.project(
            &expressions
                .iter()
                .cloned()
                .enumerate()
                .map(|(index, (expr, label))| {
                    if index < aggregates.len() {
                        // 聚合操作不需要上层节点知道 聚合操作都被转换了 没有意义
                        // 执行的时候通过column(i) 拿取数据就好了
                        (Expression::Constant(crate::sql::Value::Null), None)
                    } else {
                        // 上层节点只能拿到group by的字段
                        // select name .... group by name 只能拿到这个
                        // select age ..... group by name 是会报错的
                        (expr, label)
                    }
                })
                .collect::<Vec<_>>(),
        )?;

        Ok(Node::Aggregation {
            source: Box::new(source),
            aggregates,
        })
    }

    /// 将聚合函数提取出来
    fn extract_aggreates(
        &self,
        select: &mut Vec<(BaseExpression, Option<String>)>,
    ) -> Result<Vec<(Aggregate, BaseExpression)>> {
        let mut res = Vec::new();
        for (expr, _) in select.iter_mut() {
            expr.transform_ref(
                &mut |e| {
                    Ok(match e {
                        BaseExpression::Function(f, exprx) => {
                            let aggregate = Aggregate::from_str(f.as_str())?;
                            res.push((aggregate, *exprx));
                            BaseExpression::Column(res.len() - 1)
                        }
                        _ => e,
                    })
                },
                &mut |e| Ok(e),
            )?;
        }
        for (_, expr) in res.iter() {
            if expr.contains_aggreate() {
                return Err(Error::Plan(
                    "not support for aggregate function reference aggregate".to_string(),
                ));
            }
        }
        Ok(res)
    }

    fn extract_group_by(
        &self,
        offset: usize,
        select: &mut Vec<(BaseExpression, Option<String>)>,
        group_by: Vec<BaseExpression>,
    ) -> Result<Vec<(BaseExpression, Option<String>)>> {
        let mut groups = Vec::new();
        for group in group_by.into_iter() {
            // 如果是label说明需要在select中找
            if let BaseExpression::Field(None, label) = &group {
                if let Some(index) = select
                    .iter()
                    .position(|(_, l)| l == &Some(label.to_string()))
                {
                    // 在select中的expr找到之后替代select
                    let (expr, label) = select.get_mut(index).unwrap();
                    let swap = BaseExpression::Column(offset + groups.len());
                    let expr = std::mem::replace(expr, swap);
                    groups.push((expr, label.clone()));
                    continue;
                } else {
                    return Err(Error::Plan(format!(
                        "can't find label of group_by :{}",
                        label
                    )));
                }
            }
            // 没有label 查看是否有 表达式一致的
            if let Some(index) = select.iter().position(|(e, _)| e == &group) {
                // 在select中的expr找到之后替代select
                let (expr, label) = select.get_mut(index).unwrap();
                let swap = BaseExpression::Column(offset + groups.len());
                let expr = std::mem::replace(expr, swap);
                groups.push((expr, label.clone()));
                continue;
            }
            // 和select中毫无关系的话 就直接插入进去
            groups.push((group, None));
        }

        //  查看group中是否有聚合操作
        for (expr, _) in &groups {
            if expr.contains_aggreate() {
                return Err(Error::Plan("group cannot contain aggregates".into()));
            }
        }

        Ok(groups)
    }

    fn transform_and_inject_hidden(
        &mut self,
        expr: &mut BaseExpression,
        select: &mut Vec<(BaseExpression, Option<String>)>,
    ) -> Result<usize> {
        let mut hidden = 0;
        for (i, (select_expr, lable)) in select.iter().enumerate() {
            // 表达式一样的话就把当前 expr 改一下
            if select_expr == expr {
                *expr = BaseExpression::Column(i);
            }

            // 表达式不一样 先查看有没有相同的lable 递归巡查，
            // select name n ... order n
            // 但是下面这个就不行了 所以前面是None
            // select name n ... order stu.n
            if let Some(ref lable) = lable {
                expr.transform_ref(
                    &mut |e| match e {
                        BaseExpression::Field(None, ref l) if lable == l => {
                            Ok(BaseExpression::Column(i))
                        }
                        _ => Ok(e),
                    },
                    &mut |e| Ok(e),
                )?;
            }
        }
        // 这个地方要注意 如果是select max(id) v , studio_id a, sum(studio_id) from movies group by studio_id having sum(a)>0;
        // 此时 having 的expr 会变成 sum(column(2)) 这是不被允许的
        // 因为having执行会比select更前 所以column(2) 是会出问题的 所以function中的expr改回去
        // 因为这里的column(2)是找的select的结果， 但是having执行的早，压根找不到
        // 这里有点不好理解，需要了解后面的聚合以及groupby原理
        expr.transform_ref(&mut |e| Ok(e), &mut |e| match e {
            BaseExpression::Function(f, mut ex) => {
                ex.transform_ref(&mut |e| Ok(e), &mut |e| match e {
                    BaseExpression::Column(i) => {
                        let (r, _) = select.get(i).cloned().ok_or(Error::Plan(format!("")))?;
                        Ok(r)
                    }
                    _ => Ok(e),
                })?;
                Ok(BaseExpression::Function(f, ex))
            }
            _ => Ok(e),
        })?;

        // 如果上面转换了一边之后 还有没有转换的，那就需要加到select中了
        // 同时hidden+1 之后再根据hidden删除就行了
        // orderby和having是 需要select执行之后才进行
        // 因为他们两个是可以读取select作用域中的label的 这也是为什么要做上面的操作
        expr.transform_ref(
            &mut |e| {
                Ok(match &e {
                    BaseExpression::Field(_, _) => {
                        // 这个时候还能找到field说明select压根没有 直接放到select
                        select.push((e, None));
                        hidden += 1;
                        BaseExpression::Column(select.len() - 1)
                    }
                    BaseExpression::Function(f, a) => {
                        // 判断一下有没有这个function 不需要管arg, 因为已经放到select了
                        Aggregate::from_str(&f)?;
                        select.push((e, None));
                        hidden += 1;
                        BaseExpression::Column(select.len() - 1)
                    }
                    _ => e,
                })
            },
            &mut |e| Ok(e),
        )?;

        Ok(hidden)
    }

    fn build_from_table(&self, scope: &mut Scope, from: FromItem) -> Result<Node> {
        match from {
            FromItem::Table { name, alias } => {
                // 如果是table 则是最底层的操作
                let table = self.catalog.must_read_table(&name)?;
                scope.register_table(table)?;
                Ok(Node::Scan {
                    table: name,
                    alias,
                    filter: None,
                })
            }
            FromItem::Join {
                left,
                right,
                join_type,
                predicate,
            } => {
                // 查看一下join type
                // 如果是右连接需要调换一下，（左右连接在执行的时候一律按照左连接执行，只是执行结果右连接会进行一次投影）
                let (left, right) = match join_type {
                    JoinType::Right => (right, left),
                    _ => (left, right),
                };
                let left = Box::new(self.build_from_table(scope, *left)?);
                // 这里得到左表的字段数目，方便如果右连接的话之后进行投影
                let left_size = scope.get_column_size();
                let right = Box::new(self.build_from_table(scope, *right)?);

                let predicate = match predicate {
                    Some(expr) => Some(self.build_expresion(scope, expr)?),
                    None => None,
                };

                let outer = match join_type {
                    JoinType::Left | JoinType::Right => true,
                    JoinType::Inner | JoinType::Cross => false,
                };

                // 构建连接
                let mut node = Node::NestedLoopJoin {
                    left,
                    right,
                    predicate,
                    outer,
                };
                let size = scope.get_column_size();

                // 如果是右连接需要这里需要添加投影
                Ok(match join_type {
                    JoinType::Right => {
                        let columns = scope.get_columns();
                        // 前leftsize个放到后面
                        // 注意 project之后列的顺序取决于 Expression::Filed 在数组的位置,
                        // filed.i 这个i是project的时候从source 数据中取第几列
                        let exprs: Vec<(_, Option<String>)> = (left_size..size)
                            .chain(0..left_size)
                            .map(|i| {
                                let c = columns.get(i).cloned().ok_or_else(|| {
                                    Error::Plan(format!("try to get none value column"))
                                })?;
                                match c {
                                    // 按照逻辑来讲肯定能拿到name，如果拿不到就报错就好了
                                    (table, Some(name)) => {
                                        // 我们不期望更改lable 所以第二个置为None
                                        Ok((Expression::Field(i, Some((table, name))), None))
                                    }
                                    _ => Err(Error::Plan("".to_string())),
                                }
                            })
                            .collect::<Result<Vec<_>>>()?;
                        // scope 也需要投影，防止上层节点调用的时候找到错误列
                        scope.project(&exprs)?;

                        node = Node::Projection {
                            source: Box::new(node),
                            expressions: exprs,
                        };
                        node
                    }
                    _ => node,
                })
            }
        }
    }

    pub fn build_expresion(&self, scope: &Scope, expression: BaseExpression) -> Result<Expression> {
        match expression {
            BaseExpression::Field(table, name) => Ok(Expression::Field(
                scope
                    .get_column_index(table.clone(), name.clone())?
                    .to_owned(),
                Some((table, name)),
            )),
            BaseExpression::Column(i) => Ok(Expression::Field(i, None)),
            BaseExpression::Value(value) => Ok(Expression::Constant(value)),
            BaseExpression::Function(_, _) => Err(Error::Plan(format!(
                "get unexpected base_expression: {:?}",
                expression
            ))),
            BaseExpression::Operation(operation) => match operation {
                Operation::Negative(a) => Ok(Expression::Negative(Box::new(
                    self.build_expresion(scope, *a)?,
                ))),
                Operation::Plus(a) => {
                    Ok(Expression::Plus(Box::new(self.build_expresion(scope, *a)?)))
                }
                Operation::And(a, b) => Ok(Expression::And(
                    Box::new(self.build_expresion(scope, *a)?),
                    Box::new(self.build_expresion(scope, *b)?),
                )),
                Operation::Or(a, b) => Ok(Expression::Or(
                    Box::new(self.build_expresion(scope, *a)?),
                    Box::new(self.build_expresion(scope, *b)?),
                )),
                Operation::Like(a, b) => Ok(Expression::Like(
                    Box::new(self.build_expresion(scope, *a)?),
                    Box::new(self.build_expresion(scope, *b)?),
                )),
                Operation::Equal(a, b) => Ok(Expression::Equal(
                    Box::new(self.build_expresion(scope, *a)?),
                    Box::new(self.build_expresion(scope, *b)?),
                )),
                Operation::NotEqual(a, b) => Ok(Expression::Not(Box::new(Expression::Equal(
                    Box::new(self.build_expresion(scope, *a)?),
                    Box::new(self.build_expresion(scope, *b)?),
                )))),
                Operation::GreaterThan(a, b) => Ok(Expression::GreaterThan(
                    Box::new(self.build_expresion(scope, *a)?),
                    Box::new(self.build_expresion(scope, *b)?),
                )),
                Operation::GreaterThanOrEqual(a, b) => Ok(Expression::Or(
                    Box::new(Expression::Equal(
                        Box::new(self.build_expresion(scope, *a.clone())?),
                        Box::new(self.build_expresion(scope, *b.clone())?),
                    )),
                    Box::new(Expression::GreaterThan(
                        Box::new(self.build_expresion(scope, *a)?),
                        Box::new(self.build_expresion(scope, *b)?),
                    )),
                )),
                Operation::LessThan(a, b) => Ok(Expression::LessThan(
                    Box::new(self.build_expresion(scope, *a)?),
                    Box::new(self.build_expresion(scope, *b)?),
                )),
                Operation::LessThanOrEqual(a, b) => Ok(Expression::Or(
                    Box::new(Expression::Equal(
                        Box::new(self.build_expresion(scope, *a.clone())?),
                        Box::new(self.build_expresion(scope, *b.clone())?),
                    )),
                    Box::new(Expression::LessThan(
                        Box::new(self.build_expresion(scope, *a)?),
                        Box::new(self.build_expresion(scope, *b)?),
                    )),
                )),

                Operation::Add(a, b) => Ok(Expression::Add(
                    Box::new(self.build_expresion(scope, *a)?),
                    Box::new(self.build_expresion(scope, *b)?),
                )),
                Operation::Subtract(a, b) => Ok(Expression::Subtract(
                    Box::new(self.build_expresion(scope, *a)?),
                    Box::new(self.build_expresion(scope, *b)?),
                )),
                Operation::Multiply(a, b) => Ok(Expression::Multiply(
                    Box::new(self.build_expresion(scope, *a)?),
                    Box::new(self.build_expresion(scope, *b)?),
                )),
                Operation::Divide(a, b) => Ok(Expression::Divide(
                    Box::new(self.build_expresion(scope, *a)?),
                    Box::new(self.build_expresion(scope, *b)?),
                )),
                Operation::Exponentiate(a, b) => Ok(Expression::Exponentiate(
                    Box::new(self.build_expresion(scope, *a)?),
                    Box::new(self.build_expresion(scope, *b)?),
                )),
                Operation::Not(a) => {
                    Ok(Expression::Not(Box::new(self.build_expresion(scope, *a)?)))
                }
                Operation::IsNull(a) => Ok(Expression::IsNull(Box::new(
                    self.build_expresion(scope, *a)?,
                ))),
            },
        }
    }
}

/// 作用域 用于上层执行节点知道应该哪里找数据
/// 具体看 docs/planner.md
#[derive(Clone, Debug)]
pub struct Scope {
    // true 范围就是常量，不包含字段
    constant: bool,
    // 放入已知的table
    tables: HashMap<String, Table>,
    // 放入已经知道的column
    columns: Vec<(Option<String>, Option<String>)>,
    // 给columns加一个索引 key = (table_name, column_name) val = 上面columns中column所在的index
    // 就是有表名的放这里
    qualified: HashMap<(String, String), usize>,
    //  非限定列名放入的位置
    //  不管有没有表名的放这里 比如 select stu.name from stu 和 select name from stu 其实都一样
    //  当遇到第二条这种sql语句的时候可以使用这个map
    unqualified: HashMap<String, usize>,
    // unqualified中key 存储多次就会放这里，同时删除unqualified
    // 假设两个表都有name字段
    // select name from stu, course .....
    // 这个时候就无法判断select 是哪个表中的name
    // 这个需要直到解析的时候才能知道错误,如果select没有出现可能出现歧义的字段则无需理会
    ambiguous: HashSet<String>,
}

impl Scope {
    /// 生成一个空的作用域
    fn new() -> Self {
        Self {
            constant: false,
            tables: HashMap::new(),
            columns: Vec::new(),
            qualified: HashMap::new(),
            unqualified: HashMap::new(),
            ambiguous: HashSet::new(),
        }
    }

    /// 生成一个只用于常量计算的作用域
    fn constant() -> Self {
        let mut scope = Self::new();
        scope.constant = true;
        scope
    }

    fn get_columns(&mut self) -> Vec<(Option<String>, Option<String>)> {
        self.columns.clone()
    }

    fn get_column_size(&self) -> usize {
        self.columns.len()
    }

    fn project(&mut self, expr: &Vec<(Expression, Option<String>)>) -> Result<()> {
        if self.constant {
            return Err(Error::Plan("try to modify with constant scope".to_string()));
        }
        let mut scope = Self::new();
        scope.tables = self.tables.clone();

        expr.iter()
            .map(|(filed, label)| {
                match (filed, label) {
                    // 有label 说明我是想 重命名expr所在的列
                    (_, Some(label)) => {
                        scope.add_column(None, Some(label.clone()));
                    }
                    // 没有label 我就去找上层节点的label 复用上层节点的
                    (Expression::Field(i, _), None) => {
                        let (table, label) = scope
                            .columns
                            .get(*i)
                            .cloned()
                            .ok_or_else(|| Error::Plan("".to_string()))?;
                        scope.add_column(table, label);
                    }
                    // 其他情况就是不需要上层节点通过label找到我 只能通过index
                    _ => scope.add_column(None, None),
                }
                Result::Ok(())
            })
            .collect::<Result<Vec<()>>>()?;
        *self = scope;
        Ok(())
    }

    fn add_column(&mut self, table: Option<String>, label: Option<String>) {
        if let Some(label) = label.clone() {
            if let Some(table) = table.clone() {
                self.qualified
                    .insert((table.clone(), label.clone()), self.columns.len());
            }
            if self.unqualified.contains_key(&label) {
                self.unqualified.remove(&label);
                self.ambiguous.insert(label);
            } else {
                self.unqualified.insert(label, self.columns.len());
            }
        }
        self.columns.push((table, label));
    }

    fn register_table(&mut self, table: Table) -> Result<()> {
        if self.constant {
            return Err(Error::Plan(
                "constant scope can't register table".to_string(),
            ));
        }
        let mut base = self.clone();
        if base.tables.contains_key(&table.name) {
            return Err(Error::Plan(format!(
                "try to register repeat table: {:?}",
                table
            )));
        }
        let table_name = table.name.clone();
        for ele in table.columns.iter() {
            let column_name = ele.name.clone();
            self.qualified.insert(
                (table_name.clone(), column_name.clone()),
                self.columns.len(),
            );
            if self.unqualified.contains_key(&column_name) {
                self.unqualified.remove(&column_name);
                self.ambiguous.insert(column_name);
            } else {
                self.unqualified.insert(column_name, self.columns.len());
            }
        }
        base.tables.insert(table_name.clone(), table);
        *self = base;
        Ok(())
    }

    fn get_column_index(&self, table: Option<String>, name: String) -> Result<&usize> {
        if self.constant {
            return Err(Error::Plan(
                "try to get column with constans scope".to_string(),
            ));
        }
        // 先查看有没有table
        match table {
            Some(table) => {
                if !self.tables.contains_key(table.as_str()) {
                    return Err(Error::Plan(format!(
                        "can't get table: {} in this scope",
                        table
                    )));
                }
                // 存在的话就直接在全限定map中找
                self.qualified
                    .get(&(table.clone(), name.clone()))
                    .ok_or(Error::Plan(format!(
                        "can't find table: {}, filed: {}",
                        table.clone(),
                        name.clone()
                    )))
            }
            // 如果没有设定table
            // 需要看看是否在ambiguous中，里面的字段表示有争议，如果存在就说明我们也不知道应该给哪个了
            None => {
                if self.ambiguous.contains(name.as_str()) {
                    Err(Error::Plan(format!("try to get ambiguous filed: {}", name)))
                } else {
                    self.unqualified
                        .get(&name)
                        .ok_or(Error::Plan(format!("can't find filed: {}", name)))
                }
            }
        }
    }
}
