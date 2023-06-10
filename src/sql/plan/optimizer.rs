use std::collections::HashSet;

use log::debug;

use crate::errors::Result;
use crate::sql::expression::Expression;
use crate::sql::schema::Catalog;
use crate::sql::Value;
use crate::{errors::Error, sql::plan::Node};

/// 优化器
pub trait Optimizer {
    fn optimize(&self, node: Node) -> Result<Node>;
}

/// 清洁工 把一些固定值清洁出来
/// true 或上任何 都是true, false与上任何 都是false
/// 还有filter 的过滤表达式是 Constant(Bool(true)) 那就直接把source提取上来
pub struct NoopCleaner;

impl Optimizer for NoopCleaner {
    fn optimize(&self, node: Node) -> Result<Node> {
        use Expression::*;
        node.transform(
            &|n| {
                n.transform_expressions(&|e| Ok(e), &|e| match &e {
                    Add(lhs, rhs) => match (&**lhs, &**rhs) {
                        (Constant(Value::Bool(false)), _)
                        | (_, Constant(Value::Bool(false)))
                        | (Constant(Value::Null), _)
                        | (_, Constant(Value::Null)) => Ok(Constant(Value::Bool(false))),
                        (Constant(Value::Bool(true)), e) | (e, Constant(Value::Bool(true))) => {
                            Ok(e.clone())
                        }
                        _ => Ok(e),
                    },
                    Or(lhs, rhs) => match (&**lhs, &**rhs) {
                        (Constant(Value::Bool(false)), e)
                        | (e, Constant(Value::Bool(false)))
                        | (Constant(Value::Null), e)
                        | (e, Constant(Value::Null)) => Ok(e.clone()),
                        (Constant(Value::Bool(true)), _) | (_, Constant(Value::Bool(true))) => {
                            Ok(Constant(Value::Bool(true)))
                        }
                        _ => Ok(e),
                    },
                    _ => Ok(e),
                })
            },
            // 如果是 filter转换后 predicate是ture 就不需要这个filterNode了
            &|n| match n {
                Node::Filter { source, predicate } => match predicate {
                    Constant(Value::Bool(true)) => Ok(*source),
                    p => Ok(Node::Filter {
                        source,
                        predicate: p,
                    }),
                },
                _ => Ok(n),
            },
        )
    }
}

/// 常量优化器 ， 如果表达式中只有常量 那就直接先进行常量计算
pub struct ConstantFolder;

impl Optimizer for ConstantFolder {
    fn optimize(&self, node: Node) -> Result<Node> {
        node.transform(&|e| Ok(e), &|e| {
            e.transform_expressions(
                &|e| {
                    if !e.contains(&|e| match e {
                        Expression::Field(_, _) => true,
                        _ => false,
                    }) {
                        Ok(Expression::Constant(e.evaluate(None)?))
                    } else {
                        Ok(e)
                    }
                },
                &|e| Ok(e),
            )
        })
    }
}

/// 谓词下推
pub struct FilterPushdown;
impl Optimizer for FilterPushdown {
    // 比如我的filter 下面是 scan 那么我就可以把filter放到scan中，这样就能尽早过滤不需要的数据
    // 也有可能是join join的话就需要将各自负责的谓词进行下推比如
    // .... from stu inner join class on stu.class_number = class.id where stu.age > 10 and class.name = '三班'
    // age需要下推到stu表的scan中， name需要下推到class
    fn optimize(&self, node: Node) -> Result<Node> {
        node.transform(
            &|n| match n {
                Node::Filter {
                    source,
                    mut predicate,
                } => {
                    match *source {
                        // filter下面是scan 那就把上层的filter下沉到下面
                        Node::Scan {
                            table,
                            alias,
                            mut filter,
                        } => {
                            let predicate = std::mem::replace(
                                &mut predicate,
                                Expression::Constant(Value::Bool(true)),
                            );
                            let filter = std::mem::replace(&mut filter, None);
                            let expr = if let Some(filter) = filter {
                                Expression::And(Box::new(filter), Box::new(predicate))
                            } else {
                                predicate
                            };
                            Ok(Node::Scan {
                                table,
                                alias,
                                filter: Some(expr),
                            })
                        }
                        Node::NestedLoopJoin {
                            left,
                            right,
                            predicate: mut join_predicate,
                            outer,
                            left_size,
                        } => {
                            let predicate = std::mem::replace(
                                &mut predicate,
                                Expression::Constant(Value::Bool(true)),
                            );
                            let filter = std::mem::replace(&mut join_predicate, None);
                            let expr = if let Some(filter) = filter {
                                Expression::And(Box::new(filter), Box::new(predicate))
                            } else {
                                predicate
                            };
                            // filter 刚开始是直接将filter修改成为scan或者nextedLoopJoin
                            // 不过后来发现就无法nextedLoopJoin的优化了...因为转换过后的节点相当于已经优化过了
                            // 所以需要在这里执行push_down_join
                            // 原来是想转换成为nextedLoopJoin然后再次递归的时候进行优化
                            self.push_down_join(Node::NestedLoopJoin {
                                left,
                                right,
                                predicate: Some(expr),
                                outer,
                                left_size,
                            })
                        }
                        _ => Ok(Node::Filter { source, predicate }),
                    }
                }
                Node::NestedLoopJoin { .. } => self.push_down_join(n),
                _ => Ok(n),
            },
            &|n| Ok(n),
        )
    }
}

impl FilterPushdown {
    fn push_down(node: Node, predicate: Option<Expression>) -> Result<Node> {
        if let Some(mut predicate) = predicate {
            Ok(match node {
                Node::Scan {
                    table,
                    alias,
                    filter,
                } => {
                    if let Some(filter) = filter {
                        predicate = Expression::And(Box::new(predicate), Box::new(filter));
                    }
                    Node::Scan {
                        table,
                        alias,
                        filter: Some(predicate),
                    }
                }
                Node::NestedLoopJoin {
                    left,
                    right,
                    left_size,
                    predicate: filter,
                    outer,
                } => {
                    if let Some(filter) = filter {
                        predicate = Expression::And(Box::new(predicate), Box::new(filter));
                    };
                    Node::NestedLoopJoin {
                        left,
                        right,
                        left_size,
                        predicate: Some(predicate),
                        outer,
                    }
                }
                _ => node,
            })
        } else {
            Ok(node)
        }
    }
    // 这里涉及到 合取析取范式 充充电再来
    // 科班学生应该知道 没错 就是离散数学没想到吧
    fn push_down_join(&self, node: Node) -> Result<Node> {
        // 首先要先合取范式 将所有按照 and 连接的式子进行解析
        match node {
            Node::NestedLoopJoin {
                mut left,
                mut right,
                left_size,
                predicate,
                outer,
            } => {
                if let Some(mut predicate) = predicate {
                    let cnf: Vec<Expression> = predicate.to_cnf_vec()?;
                    // 拿出来都是与连接的子句 这里就需要去汲取左表和右表的相关式子了
                    // 这里要注意 其实是排除含右表列的expression
                    // 反选获得
                    let (left_expr, cnf): (Vec<Expression>, Vec<Expression>) =
                        cnf.into_iter().partition(|e| {
                            debug!("{:#?}", e);
                            !e.contains(&|expr| match expr {
                                Expression::Field(i, _) => {
                                    if i >= &left_size {
                                        debug!("left left_size : {}", i);
                                        true
                                    } else {
                                        debug!("left left_size : {} return fasle", i);
                                        false
                                    }
                                }
                                _ => return false,
                            })
                        });
                    let (right_expr, cnf): (Vec<Expression>, Vec<Expression>) =
                        cnf.into_iter().partition(|e| {
                            !e.contains(&|expr| match expr {
                                Expression::Field(i, _) => {
                                    if i < &left_size {
                                        debug!("left right : {}", i);
                                        true
                                    } else {
                                        false
                                    }
                                }
                                _ => return false,
                            })
                        });

                    let right_expr = right_expr
                        .into_iter()
                        .map(|mut e| {
                            e.transform_ref(&|c| Ok(c), &|c| match c {
                                Expression::Field(i, f) => Ok(Expression::Field(i - left_size, f)),
                                _ => Ok(c),
                            })?;
                            return Ok(e);
                        })
                        .collect::<Result<Vec<_>>>()?;

                    // 此时cnf中包含的就是同时含有两个表的字段
                    // 将只包含左右表的字段进行下沉
                    left = Box::new(Self::push_down(*left, Expression::from_cnf_vec(left_expr))?);
                    right = Box::new(Self::push_down(
                        *right,
                        Expression::from_cnf_vec(right_expr),
                    )?);

                    Ok(Node::NestedLoopJoin {
                        left,
                        right,
                        left_size,
                        predicate: Expression::from_cnf_vec(cnf),
                        outer,
                    })
                } else {
                    Ok(Node::NestedLoopJoin {
                        left,
                        right,
                        left_size,
                        predicate,
                        outer,
                    })
                }
            }
            _ => {
                return Err(Error::Optimizer(format!(
                    "expect NestedLoopJoin get {}",
                    node
                )))
            }
        }
    }
}

///  寻找索引
pub struct IndexLookup<'a> {
    catalog: &'a dyn Catalog,
}

impl<'a> IndexLookup<'a> {
    pub fn new(catalog: &'a dyn Catalog) -> Box<Self> {
        Box::new(Self { catalog })
    }
}

impl<'a> Optimizer for IndexLookup<'a> {
    fn optimize(&self, node: Node) -> Result<Node> {
        node.transform(
            &|n| match &n {
                Node::Scan {
                    table,
                    alias,
                    filter,
                } => {
                    if let Some(mut filter) = filter.clone() {
                        let table = self.catalog.must_read_table(table.as_str())?;

                        let key_index = table.columns.iter().position(|e| e.primary_key).ok_or(
                            Error::Optimizer(format!("failed to get table:{} key", table.name)),
                        )?;

                        let indexs: Vec<(usize, String)> = table
                            .columns
                            .clone()
                            .into_iter()
                            .enumerate()
                            .filter(|(_, e)| e.index)
                            .map(|(i, e)| (i, e.name))
                            .collect();

                        let mut cnf = filter.to_cnf_vec()?;
                        for (index, e) in cnf.clone().iter().enumerate() {
                            if let Some(vals) = e.look_up(key_index) {
                                cnf.remove(index);
                                let mut node = Node::KeyLookup {
                                    table: table.name.clone(),
                                    alias: alias.clone(),
                                    keys: vals,
                                };
                                if let Some(predicate) = Expression::from_cnf_vec(cnf) {
                                    node = Node::Filter {
                                        source: Box::new(node),
                                        predicate,
                                    }
                                }
                                return Ok(node);
                            }

                            for (i_index, name) in indexs.clone().into_iter() {
                                if let Some(vals) = e.look_up(i_index) {
                                    cnf.remove(index);
                                    let mut node = Node::IndexLookup {
                                        table: table.name.clone(),
                                        alias: alias.clone(),
                                        values: vals,
                                        column: name,
                                    };
                                    if let Some(predicate) = Expression::from_cnf_vec(cnf) {
                                        node = Node::Filter {
                                            source: Box::new(node),
                                            predicate,
                                        }
                                    }
                                    return Ok(node);
                                }
                            }
                        }
                    }
                    Ok(n)
                }
                _ => Ok(n),
            },
            &|n| Ok(n),
        )
    }
}

/// join优化 如果是两个字段相等的连接 可以使用hashJoin
pub struct JoinType;

impl Optimizer for JoinType {
    fn optimize(&self, node: Node) -> Result<Node> {
        use Expression::Field;
        node.transform(
            &|n| match n {
                Node::NestedLoopJoin {
                    left,
                    right,
                    predicate,
                    outer,
                    left_size,
                } => match predicate {
                    // Join优化要一定在下推优化之后，
                    // 这样就保证这里的predicate如果相等，肯定是包含了两个表的字段
                    // 那么就是一个左表的，一个是右表的
                    // 就下面这个case重要一些 下面写的比较丑陋 以后看看怎么写的优雅一点
                    Some(Expression::Equal(e1, e2)) => match (*e1, *e2) {
                        (Field(i1, l1), Field(i2, l2)) => {
                            let (left_field, right_field) = if i1 < i2 {
                                ((i1, l1), (i2, l2))
                            } else {
                                ((i2, l2), (i1, l1))
                            };

                            Ok(Node::HashJoin {
                                left,
                                left_field,
                                right,
                                right_field,
                                outer,
                            })
                        }
                        (e1, e2) => Result::Ok(Node::NestedLoopJoin {
                            left,
                            right,
                            predicate: Some(Expression::Equal(Box::new(e1), Box::new(e2))),
                            outer,
                            left_size,
                        }),
                    },
                    _ => Ok(Node::NestedLoopJoin {
                        left,
                        right,
                        predicate,
                        outer,
                        left_size,
                    }),
                },
                _ => Ok(n),
            },
            &|n| Ok(n),
        )
    }
}
