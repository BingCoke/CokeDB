# 知识储备

## [Precedence climbing 优先级爬升算法](https://eli.thegreenplace.net/2012/08/02/parsing-expressions-by-precedence-climbing)
该算法用于表达式解析
中文说明[掘金](https://juejin.cn/post/6844904019278708744#heading-3)
我们将每一个操作符号赋予一个优先级+结合性 
粘性分为左粘性，右粘性 比如+,-,*,/ 就是左结合性  而 ^,+(正号),-（负号)就是右结合性（所以掘金的有问题）

这里不多bb了，直接给出符号的优先级以及结合性
| Precedence | Operator                 | Associativity |
| ---------- | ------------------------ | ------------- |
| 9          | `+`, `-`, `NOT` (prefix) | Right         |
| 8          | `!`, `IS` (postfix)      | Left          |
| 7          | `^`                      | Right         |
| 6          | `*`, `/`, `%`            | Left          |
| 5          | `+`, `-`                 | Left          |
| 4          | `>`, `>=`, `<`, `<=`     | Left          |
| 3          | `=`, `!=`, `LIKE`        | Left          |
| 2          | `AND`                    | Left          |
| 1          | `OR`                     | Left          |

# 实现

## 词法分析器 lexer

总的来说首先就是将传入的字符串转换为 token 迭代器，一个个取出 token，
那我们首先需要将 token 分类

- keyword 就是 sql 语句中关键字
- string 由双引号包裹
- number 数字
- indent 标识，比如表名，列名
- 特殊符号 例如加减乘除

```rust
pub enum Token {
    Number(String),
    String(String),
    Ident(String),
    Keyword(Keyword),
    // .
    Period,
    // =
    Equal,
    // >
    GreaterThan,
    // >=
    GreaterThanOrEqual,
    // <
    LessThan,
    // <=
    LessThanOrEqual,
    // <>
    LessOrGreaterThan,
	....
}
```

lexer 不断的 next 去获取下一个 token

## parser 解析器

当我们能够通过 lexer 获得 token 迭代器之后就可以尝试进行将 token 解析成为一个 ast 语法树

### statement
根据不同的 sql 语句我们需要生成不同的语法树比如 select,delete,update....

```rust
pub enum Statement{
    // delete 语句需要知道删除的table 删除条件
    Delete {
        table: String,
        r#where: Option<Expression>,
    },
    // insert 需要知道插入的table,需要插入哪个字段，每个字段对应的值
    Insert {
        table: String,
        columns: Option<Vec<String>>,
        values: Vec<Vec<Expression>>,
    },
    ....
}
```

### base_expression
expression是表达式，可以用于计算 比如 1+3*2 可以用于条件解析例如 table.filed=12
从token中可以转成初步的expression
> 为什么是初步的expression？一般来讲就直接生成最终expression了，因为目前使用表达式解析的算法是优先级爬升算法，按照面向对象的原则来讲，
我们需要将表达式分成atom,operator 而它们还有细分， atom 有 field,value,function 而 operator 有 前缀 后缀 中缀
我们需要解析他们并转换成expression,所以一口吃成一个大胖子也不合适。先转成一个初步的expression最终转换成final_expression。


#### select
select * from where join  group_by order having 
- select_expr 查询的结果 每个列由逗号隔开 组成vec<(base_expression,Option(String))> 示例 select stu.name stu_name, max(age) from .. 所以 expression中元素需要包含field,function 
- where_expr filter表达式 整个表达式为true这一列才可以加入结果集  base_expression
- group_by 分组的expr, 相同结果的expr列同为一组 vec<base_expression>
- having expr 条件过滤分组 一般与group_by一起使用使用sum等聚合函数 例如 .... having max(age) > 18 and sum(age) > 100  base_expression 
- order 排序，根据某个结果列进行排序 vec<(base_expression,order_type)>
- offset 表示从第几条数据开始取 base_expression ***表达式结果必须是固定值比如12或者10+2***
- limit 表示取多少数据  base_expression ***表达式结果必须是固定值比如12或者10+2***
### update
- table 表示更新的表 String
- filter 满足条件的行进行更新 base_expression
- set 表示进行更新 由多个expr组成 ... set name="xiaoming",age=19 ... 这里name="xiaoming"最终被表示为一个 base_expression
### Insert
- table 插入的表 String
- columns 插入哪些列option<vec<String>> 如果是None就是插入全列
- values vec<base_expression> ***表达式结果必须是固定值***

### delete

- table 更新的表 String
- filter 满足条件的行 base_expression
### drop_table
- 删除的表 Strign
### CreateTable
- table_name  表明 String
- 列 Vec<Column>

# 
