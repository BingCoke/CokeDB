# CokeDB

Coke 是由 rust 语言编写，基于 KV 存储的关系型数据库。
创建此项目的目标是学习 rust 语言和关系型相关实现原理数据库。

## 该项目目前的特性

- 基于优先爬升算法实现表达式解析生成语法解析树，基于火山模型生成抽象语法树
- 支持 sql 解析 filter, joins, aggregate, projections 等
- 实现谓词下推，索引等优化
- 实现 mvcc 多版本并发控制，实现可重复读隔离级别

## next(正在努力实现)

- [ ] 基于 raft 实现分布式系统
- [ ] 自己实现底层存储 B+Tree 和跳表

## 运行

使用 cargo build --release 进行编译

### server 端执行

在项目根目录**/target/release**中 有 dbserver 二进制可执行文件,使用 -h 参数查看 help

```shell
> ./dbserver -h
this is db server

Usage: dbserver [OPTIONS]

Options:
  -c, --config <CONFIG>  config file path [default: /home/bk/.config/coke_db/coke_db.yml]
  -h, --help             Print help
  -V, --version          Print version
```

**_config 默认读取$HOME/.config/coke_db/coke_db.yml_**

> 也可以直接使用 cargo run --bin dbserver

### client 运行

同样在项目根目录**_/target/release_**中 有 dbcli 二进制可执行文件,使用 -h 参数查看 help

```shell
> ./dbcli -h
this is a dbcli to connect CokeDB

Usage: dbcli [OPTIONS]

Options:
  -H, --host <HOST>  [default: 127.0.0.1]
  -p, --port <PORT>  port column headers [default: 9653]
  -h, --help         Print help
  -V, --version      Print version
```

> 也可以直接使用 cargo run --bin dbcli

### cli 使用

在客户端输入 **!h** 可获得帮助

```shell
coke_db >> !h

ctrl+c => quit
!tables => get all tables
!table <table> => get table
!status => get status

```

## sql 语句

> 可能某些复杂的查询语句仍有问题 示例相关语句是完全支持的 正在积极寻找 bug 并解决中

### 示例数据插入

```sql
CREATE TABLE student (
    id INTEGER PRIMARY KEY,
    sex BOOL NOT NULL,
    year INTEGER NOT NULL,
    name STRING NOT NULL
);


INSERT INTO student VALUES
(1,true,2001,"xiaoming"),
(2,false,2002,"xiaohong"),
(3,true,2002,"xiaogang"),
(4,false,2003,"xiaoli");



CREATE TABLE course (
    id INTEGER PRIMARY KEY,
    name STRING NOT NULL
);

INSERT INTO course VALUES (1,"语文"),(2, "数学"),(3,"英语");


CREATE TABLE grade (
    id INTEGER PRIMARY KEY,
    stu_id INTEGER NOT NULL,
    course_id INTEGER NOT NULL,
    grade FLOAT NOT NULL
);


INSERT INTO grade VALUES
(1,1,1,99.0),
(2,1,2,80.0),
(3,2,3,70.0),
(4,2,1,99.0)
;

```

### Create table

大致语法

```sql
CREATE TABLE <table_name> {
    [cloumns]
}
```

cloumns 书写

```sql
cloumn_name column_type [Options]
```

可支持的 type 有:

- Bool
- Char
- Double
- Float
- Integer
- String

支持的 option

- Primary_key (目前只支持一个字段为主键)
- Index (无法形成联合索引)
- Unique
- Not Null
- Null
- Default <表达式>

> 不支持外键

### Drop Table

```sql
DROP  TABLE <table_name>
```

### Select

支持多表联查，算术基本计算，聚合函数，排序, limit, offset 等

```coke_db
coke_db >> select (1.0+4)/2 as res ;

res
2.5

---
coke_db >> select * from student;

id|sex|year|name
1|TRUE|2001|xiaoming
2|FALSE|2002|xiaohong
3|TRUE|2002|xiaogang
4|FALSE|2003|xiaoli

---
coke_db >>
SELECT id,name,2023-year as age FROM student
WHERE year >= 2001 AND sex
ORDER BY age ASC;

id|name|age
3|xiaogang|21
1|xiaoming|22

---
coke_db >>
select count(*),average(2023-year),sum(2023-year)
FROM student
group by student.sex;

Count|Average|Sum
2|21|43
2|20|41

---
coke_db >>
SELECT student.name student_name,course.name course_name, grade
FROM student,grade,course
where
course.id = grade.course_id AND grade.stu_id=student.id
ORDER BY grade;

student_name|course_name|grade
xiaoming|语文|99
xiaohong|语文|99
xiaoming|数学|80
xiaohong|英语|70

---

coke_db >>
SELECT student.name student_name,course.name course_name, grade
FROM
student
LEFT JOIN grade
on student.id = grade.stu_id
LEFT JOIN course
on course.id = grade.course_id
ORDER BY grade;

student_name|course_name|grade
xiaoming|语文|99
xiaohong|语文|99
xiaoming|数学|80
xiaohong|英语|70
```

### Insert

```
Insert Into <table_name> ([column_name]) values ([values])
```

支持多行插入,cloumn_name 为可选 sql 项，如果不写默认插入所有字段

### Delete

```sql

DELETE FROM <table_name> where <expression>

```

### Update

```sql

UPDAET  <table_name> SET <expression> where <expression>

```

### EXPLAIN

```
coke_db >> EXPLAIN
SELECT student.name student_name,course.name course_name, grade
FROM student,grade,course
where
course.id = grade.course_id AND grade.stu_id=student.id

Projection: student.name, course.name, grade
  └─ NestedLoopJoin: inner on course.id = grade.course_id
      ├─ NestedLoopJoin: inner on grade.stu_id = student.id
      │  ├─ Scan: student
      │  └─ Scan: grade
      └─ Scan: course
```

### 事务的支持

#### Commit

in client A

```sql

coke_db >> begin transaction;
Began transaction 56

coke_db: 56 >> update grade set grade=77.0 where id=1;
Updated 1 rows

coke_db: 56 >> select * from grade;
id|stu_id|course_id|grade
1|1|1|77
2|1|2|80
3|2|3|70
4|2|1|99
coke_db: 56 >> commit;
Committed transaction 56

coke_db >>  select * from grade;
id|stu_id|course_id|grade
1|1|1|77
2|1|2|80
3|2|3|70
4|2|1|99

```

in client B
在 client A 事务提交之前

```sql
coke_db >> select * from grade;

id|stu_id|course_id|grade
1|1|1|99
2|1|2|80
3|2|3|70
4|2|1|99

```

#### Rollback

```sql

coke_db >> begin transaction;
Began transaction 54

coke_db: 54 >> update grade set grade=77.0 where id=1;
Updated 1 rows

coke_db: 54 >> select * from grade;
id|stu_id|course_id|grade
1|1|1|77
2|1|2|80
3|2|3|70
4|2|1|99

coke_db: 54 >> rollback;
Rolled back transaction 54

coke_db >> select * from grade;

id|stu_id|course_id|grade
1|1|1|99
2|1|2|80
3|2|3|70
4|2|1|99
```
