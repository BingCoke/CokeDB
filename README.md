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

### cli 运行

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
