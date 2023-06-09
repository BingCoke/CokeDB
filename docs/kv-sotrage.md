
# kv存储
主要是两个文件
- encoding.rs 这里通过base编解码数据
- mvss.rs mvcc的实现，基于版本的实现  [link](https://levelup.gitconnected.com/implementing-your-own-transactions-with-mvcc-bba11cab8e70)

## encoding相关
数据库是基于kv存储的 所以所有的数据都是kv保存
所以我们需要保存什么样的数据就需要有对应的key
行数据的key就是id
表数据的key就是table name...
但是存储的时候都是字节，为了区分读取的数据，需要在key前面添加字节，例如 
## mvcc
对于表中的行数据修改时，会在encoding的时候添加
