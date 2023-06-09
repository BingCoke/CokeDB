# 执行器
执行器没什么好说的
就是基于plan生成的node去执行 基于火山模型 迭代思想
- aggrgation.rs 聚合函数的执行
- join.rs 执行连接的相关操作
- mutation.rs 增删改
- schema.rs 对表的一些操作，只支持增删操作
- query.rs 查询操作，这里指的不是底层查询，filter,sort等
- source.rs 这里是底层的查询，从存储中拿出数据
