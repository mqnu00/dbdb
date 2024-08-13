# dbdb

[500lines/data-store at master · aosabook/500lines (github.com)](https://github.com/aosabook/500lines/tree/master/data-store)

# 可借鉴的存储模型设计

bitcask：日志的预写入、kv分离（key在内存哈希，value在存储追加）、

LSM：日志的预写入（内存树结构、存储追加）

## 底层数据结构

跳表：多层索引

树：b+ b- 对树结构的持久化

哈希：不冲突的情况下性能最优

# 设计

## 存储方式

`key`在内存中，使用跳表来索引，退出时持久化在硬盘方便下次重建索引

`value`在硬盘中，使用文件追加方式。

## 运行逻辑

数据库提供了5个接口：是否存在（contain），新增数据（set），删除数据（del），查找数据（get），提交（commit）。

更改数据包含在`set`内，这是由于数据的`key`和数据在磁盘的`address`作为元组保存在内存中，无法修改，所以需要先删除数据，再新增数据。

`set`、`get`、`del`都要求先判断`contain`。

### 磁盘的数据储存

|         length         |                data                |
| :--------------------: | :--------------------------------: |
| 数据的长度（几个字节） | pickle.loads(data) -> (key, value) |

由这样一条一条组成。

新增的数据从文件的尾部增加。

实际上这个结构应该是

```
{
	int key_length,
	int value_length,
	char key[key_length],
	char value[value_length]
}
```

只是data会通过pickle包去解析成元组，所以我没有去区分key、value可变长。

#### 校验

length和data的校验需要分开。

因为，data的长度是不知道的，就算想将length和data合在一起校验，也不知道读到哪里再校验。

##### length校验

不建议哈希，因为length只有8字节，使用哈希的话，校验位短了会提高碰撞概率，长了显得浪费。

推荐的校验方法可能有奇偶校验，CRC，校验和，汉明码等，留待讨论。

##### data校验

哈希

### 内存的key结构

| key  |         value_pos          | data_length |
| :--: | :------------------------: | :---------: |
|      | data在文件内的位置（指针） | data的长度  |

我最开始的想法是内存仅有`key`和`value_pos`，但是忽略了`data_length`，这导致读取数据会需要两次磁盘IO操作，一次是从文件读取`data_length`，第二次读取`data`

### 其他

`commit`：新建tmp_dbname文件，根据内存中的`key`和`address`将数据从原来的文件（`dbname`）复制到`tmp_dbname`，然后删除`dbname`，重命名`tmp_dbname`为`dbname`。

## 预计效果

原子性

一致性

隔离性

持久性

## 未来设计

日志操作记录，数据恢复

快照

数据库语法分析

网络层，通信协议



