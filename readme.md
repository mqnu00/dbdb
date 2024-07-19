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

## 预计效果

原子性

一致性

隔离性

持久性