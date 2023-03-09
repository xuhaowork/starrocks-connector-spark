# starrocks-connector-spark
spark读写starrocks的工具包，读写一体，结合了https://github.com/apache/doris-spark-connector和https://github.com/StarRocks/starrocks-connector-for-apache-spark


## 背景
- doris-spark-connector不支持写入starrocks    
https://github.com/apache/doris-spark-connector获取be节点的接口与starrocks支持的不一致, 因此会导致无法写入.

- starrocks-connector-for-apache-spark不支持写入   
此前不支持写入，最近我又看了下源码发现有支持写入的分支推上去了, 但有些sdk包拉不下来

将两个结合一下，方便自用。等后面starrocks-connector-for-apache-spark生态比较好了直接用starrocks-connector-for-apache-spark。

## build


## 读


## 写

