# starrocks-connector-spark
[apache spark](https://github.com/apache/spark)读写[starrocks](https://github.com/StarRocks/starrocks)的工具包，读写一体，结合了[doris-spark-connector](https://github.com/apache/doris-spark-connector)和[starrocks-connector-for-apache-spark](https://github.com/StarRocks/starrocks-connector-for-apache-spark)。


## 背景
- [doris-spark-connector](https://github.com/apache/doris-spark-connector)不支持写入starrocks    
[doris-spark-connector](https://github.com/apache/doris-spark-connector)写入时获取be节点的接口与starrocks略有出入, 因此会导致无法写入.

- [starrocks-connector-for-apache-spark](https://github.com/StarRocks/starrocks-connector-for-apache-spark)不支持写入   
ps: 最近我又看了下源码发现有支持写入的分支推上去了, 但有些依赖的sdk包拉不下来，无法完成编译，还是没法用。

将两个结合一下，现阶段主要是方便自用。等后面starrocks-connector-for-apache-spark生态比较好了直接用starrocks-connector-for-apache-spark。

## Build

如果要引用该工具需要手动编译。

```bash
mvn install -Dskiptest
# 如果需要频繁使用可以放到maven私服
mvn deploy # you need to set your distribution manager before
```

## Usage

方式一，pom.xml中添加依赖包（需要上述build过程中deploy到maven repository）：

```xml
<dependency>
    <groupId>cn.howard</groupId>
    <artifactId>starrocks-connector-spark</artifactId>
    <version>0.1.0-SNAPSHOT</version>
    <scope>compile</scope>
</dependency>
```

方式二，如果没有传到私服，需要通过system的方式将[Build](#Build)过程生成的jar包引入进来：

```xml
<dependency>
    <groupId>cn.howard</groupId>
    <artifactId>starrocks-connector-spark</artifactId>
    <version>0.1.0-SNAPSHOT</version>
    <systemPath>your_path/starrocks-connector-spark-0.1.0-SNAPSHOT.jar</systemPath>
    <scope>system</scope>
</dependency>
```

your_path是工程中jar包存放的相对路径。

## Reading

```scala
val fenodes = args(0) // eg: 127.0.0.1:8030
val user = args(1) // eg: admin
val password = args(2) // eg: 123456
val dbtable = "test_table"

val data = spark.read
  .format("starrocks")
  .option("starrocks.fenodes", fenodes)
  .option("user", user)
  .option("password", password)
  .option("starrocks.table.identifier", dbtable)
  .load()
data.show()
```



## Writing

- 需要先创建表
- 仅支持append方式持续写入

示例:

1)创建表格    

```sql
CREATE TABLE `your_db`.`test_table`
(
    `code`  varchar(255) NOT NULL COMMENT '学号',
    `name`  varchar(255) NOT NULL COMMENT '姓名',
    `value` double       NOT NULL COMMENT '成绩'
) ENGINE = OLAP PRIMARY KEY(`code`)
COMMENT '测试starrocks-connector-spark读写'
DISTRIBUTED BY HASH(`code`) BUCKETS 4
PROPERTIES (
  "replication_num" = "3",
  "in_memory" = "false",
  "storage_format" = "DEFAULT"
);
```

2)写入代码

```scala
val fenodes = args(0) // eg: 127.0.0.1:8030
val user = args(1) // eg: admin
val password = args(2) // eg: 123456
val dbtable = "your_db.test_table"

val data = spark.createDataFrame(
  Seq(
    ("1001", "张三", 103.5),
    ("1001", "李四", 93.12),
    ("1001", "王五", 119.8),
    ("1001", "赵六", 112.7),
  )
).toDF("code", "name", "value")
data.write.format("starrocks")
  .option("starrocks.fenodes", fenodes)
  .option("user", user)
  .option("password", password)
  .option("starrocks.table.identifier", dbtable)
  .save()
```













