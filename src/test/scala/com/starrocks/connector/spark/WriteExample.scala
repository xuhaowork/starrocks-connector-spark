package com.starrocks.connector.spark

import org.apache.spark.sql.SparkSession

/**
 * Write Example:
 * 需要先创建表格:
 * {{{
 * CREATE TABLE `your_db`.`test_table`
 *     (
 *         `code`  varchar(255) NOT NULL COMMENT '学号',
 *         `name`  varchar(255) NOT NULL COMMENT '姓名',
 *         `value` double       NOT NULL COMMENT '成绩'
 *     ) ENGINE = OLAP PRIMARY KEY(`code`)
 *     COMMENT '测试starrocks-connector-spark读写'
 *     DISTRIBUTED BY HASH(`code`, `table_name`) BUCKETS 4
 *     PROPERTIES (
 *       "replication_num" = "3",
 *       "in_memory" = "false",
 *       "storage_format" = "DEFAULT"
 *     );
 * }}}
 * 执行:
 * {{{
 * spark-submit your-jar-with-dependency.jar <your-fe-nodes> <user> <password> test_table
 * }}}
 */
object WriteExample {
  val spark: SparkSession = SparkSession.builder()
    .appName("starrocks-connector-spark-test")
    .master("local[*]") // or yarn
    .getOrCreate()

  def main(args: Array[String]): Unit = {
    val fenodes = args(0)
    val user = args(1)
    val password = args(2)
    val dbtable = args(3) // test_table in this case

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

  }

}
