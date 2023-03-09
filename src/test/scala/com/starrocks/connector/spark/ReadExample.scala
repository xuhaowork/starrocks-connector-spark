package com.starrocks.connector.spark

import org.apache.spark.sql.SparkSession

/**
 * Read Example:
 * 执行方式:
 * {{{
 * spark-submit your-jar-with-dependency.jar <your-fe-nodes> <user> <password> <dbtable>
 * }}}
 *
 */
object ReadExample {
  val spark: SparkSession = SparkSession.builder()
    .appName("starrocks-connector-spark-test")
    .master("local[*]") // or yarn
    .getOrCreate()

  def main(args: Array[String]): Unit = {
    val fenodes = args(0)
    val user = args(1)
    val password = args(2)
    val dbtable = args(3)

    val data = spark.read
      .format("starrocks")
      .option("starrocks.fenodes", fenodes)
      .option("user", user)
      .option("password", password)
      .option("starrocks.table.identifier", dbtable)
      .load()

    data.show()

  }

}
