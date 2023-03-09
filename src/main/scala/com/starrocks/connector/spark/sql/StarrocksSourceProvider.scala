// Modifications Copyright 2021 StarRocks Limited.
//
// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package com.starrocks.connector.spark.sql

import com.starrocks.connector.spark.StreamLoader
import com.starrocks.connector.spark.cfg.{ConfigurationOptions, SparkSettings}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.execution.streaming.Sink
import org.apache.spark.sql.sources._
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}

import scala.collection.JavaConverters.mapAsJavaMapConverter
import scala.collection.mutable.ArrayBuffer
import scala.util.matching.Regex

/**
 * StarRocks的读写实现类
 */
class StarrocksSourceProvider extends DataSourceRegister
  with RelationProvider
  with CreatableRelationProvider
  with StreamSinkProvider
  with Logging
  with Serializable {
  override def shortName(): String = "starrocks"

  override def createRelation(sqlContext: SQLContext, parameters: Map[String, String]): BaseRelation = {
    new StarrocksRelation(sqlContext, Utils.params(parameters, log))
  }

  override def createRelation(sqlContext: SQLContext,
                              mode: SaveMode,
                              parameters: Map[String, String],
                              data: DataFrame): BaseRelation = {
    val sparkSettings = new SparkSettings(sqlContext.sparkContext.getConf)
    sparkSettings.merge(Utils.params(parameters, log).asJava)

    val feNodesString = sparkSettings.getProperty(ConfigurationOptions.STARROCKS_FENODES)
    val HOST_PORT_PATTERN = new Regex("(.*):(.*)")
    val feNodes = feNodesString.split(",").map {
      case HOST_PORT_PATTERN(host, port) => (host, port.toInt)
      case _ => throw new IllegalArgumentException(s"输入的参数${ConfigurationOptions.STARROCKS_FENODES}不合法, 形式应为: 'ip:port,ip2:port2'")
    }
    val dbtable: String = sparkSettings.getProperty(ConfigurationOptions.STARROCKS_TABLE_IDENTIFIER)
    if(dbtable == null) {
      throw new IllegalArgumentException(s"未设置${ConfigurationOptions.STARROCKS_TABLE_IDENTIFIER}")
    }
    if(dbtable.split("\\.").length != 2) {
      throw new IllegalArgumentException(s"${ConfigurationOptions.STARROCKS_TABLE_IDENTIFIER}形式为database.table形式")
    }

    val Array(database, table) = dbtable.split("\\.")
    val user: String = parameters("user")
    val password: String = parameters("password")
    // init stream loader
    val streamLoader = new StreamLoader(feNodes, database, table, user, password, data.columns, sparkSettings)

    val maxRowCount = sparkSettings.getIntegerProperty(ConfigurationOptions.STARROCKS_BATCH_SIZE, ConfigurationOptions.STARROCKS_BATCH_SIZE_DEFAULT)
    val maxRetryTimes = sparkSettings.getIntegerProperty(ConfigurationOptions.STARROCKS_REQUEST_RETRIES, ConfigurationOptions.STARROCKS_REQUEST_RETRIES_DEFAULT)
    val intervalBetweenRetry = sparkSettings.getLongProperty(ConfigurationOptions.INTERVAL_BETWEEN_RETRY, ConfigurationOptions.INTERVAL_BETWEEN_RETRY_DEFAULT)

    log.info(s"maxRowCount ${maxRowCount}")
    log.info(s"maxRetryTimes ${maxRetryTimes}")

    data.rdd.foreachPartition {
      partition =>
        // buffer为当前分区数据数和starrocks.batch.size的最小值
        val rowsBuffer = ArrayBuffer.empty[Seq[Any]]

        partition.foreach {
          row =>
            rowsBuffer.append(row.toSeq)
            if (rowsBuffer.size > maxRowCount) {
              runWithMaxTry(maxRetryTimes, intervalBetweenRetry) {
                streamLoader.loadAsJson(rowsBuffer)
                rowsBuffer.clear()
              }(streamLoader.loadBeUrl())
            }
        }
        if (rowsBuffer.nonEmpty) {
          runWithMaxTry(maxRetryTimes, intervalBetweenRetry) {
            streamLoader.loadAsJson(rowsBuffer)
            rowsBuffer.clear()
          }(streamLoader.loadBeUrl())
        }
    }


    new BaseRelation {
      override def sqlContext: SQLContext = unsupportedException

      override def schema: StructType = unsupportedException

      override def needConversion: Boolean = unsupportedException

      override def sizeInBytes: Long = unsupportedException

      override def unhandledFilters(filters: Array[Filter]): Array[Filter] = unsupportedException

      private def unsupportedException =
        throw new UnsupportedOperationException("BaseRelation from doris write operation is not usable.")
    }
  }

  override def createSink(sqlContext: SQLContext, parameters: Map[String, String], partitionColumns: Seq[String], outputMode: OutputMode): Sink = {
    throw new UnsupportedOperationException("暂不支持")
  }

  private def createStreamLoader(): Unit = {
    throw new UnsupportedOperationException()
  }


  def runWithMaxTry[U](maxRetryTimes: Int, intervalBetweenRetry: Long)(body: => U)(flushStatus: => Unit): U = {
    var finished: Boolean = false
    var times: Int = 0
    var result: U = null.asInstanceOf[U]

    while (!finished && times <= maxRetryTimes) {
      try {
        result = body
        finished = true
      } catch {
        case e: Throwable =>
          if (times == maxRetryTimes) {
            log.debug(s"try $maxRetryTimes times but failed: ${e.getMessage}")
            throw e
          } else {
            log.debug(e.getMessage)
            Thread.sleep(intervalBetweenRetry)
            flushStatus
            log.debug("now try next time")
          }
      }

      times += 1
    }
    result
  }


}


object StarrocksSourceProvider