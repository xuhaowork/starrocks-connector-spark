package com.starrocks.connector.spark

import com.fasterxml.jackson.databind.ObjectMapper
import com.starrocks.connector.spark.rest.RestService
import org.apache.commons.io.IOUtils
import org.apache.http.HttpStatus
import org.apache.http.client.config.RequestConfig._
import org.apache.http.client.methods.{HttpGet, HttpPost, HttpRequestBase}
import org.junit.Test
import org.slf4j.Logger

import java.io.{BufferedReader, IOException, InputStream, InputStreamReader, PrintWriter}
import java.net.{HttpURLConnection, URL}
import java.nio.charset.StandardCharsets
import java.util.{ArrayList, Base64, HashMap => juHashMap, Map => juMap}
import java.util


object ObjectMapperTest {
  def send(request: HttpRequestBase, logger: Logger): String = {
    null
  }

  def main(args: Array[String]): Unit = {
    val host = "xxx.xxx.xxx"
    val port = 8030
    val database = "your_db"
    val table = "your_tb"
    val httpGet = new HttpGet(s"http://${host}:${port}/api/${database}/${table}/_stream_load")



  }

}
