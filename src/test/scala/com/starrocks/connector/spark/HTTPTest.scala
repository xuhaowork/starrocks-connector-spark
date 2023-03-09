package com.starrocks.connector.spark

import com.starrocks.connector.spark.StreamLoader.get_be_url
import org.apache.http.impl.client.{CloseableHttpClient, DefaultConnectionKeepAliveStrategy, DefaultRedirectStrategy, HttpClientBuilder}

object HTTPTest {
  def main(args: Array[String]): Unit = {
    val host = "xxx"
    val port = 8030
    val user = "xxx"
    val password = "xxxx"
    val database = "your_db"
    val table = "your_table"
    val httpClient: CloseableHttpClient = HttpClientBuilder.create()
      .setKeepAliveStrategy(new DefaultConnectionKeepAliveStrategy)
      .setRedirectStrategy(new DefaultRedirectStrategy() { // refer to linux cmd: curl --location-trusted
        override def isRedirectable(method: String): Boolean = {
          super.isRedirectable(method)
          true
        }
      }).build()

    val be_url = get_be_url(
      httpClient,
      host,
      port,
      database,
      table,
      user,
      password
    )


  }

}
