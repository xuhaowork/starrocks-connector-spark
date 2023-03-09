package com.starrocks.connector.spark

import com.fasterxml.jackson.databind.ObjectMapper
import com.starrocks.connector.spark.cfg.SparkSettings
import org.apache.commons.net.util.Base64
import org.apache.http.HttpHeaders
import org.apache.http.client.config.RequestConfig
import org.apache.http.client.methods.HttpPut
import org.apache.http.entity.StringEntity
import org.apache.http.impl.client.{CloseableHttpClient, DefaultConnectionKeepAliveStrategy, DefaultRedirectStrategy, HttpClientBuilder}

import java.io.{BufferedOutputStream, BufferedReader, IOException, InputStream, InputStreamReader}
import java.net.{HttpURLConnection, URL}
import java.nio.charset.{Charset, StandardCharsets}
import java.util
import java.util.{Date, Random}

class StreamLoader(
                    val feNodes: Array[(String, Int)],
                    val database: String,
                    val table: String,
                    val user: String,
                    val password: String,
                    val columns: Array[String],
                    settings: SparkSettings) extends Serializable {

  import StreamLoader._


  private val authEncoding: String = basicAuthHeader(user, password)

  var loadUrl: String = _

  /**
   * 随机选择FE节点
   *
   * @return ip, port
   */
  def randomChooseFE(): (String, Int) = {
    val rd = new Random()
    feNodes.apply(rd.nextInt(feNodes.length))
  }

  /**
   * 获取BE节点
   */
  def loadBeUrl(): Unit = {
    val (host, port) = randomChooseFE()
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
    httpClient.close()
    loadUrl = be_url
  }

  loadBeUrl()

  lazy val maxFilterRatio: String = settings.getProperty("max_filter_ratio")

  /**
   * 创建HTTP连接
   *
   * @param urlStr 连接url
   * @param label  标签(用于标记一次连接)
   * @throws IOException 连接异常
   * @return HTTP连接
   */
  @throws[IOException]
  private def getConnection(urlStr: String, label: String): HttpURLConnection = {
    val url = new URL(urlStr)
    val conn = url.openConnection.asInstanceOf[HttpURLConnection]
    conn.setInstanceFollowRedirects(false)
    conn.setRequestMethod("PUT")
    conn.setRequestProperty("Authorization", authEncoding)
    conn.addRequestProperty("Expect", "100-continue")
    conn.addRequestProperty("Content-Type", "text/plain; charset=UTF-8")
    conn.setDoOutput(true)
    conn.setDoInput(true)
    conn.addRequestProperty("format", "json")
    conn.addRequestProperty("strip_outer_array", "true")
    conn.addRequestProperty("label", label)

    if (columns != null && columns.nonEmpty)
      conn.addRequestProperty("columns", columns.mkString(","))
    if (maxFilterRatio != null && !(maxFilterRatio == ""))
      conn.addRequestProperty("max_filter_ratio", maxFilterRatio)

    conn
  }

  /**
   * 写入字符串
   *
   * @param value json字符串
   * @return (http请求返回代码, msg, 返回数据)
   */
  def loadAsJson(value: String): (Int, String, String) = {
    val label = table + "_" + System.currentTimeMillis()
    val conn = getConnection(loadUrl, label)

    var status = -1
    try {
      val bos = new BufferedOutputStream(conn.getOutputStream)
      bos.write(value.getBytes("UTF-8"))
      bos.close()

      // get respond
      status = conn.getResponseCode
      val respMsg = conn.getResponseMessage
      val stream = conn.getContent().asInstanceOf[InputStream]
      val br = new BufferedReader(new InputStreamReader(stream))
      val response = new java.lang.StringBuilder()
      var line = br.readLine()
      while (line != null) {
        response.append(line)
        line = br.readLine()
      }

      println(status, respMsg, response.toString)
      (status, respMsg, response.toString)
    } catch {
      case e: Exception =>
        e.printStackTrace()
        val err = "http request exception,load url : " + loadUrl + ",failed to execute spark streamload with label: " + label
        (status, e.getMessage, err)
    } finally {
      conn.disconnect()
    }
  }

  /**
   * 以JSON格式通过streamload进行导入
   *
   * @param rows 行数据
   * @return
   */
  def loadAsJson(rows: Seq[Seq[Any]]): (Int, String, String) = {
    val dataList = new java.util.ArrayList[java.util.Map[Any, Any]]()

    rows.foreach {
      row =>
        val dataMap = new java.util.HashMap[Any, Any]()
        if (columns.length != row.length) {
          throw new IllegalArgumentException(s"数据长度和列长度不一致: ${row.length}, ${columns.length}")
        }
        for (i <- row.indices) {
          dataMap.put(columns(i), row(i))
        }
        dataList.add(dataMap)
    }

    loadAsJson(new ObjectMapper().writeValueAsString(dataList))
    // todo: 判断是否成功
    // {    "TxnId": -1,    "Label": "order_company_query_algorithm_with_similarity_1665649192625",    "Status": "Fail",    "Message": "no valid Basic authorization",    "NumberTotalRows": 0,    "NumberLoadedRows": 0,    "NumberFilteredRows": 0,    "NumberUnselectedRows": 0,    "LoadBytes": 0,    "LoadTimeMs": 0,    "BeginTxnTimeMs": 0,    "StreamLoadPutTimeMs": 0,    "ReadDataTimeMs": 0,    "WriteDataTimeMs": 0,    "CommitAndPublishTimeMs": 0}
  }

}


object StreamLoader {
  /**
   * 根据用户名密码进行base64加密
   *
   * @param username 用户名
   * @param password 密码
   * @return 加密后的字符串, 前缀"Basic "字样
   */
  def basicAuthHeader(username: String, password: String): String = {
    val tobeEncode = username + ":" + password
    val encoded = Base64.encodeBase64(tobeEncode.getBytes(StandardCharsets.UTF_8))
    "Basic " + new String(encoded)
  }

  /**
   * 获取BE节点
   *
   * @param httpClient   HTTP Client
   * @param feHost       fe节点ip
   * @param httpPort     http服务端口
   * @param database     库名
   * @param tableName    表名
   * @param user AUTHORIZATION(用户名密码的BASE64编码)
   * @param password AUTHORIZATION(用户名密码的BASE64编码)
   * @return BE节点 stream load url
   */
  //noinspection HttpUrlsUsage
  def get_be_url(
                  httpClient: CloseableHttpClient,
                  feHost: String,
                  httpPort: Int,
                  database: String,
                  tableName: String,
                  user: String,
                  password: String
                ): String = {
    val authEncoding = basicAuthHeader(user, password)


    val api = s"http://$feHost:$httpPort/api/$database/$tableName/_stream_load"

    val httpPut = new HttpPut(api)
    val requestConfig = RequestConfig.custom()
      .setAuthenticationEnabled(true)
      .setRedirectsEnabled(false)
      .build()

    httpPut.setConfig(requestConfig)
    httpPut.setHeader(HttpHeaders.EXPECT, "100-continue") // .setExpectContinueEnabled(true)
    httpPut.setHeader(HttpHeaders.AUTHORIZATION, authEncoding) // Authorization: Basic cm9vdDo=

    // 发送一条空信息
    val content = new StringEntity("", Charset.forName("UTF-8"))
    content.setContentType("text/plain; charset=UTF-8")
    content.setContentEncoding("UTF-8")
    httpPut.setEntity(content)

    val response = httpClient.execute(httpPut)
    response.getAllHeaders.foreach(println)
    response.getHeaders("Location")
    val location = response.getFirstHeader("Location")
    val statusCode = response.getStatusLine.getStatusCode
    if (location == null || location.getValue.isEmpty) {
      if (statusCode == 401)
        throw new IllegalAccessException(s"无法找到BE节点, api: $api, user: $user")
      if (statusCode != 307)
        throw new IllegalAccessException(s"无法找到BE节点, api: ${api}, status: $statusCode")
    }

    location.getValue
  }
}
