package com.atguigu.gmall.util


import java.util.Objects

import com.alibaba.fastjson.JSON
import com.atguigu.gmall.constant.GmallConstants
import com.google.gson.GsonBuilder
import io.searchbox.client.config.HttpClientConfig
import io.searchbox.client.{JestClient, JestClientFactory}
import io.searchbox.core.{Bulk, BulkResult, Index}


object MyEsUtil {
  private val ES_HOST = "http://hadoop102"
  private val ES_HTTP_PORT = 9200
  private var factory: JestClientFactory = null

  /**
    * 获取客户端
    *
    * @return jestclient
    */
  def getClient: JestClient = {
    if (factory == null) build()
    factory.getObject
  }

  /**
    * 关闭客户端
    */
  def close(client: JestClient): Unit = {
    if (!Objects.isNull(client)) try
      client.shutdownClient()
    catch {
      case e: Exception =>
        e.printStackTrace()
    }
  }

  /**
    * 建立连接
    */
  private def build(): Unit = {
    factory = new JestClientFactory
    factory.setHttpClientConfig(new HttpClientConfig.Builder(ES_HOST + ":" + ES_HTTP_PORT).multiThreaded(true)
      .maxTotalConnection(20) //连接总数
      .connTimeout(10000).readTimeout(10000).build)

  }

  def main(args: Array[String]): Unit = {
//    val client: JestClient = getClient
//    val json = "{\n  \"name\" : \"love\",\n  \"age\" : 12\n}"
//    val index = new Index.Builder(json).index("gmall_test").`type`("_doc").build()
//    client.execute(index)
    val str = "{\"area\":\"beijin\",\"uid\":\"41\",\"itemid\":27,\"npgid\":36,\"evid\":\"addFavor\",\"os\":\"andriod\",\"pgid\":14,\"appid\":\"gmall12138\",\"mid\":\"mid_418\",\"type\":\"event\"}"

    indexBulkTO(GmallConstants.ES_INDEX_DAU,str)
  }

  def indexBulk(indexName:String, list: List[Any]): Unit ={
    val client: JestClient = getClient
    val builder = new Bulk.Builder().defaultIndex(indexName).defaultType("_doc")
    for (doc <- list) {
      println(doc)
      val index = new Index.Builder(doc).build()
      builder.addAction(index)
    }
    val items = client.execute(builder.build()).getItems
    println("保存了："+ items.size() + "条")
    close(client)
  }

  def indexBulkTO(indexName:String, oneTask : String): Unit ={
    val client: JestClient = getClient
    val builder = new Bulk.Builder().defaultIndex(indexName).defaultType("_doc")
    val index = new Index.Builder(oneTask).build()
    builder.addAction(index)
    val items = client.execute(builder.build()).getItems
    println("保存了："+ items.size() + "条")
    close(client)
  }
}
 
 
