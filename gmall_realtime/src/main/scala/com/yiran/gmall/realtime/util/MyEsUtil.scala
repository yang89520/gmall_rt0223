package com.yiran.gmall.realtime.util

import com.yiran.gmall.realtime.bean.DauInfo
import io.searchbox.client.config.HttpClientConfig
import io.searchbox.client.{JestClient, JestClientFactory}
import io.searchbox.core._
import org.elasticsearch.index.query.{BoolQueryBuilder, MatchQueryBuilder, TermQueryBuilder}
import org.elasticsearch.search.builder.SearchSourceBuilder
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder
import org.elasticsearch.search.sort.SortOrder
import org.junit.Test

import java.util

object MyEsUtil {


  //提供Jest客户端工厂
  private var jestClientFactory: JestClientFactory = null

  //提供获取Jest客户端方法
  def getJestClient():JestClient = {
    if (jestClientFactory==null)
      build()
    jestClientFactory.getObject
  }
  def build():Unit= {
    jestClientFactory = new JestClientFactory
    jestClientFactory.setHttpClientConfig(new HttpClientConfig.Builder("http://hadoop103:9200")
      .multiThreaded(true)
      .maxTotalConnection(20)
      .connTimeout(10000)
      .readTimeout(1000).build())
  }

  /**
   * 向ES中插入单条数据--------将插入文档的数组与json的形式直接传递
   */
  def putIndex1():Unit={
    val client: JestClient = getJestClient()
    var source:String =
      """
        |{
        |   "id":200,
        |   "name":"operation meigong river",
        |   "doubanScore":8.0,
        |   "actorList":[
        |     {"id":3,"name":"zhang han yu"}
        |   ]
        |}
        |""".stripMargin
    val index: Index = new Index.Builder(source)
      .index("movie_index_5")
      .`type`("movie")
      .id("1")
      .build()
    client.execute(index)
    client.close()
  }

  /**
   * 向ES中插入单条数据--------样例类Movie封装ES数据
   */
  def putIndex2():Unit={
    val jestClient: JestClient = getJestClient()
    val actorList: util.ArrayList[util.Map[String, Any]] = new util.ArrayList[util.Map[String, Any]]()
    val actorMap: util.HashMap[String, Any] = new util.HashMap[String, Any]()
    actorMap.put("id",66)
    actorMap.put("name","李若彤")
    actorList.add(actorMap)

    val movie: Movie = Movie(300, "天龙八部", 9.0f, actorList)
    val index: Index = new Index.Builder(movie)
      .index("movie_index_5")
      .`type`("movie")
      .id("2").build()
    jestClient.execute(index)
    jestClient.close()
  }

  /**
   * 根据文档的id，从ES中查询出一条记录
   * @param args
   */
  def queryIndexById():Unit={
      val jestClient: JestClient = getJestClient()
      val get: Get = new Get.Builder("movie_index_5", "2").build()
      val result: DocumentResult = jestClient.execute(get)
      println(result.getJsonString)
      jestClient.close()
    }

  /**
   * 根据指定查询条件，从ES中查询多个文档  方式1
   * SearchResult对象.getHits.asScala.map(_.source).toList
   */
  def queryIndexByCondition1(): Unit ={
    val jestClient = getJestClient()
    var query:String =
      """
        |{
        |  "query": {
        |    "bool": {
        |       "must": [
        |        {"match": {
        |          "name": "天龙"
        |        }}
        |      ],
        |      "filter": [
        |        {"term": { "actorList.name.keyword": "李若彤"}}
        |      ]
        |    }
        |  },
        |  "from": 0,
        |  "size": 20,
        |  "sort": [
        |    {
        |      "doubanScore": {
        |        "order": "desc"
        |      }
        |    }
        |  ],
        |  "highlight": {
        |    "fields": {
        |      "name": {}
        |    }
        |  }
        |}
      """.stripMargin
    //封装Search对象
    val search: Search = new Search.Builder(query)
      .addIndex("movie_index_5").build()
    val result: SearchResult = jestClient.execute(search)
    //数据都封装在Hits里面  转为json string
    val resList: util.List[SearchResult#Hit[util.Map[String, Any], Void]] = result.getHits(classOf[util.Map[String, Any]])
    import scala.collection.JavaConverters._
    val resList1: List[util.Map[String, Any]] = resList.asScala.map(_.source).toList
    println(resList1.mkString("\n"))
    jestClient.close()
  }

  /**
   * 根据指定查询条件，从ES中查询多个文档  方式2
   */
  def queryIndexByCondition2(): Unit = {
    val jestClient = getJestClient()
    //SearchSourceBuilder用于构建查询的json格式字符串
    val searchSourceBuilder = new SearchSourceBuilder()
    val boolQueryBuilder = new BoolQueryBuilder()
    boolQueryBuilder.must(new MatchQueryBuilder("name","天龙"))
    boolQueryBuilder.filter(new TermQueryBuilder("actorList.name.keyword","李若彤"))
    searchSourceBuilder.query(boolQueryBuilder)
    searchSourceBuilder.from(0)
    searchSourceBuilder.size(10)
    searchSourceBuilder.sort("doubanScore",SortOrder.ASC)
    searchSourceBuilder.highlighter(new HighlightBuilder().field("name"))
    val query: String = searchSourceBuilder.toString
    //println(query)

    val search: Search = new Search.Builder(query).addIndex("movie_index_5").build()
    val res: SearchResult = jestClient.execute(search)
    val resList: util.List[SearchResult#Hit[util.Map[String, Any], Void]] = res.getHits(classOf[util.Map[String,Any]])

    import scala.collection.JavaConverters._
    val list = resList.asScala.map(_.source).toList
    println(list.mkString("\n"))

    jestClient.close()
  }




  def main(args: Array[String]): Unit = {
    //putIndex1()
    putIndex2()
    queryIndexById()
  }


  //#############################以上是Es的练习测试方法

  /**
   * 多条数据插入ES
   *    使用bulk缓冲区逐条存放index
   *    然后调用build() 返回 bulk对象
   *    jestClient.execute(bulk)执行
   * @param sourceIter
   */
  def putIndex(sourceList:List[(String,Any)],indexName:String): Unit = {

    if (sourceList != null && sourceList.size>0){
      //创建客户端对象
      val jestClient: JestClient = getJestClient()
      //初始化bulk构造者对象  用于存放批 index 插入的数据
      val bulkBuilder: Bulk.Builder = new Bulk.Builder
      //不能用foreach？？因为foreach 会把List里的tuple当做一个元素
      for ((id,dauInfo) <- sourceList) {
        val index: Index = new Index.Builder(dauInfo)
          .index(indexName)
          //手动生成id
          .id(id)
          .`type`("_doc")
          .build()
        bulkBuilder.addAction(index) //逐条放到bulk缓冲区
      }
      //build 创建bulk对象
      val bulk: Bulk = bulkBuilder.build()
      val bulkResult: BulkResult = jestClient.execute(bulk)
      println("向ES中插入"+bulkResult.getItems.size()+"条数据")
      jestClient.close()
    }
  }
}
case class Movie(id:Long,name:String,doubanScore:Float,actorList:java.util.List[java.util.Map[String,Any]]){}