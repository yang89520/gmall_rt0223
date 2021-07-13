package com.yiran.gmall.realtime.app

import com.alibaba.fastjson.{JSON, JSONObject}
import com.yiran.gmall.Constansts
import com.yiran.gmall.realtime.bean.DauInfo
import com.yiran.gmall.realtime.util.{MyEsUtil, MyKafkaUtil, MyRedisUtil, OffsetManagerUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis

import java.{lang, util}
import java.time.{Instant, LocalDateTime, ZoneId}
import java.time.format.DateTimeFormatter

/**
 * 日活业务
 *    功能 1：SparkStreaming 消费 kafka 数据
 *    功能 2：利用 redis 过滤当日已经计入的日活设备（对一个用户的多次访问进行去重）
 *    功能 3：把每批次新增的当日日活信息保存到 ES 中去
 *    功能 4：从 ES 中查询出数据，发布成数据接口，可视化工程进行调用
 *    优化：精准一次处理
 *     * 需要手动维护偏移量offset
       * 精准一次：
       *    先落盘【存到redis】，再提交偏移量
       *    方案一：幂等操作 + 手动提交 √
       *    方案二：事务，不好事务效率低
 */
object DAUApp {

  /**
   * 写入redis
   * @param jsonDStream
   * @return
   */
  def writeRedisWithSource(jsonDStream: DStream[JSONObject],mode:Boolean=false):DStream[JSONObject] = {
    var resDStream: DStream[JSONObject] = null
    if (mode==false) {
      //mapPartition的元素时迭代器
      resDStream = jsonDStream.mapPartitions(startJsonLogIter => {
        //获取redis连接
        val jedis: Jedis = MyRedisUtil.getJedisClient()

        //首次启动的数据保存到集合  因为要保存完整的一条数据，所以可以使用list
        //val firstStartLogMap = new util.HashMap[String, String]()
        val firstStartLogList = new util.ArrayList[JSONObject]()


        //向redis写数据
        startJsonLogIter.foreach(startLog => {
          val dt: String = startLog.getString("dt")
          val mid: String = startLog.getJSONObject("common").getString("mid")
          //用什么写入？ 写入什么？ sadd写入set保证数据写入的一致性  key：因为日活需求是以日期mid 为粒度，所以key为 日期 value：mid
          val dauKey: String = "dau:" + dt
          val isFrist: lang.Long = jedis.sadd(dauKey, mid)
          //设置数据的过期时间，因为redis是基于内存的数据库
          if (jedis.sismember(dauKey, mid))
            jedis.expire(dauKey, 3600 * 24)
          //判断是否是已经写入了第一条 写一个钩子，判断是否是第一次写入，即sadd的返回值是1  是的话将数据加入到首次启动的集合中
          if (isFrist == 1L)
            firstStartLogList.add(startLog)
        })
        //
        jedis.close()
        import scala.collection.JavaConverters._
        firstStartLogList.asScala.iterator
      })
    }
    else {
      //mapPartition的元素时迭代器
      resDStream = jsonDStream.mapPartitions(startJsonLogIter => {
        //获取redis连接
        val jedis: Jedis = MyRedisUtil.getJedisClient()

        //首次启动的数据保存到集合  因为要保存完整的一条数据，所以可以使用list
        //val firstStartLogMap = new util.HashMap[String, String]()
        val firstStartLogList = new util.ArrayList[JSONObject]()


        //向redis写数据
        startJsonLogIter.foreach(startLog => {
          val dt: String = startLog.getString("dt")
          val mid: String = startLog.getJSONObject("common").getString("mid")
          //用什么写入？ 写入什么？ sadd写入set保证数据写入的一致性  key：因为日活需求是以日期mid 为粒度，所以key为 日期 value：mid
          val dauKey: String = "dau:" + dt
          val isFrist: lang.Long = jedis.sadd(dauKey, mid)
          //设置数据的过期时间，因为redis是基于内存的数据库
          if (jedis.sismember(dauKey, mid))
            jedis.expire(dauKey, 3600 * 24)
          //判断是否是已经写入了第一条 写一个钩子，判断是否是第一次写入，即sadd的返回值是1  是的话将数据加入到首次启动的集合中
          if (isFrist == 1L)
            firstStartLogList.add(startLog)
        })
        //
        jedis.close()
        import scala.collection.JavaConverters._
        firstStartLogList.asScala.iterator
      })
      resDStream.foreachRDD(rdd=>{
        rdd.saveAsTextFile("tttt.josn")
      })
    }
    resDStream
  }

  def main(args: Array[String]): Unit = {
    //获取常量
    //日活只需要启动日志
    val topic = Constansts.GMALL_STARTUP_LOG
    val groupId = Constansts.GMALL_DAU_APP_GROUP
    //获取StreamingContest
    val sparkConf: SparkConf = new SparkConf().setAppName("DauApp").setMaster("local[*]").set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
    val ssc:StreamingContext = new StreamingContext(sparkConf, Seconds(5))

    var recordDStream: InputDStream[ConsumerRecord[String, String]] = null
    //利用redis幂等操作  对访客去重，计算日活
      //拿到offset
    val offsetMap: Map[TopicPartition, Long] = OffsetManagerUtil.getOffset(topic, groupId)

    /**
     * 在错误恢复后，
     *    如果offsetMap非空存在 从手动维度的偏移量起消费
     *    否则从最新位置开始消费
     */
    if (offsetMap!=null && offsetMap.size>0) {
      recordDStream = MyKafkaUtil.getKafkaStream(topic, ssc, offsetMap, groupId)
    }
    else {
      recordDStream = MyKafkaUtil.getKafkaStream(topic, ssc, groupId)
    }

    //获取当前采集周期从kafka中消费的数据的起始偏移量以及结束偏移量
    var offsetRanges: Array[OffsetRange] = Array.empty[OffsetRange]

    //拿到偏移量范围
    //因为recodeDStream底层封装的是KafkaRDD，混入了HasOffsetRanges特质，这个特质中提供了可以获取偏移量范围的方法
    val startLogDStream: DStream[ConsumerRecord[String, String]] = recordDStream.transform(rdd => {
      val offsetRanges: Array[OffsetRange] = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      offsetRanges.foreach(println)
      rdd
    })

    val jsonDStream: DStream[JSONObject] = startLogDStream.map {
      record => {
        val jsonStr: String = record.value()
        val jsonObject: JSONObject = JSON.parseObject(jsonStr)
        val ts: lang.Long = jsonObject.getLong("ts")
        //通过ts时间戳 封装 dt日期，hr小时
        val formatter: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd")
        val localDateTime: LocalDateTime = LocalDateTime.ofInstant(Instant.ofEpochMilli(ts), ZoneId.of("Asia/Shanghai"))
        var dt = localDateTime.format(formatter)
        var hr = localDateTime.getHour.toString
        println("dt:"+dt+" hr:"+hr)
        jsonObject.put("dt", dt)
        jsonObject.put("hr", hr)
        jsonObject
      }
    }
    // test 查看数据状况 是否插入了dt和hr
    //jsonDStream.print(100)



    var resDStream:DStream[JSONObject] = writeRedisWithSource(jsonDStream,false)
    //println("====================")  不能使用println 会阻塞线程
    //将去重的结果保存到Elasticsearh
    resDStream.foreachRDD(rdd=>{
      //foreachPartition针对分区建立Es连接
      rdd.foreachPartition(jsonIter=>{
        //需要导入到ES的数据迭代器
        val resIter: Iterator[(String, DauInfo)] = jsonIter.map(jsonObj => {
          val commonJsonObj: JSONObject = jsonObj.getJSONObject("common")
          //封装数据到样例类DauInfo
          val dauInfo = DauInfo(
            commonJsonObj.getString("mid"),
            commonJsonObj.getString("uid"),
            commonJsonObj.getString("ar"),
            commonJsonObj.getString("ch"),
            commonJsonObj.getString("vc"),
            jsonObj.getString("dt"),
            jsonObj.getString("hr"),
            "00",
            jsonObj.getLong("ts")
          )
          println(dauInfo)
          (dauInfo.mid, dauInfo)
        })

        val resList: List[(String, DauInfo)] = resIter.toList
        //id怎么构造保证唯一？ 因为dau是以天更新的，所以以日期为表名即可
        val formatter: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd")
        val nowTime: LocalDateTime = LocalDateTime.now()
        var dt = nowTime.format(formatter) //String
        //写入ES
        MyEsUtil.putIndex(resList,"gmall0223_dau_info_"+dt)
      })
      //保存偏移量
      OffsetManagerUtil.saveOffset(topic, groupId, offsetRanges)
    })
    ssc.start()
    ssc.awaitTermination()
  }
}
