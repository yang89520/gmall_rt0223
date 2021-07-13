package com.yiran.gmall.realtime.util

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}

import java.util.Properties

/**
 * kafka消费者工具类
 * getKafkaStream 三个重载方法
 *    创建DStream，使用默认消费者组，返回接收数据
 *    指定消费者组
 *    指定偏移量消费
 */
object MyKafkaUtil {

  //读取外部kafka配置
  private val prop: Properties = MypropertiesUtil.load("config.properties")
  var broker_list:String = prop.getProperty("kafka.broker.list")
  //kafka连接参数设置
  var kafkaParam = collection.mutable.Map(
    "bootstrap.servers" -> broker_list,
    "key.deserializer" -> classOf[StringDeserializer],
    "value.deserializer" -> classOf[StringDeserializer],
    //用于标识这个消费者属于哪个消费者组
    "group.id" -> "gmall0223_group",
    //latest自动重置偏移量为最新的偏移量
    "auto.offset.reset" -> "latest",
    //如果是true，则这个消费者的偏移量会在后台自动提交,但是kafka宕机容易丢失数据
    //如果是false，会需要手动维护kafka偏移量
    "enable.auto.commit" -> (false: java.lang.Boolean)
  )

  /**
   * 创建DStream，使用默认消费者组，返回接收数据
   */
  def getKafkaStream(topic:String,ssc:StreamingContext):InputDStream[ConsumerRecord[String, String]]={
    val dStream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream(ssc
      , LocationStrategies.PreferConsistent
      , ConsumerStrategies.Subscribe[String,String](Array(topic), kafkaParam)
    )
    dStream
  }

  /**
   * 指定消费者组
   */
  def getKafkaStream(topic:String,ssc:StreamingContext,groupId:String)={
    //scala map的两种修改元素写法
    kafkaParam.put("group.id",groupId)
    //kafkaParam("group.id")=groupId
    println(kafkaParam.get("group.id"))
    val dStream: InputDStream[ConsumerRecord[String,String]] = KafkaUtils.createDirectStream(ssc
      , LocationStrategies.PreferConsistent
      , ConsumerStrategies.Subscribe[String,String](Array(topic), kafkaParam)
    )
    dStream
  }

  /**
   * 版本1：指定偏移量消费
   */
    /*
  def getKafkaStream(topic:String, ssc:StreamingContext, offset:Map[TopicPartition,Long], groupId:String)={
    //scala map的两种修改元素写法
    kafkaParam.put("group.id",groupId)
    //kafkaParam("group.id")=groupId
    println(kafkaParam.get("group.id"))
    val dStream: InputDStream[ConsumerRecord[String,String]] = KafkaUtils.createDirectStream(ssc
      , LocationStrategies.PreferConsistent
      , ConsumerStrategies.Subscribe[String,String](Array(topic), kafkaParam,offset)
    )
    dStream
  }
  */

  /**
   * 版本2：改进指定偏移量消费的方法
   *    即没有读取到偏移量从头开始消费earliest
   *    此方法返回的的是kafka中读取出来的流
   */
  def getKafkaStream(topic:String, ssc:StreamingContext, offset:Map[TopicPartition,Long], groupId:String)={
    kafkaParam += "group.id" -> groupId
    kafkaParam += "auto.offset.reset" -> "earliest"
    kafkaParam += "enable.auto.commit" -> (false:java.lang.Boolean) //需要手动维护offsets
    KafkaUtils
      .createDirectStream[String, String](
        ssc,
        LocationStrategies.PreferConsistent,
        Subscribe[String, String](Set(topic), kafkaParam,offset)
      )
    //.map(_.value()) 只有从kafka直接得到的流才有offset的信息，map之后就没了
  }

}
