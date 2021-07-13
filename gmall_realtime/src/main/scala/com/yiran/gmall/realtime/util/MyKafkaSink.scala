package com.yiran.gmall.realtime.util

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

import java.util.Properties

/**
 * 向Kafka主题中发送数据
 *
 * kafka生产者::分层数据写回kafka
 * send 两个重载
 *    topic msg
 *    topic key msg
 */
object MyKafkaSink {
  private val properties: Properties = MypropertiesUtil.load("config.properties")
  val broker_list = properties.getProperty("kafka.broker.list")

  private var kafkaProducer:KafkaProducer[String,String] = _

  /**
   * 创建KafkaProducer
   * @return KafkaProducer对象
   */
  def createKafkaProducer():KafkaProducer[String,String]={
    val properties = new Properties
    properties.put("bootstrap.servers", broker_list)
    properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    properties.put("enable.idempotence",(true: java.lang.Boolean))
    var producer:KafkaProducer[String,String]= null
    try
      producer=new KafkaProducer[String,String](properties)
    catch {
      case e: Exception => e.printStackTrace()
    }
    producer
  }

  /**
   * topic value
   * @param topic
   * @param msg
   * @return
   */
  def send(topic:String,msg:String)={
    if (kafkaProducer==null)
      kafkaProducer = createKafkaProducer()
    if (kafkaProducer!=null)
      kafkaProducer.send(new ProducerRecord[String,String](topic,msg))
  }

  /**
   * topic key value
   * @param topic
   * @param key
   * @param msg
   */
  def send(topic: String,key:String, msg: String): Unit = {
    if (kafkaProducer==null)
      kafkaProducer = createKafkaProducer()
    if (kafkaProducer!=null)
    kafkaProducer.send(new ProducerRecord[String, String](topic,key, msg))
  }
}
