package com.yiran.gmall.realtime.util

import org.apache.kafka.common.TopicPartition
import org.apache.spark.streaming.kafka010.OffsetRange
import redis.clients.jedis.Jedis

import java.util
import scala.collection.mutable
import scala.collection.mutable.ListBuffer


/**
 * redis维护偏移量工具：对offset的管理，读取和保存
	    1.读取：
		    需要在实时启动时从redis读取上一次结束后的offset，
        然后开始消费数据，只需读取一次即可
	    2.保存：
		    每消费一批次的数据，成功保存后，在手动保存消费过的offset
 */
object OffsetManagerUtil {

  /**
   * 查询偏移量的方法
   * @param topic
   * @param groupId
   * @return Map[TopicPartition, Long] offset对象
   */
  def getOffset(topic:String,groupId:String)= {
    // 一个redis连接
    val jedis: Jedis = MyRedisUtil.getJedisClient()
    // key
    var offsetKey = "offset:" + topic + ":" + groupId
    // 返回哈希表中，所有的字段和值
    val offsetMap: util.Map[String, String] = jedis.hgetAll(offsetKey)
    //关闭客户端
    jedis.close()

    //转为scala处理为saprkStreaming kafka消费者需要的offset参数格式 传入MyKafkaUtil中
    import scala.collection.JavaConverters._
    //asScala隐式转换提供的方法java Map--> Scala Map
    val oMap: mutable.Map[TopicPartition, Long] = offsetMap.asScala.map {
      case (partition, offset) => {
        println("读取分区偏移量：" + partition + ":" + offset)
        //转为 Map[TopicPartition,Long]
        (new TopicPartition(topic, partition.toInt), offset.toLong)
      }
    }
    //隐式转换提供的方法 可变Map转不可变Map
    oMap.toMap
  }

  /**
   * 存储偏移量的方法
   * @param topic
   * @param groupId
   * @param offsetRanges
   * @return
   */
  def saveOffset(topic:String,groupId:String,offsetRanges:Array[OffsetRange])={
    //拼接redis中操作偏移量的key
    var offsetKey = "offset:" + topic + ":" + groupId
    //定义java的map集合，用于存放每个分区对应的偏移量
    val offsetValue = new util.HashMap[String, String]()
    offsetRanges.foreach(offsetRange=>{
      val partitionId: Int = offsetRange.partition //分区
      val fromOffset: Long = offsetRange.fromOffset //起始偏移量
      val untilOffset: Long = offsetRange.untilOffset //结束偏移量
      offsetValue.put(partitionId.toString,untilOffset.toString)
      println("保存分区" + partitionId + ":" + fromOffset + "----->" + untilOffset)
    })
    val jedis: Jedis = MyRedisUtil.getJedisClient()
    //存到hash表中，天然去重
    if (offsetValue!=null && offsetValue.size>0) {
      jedis.hmset(offsetKey,offsetValue)
    }
    jedis.close()
  }

  "offset在redis中的存储格式："
  /*
        key                                  		value(hash)
    "offset:groupid:topic"             		field           value
                                          partiton_id     offset
                                          partiton_id     offset
                                          ...
  */
  def saveOffset(topic:String,groupId:String,offsetRanges:ListBuffer[OffsetRange]) = {
    //1.获取redis客户端
    val jedis: Jedis = MyRedisUtil.getJedisClient()
    "获取key值"
    //2.将主函数传过来的groupid和topicp拼接得到key
    val key = s"offset:${groupId}:${topic}"
    //3.将存储偏移量的集合解析
    import scala.collection.JavaConverters._
    val fieldAndvalue: util.Map[String, String] = offsetRanges
      .map(offsetRange => {
        "获取value值"
        //4.解析出集合中的分区和偏移量组成一个元组，作为value
        offsetRange.partition.toString -> offsetRange.untilOffset.toString
      })
      //5.转成一个map集合，里面是元组，可以转
      .toMap
      .asJava

    //6.将key和value存入redis  ————> redis端用的是java的map，这里需要使用隐式转换将scala中的map转成java中的map
    jedis.hmset(key,fieldAndvalue)

    println("保存偏移量 topic_partition-> offset: " + fieldAndvalue)
    //7.关闭redis客户端
    jedis.close()
  }


  def getOffsets(groupId: String, topic: String) = {
    "key值"
    //1.获取需要读取哪个分区哪个主题的偏移量
    val key = s"offset:${groupId}${topic}"

    //2.获取redis客户端
    val jedis = MyRedisUtil.getJedisClient()

    println("读取开始的offset")
    import scala.collection.JavaConverters._
    val topicPartitionAndOffset: Map[TopicPartition, Long] = jedis
      //"初始偏移量"
      //3.获取该分区的这个主题的所有数据（包含数据和偏移量）
      .hgetAll(key)

      //4.将得到的java的Map集合转成scala的Map集合 ————>存到时候存到是一个hashmap，读的时候也是读一个hashmap，得到key对应的value，是一个java的map
      .asScala
      .map {
        case (partition, offset) =>
          //5.将key转化成topicpartition，value转化成long类型  ————>得到的是一个scala的可变的Map集合
          new TopicPartition(topic, partition.toInt) -> offset.toLong
      }
      //6.将获取的偏移量转成不可变的Map集合  ————>因为kafka接收时用的是一个不可变的Map接收的
      .toMap
    //打印出初始偏移量
    println("初始偏移量：" + topicPartitionAndOffset)

    //7.关闭redis客户端
    jedis.close()
    //8.返回这个流数据 ————>有日志和偏移量的数据
    topicPartitionAndOffset
  }

  def main(args: Array[String]): Unit = {
    getOffset("GMALL_STARTUP_LOG1","gmall_dau_app")
  }
}
