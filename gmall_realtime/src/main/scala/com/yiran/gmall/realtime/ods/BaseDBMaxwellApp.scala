package com.yiran.gmall.realtime.ods

import com.alibaba.fastjson.{JSON, JSONArray, JSONObject}
import com.yiran.gmall.Constansts
import com.yiran.gmall.realtime.util.{MyKafkaSink, MyKafkaUtil, MyRedisUtil, OffsetManagerUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import redis.clients.jedis.Jedis

object BaseDBMaxwellApp {
  def main(args: Array[String]): Unit = {
    //spark相关连接对象
    //topic groupId
    val conf: SparkConf = new SparkConf().setAppName("canalApp").setMaster("local[*]")
    val ssc = new StreamingContext(conf, Seconds(5))
    val topic: String = Constansts.GMALL_DB_MAXWELL
    val groupId: String = Constansts.GMALL_DB_MAXWELL_GROUP

    //redis读取偏移量
    val jedis: Jedis = MyRedisUtil.getJedisClient()
    val offsetMap: Map[TopicPartition, Long] = OffsetManagerUtil.getOffset(topic, groupId)

    //通过偏移量判断使用何种方式生成recordDStream消费数据
    var recordDStream: InputDStream[ConsumerRecord[String, String]] = null;
    if (offsetMap!=null && offsetMap.size>0){
      recordDStream = MyKafkaUtil.getKafkaStream(topic, ssc, offsetMap, groupId)
    }else{
      recordDStream = MyKafkaUtil.getKafkaStream(topic, ssc, groupId)
    }

    //从kafka中读取当前批次的偏移量
    var offsetRanges: Array[OffsetRange] = null
    val offsetDStream: DStream[ConsumerRecord[String, String]] = recordDStream.transform(rdd => {
      offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      rdd
    })

    //将offsetDStream中的数据转为JSONObject
    val jsonDStream: DStream[JSONObject] = offsetDStream.map(record => {
      val jsonStr: String = record.value()
      val jsonObject: JSONObject = JSON.parseObject(jsonStr)
      jsonObject
    })

    //分流，获取jsonObjDStream里的“table”拿到表名，“data”拿到数据，“type”拿到数据操作类型
    //通过ods_+table构建不同主题从而分流
    jsonDStream.foreachRDD(rdd=>{
      rdd.foreach(jsonObj=>{
        val opType: String = jsonObj.getString("type")
        //判断是否是insert插入
        if ("INSERT".equals(opType)){
          //获取表名
          val tableName: String = jsonObj.getString("table")
          //获取数据值，是个json数组
          val dataArray: JSONArray = jsonObj.getJSONArray("data")
          var sendTableName:String = "ods_"+tableName
          if (dataArray != null && dataArray.size()>0) {
            import scala.collection.JavaConverters._
            //将json数据遍历发到kafka里面
            //todo：没有对这部分数据做精准一次处理
            for (dataJson <- dataArray.asScala) {
              MyKafkaSink.send(sendTableName, dataJson.toString)
            }
          }
        }
      })
      //提交偏移量
      OffsetManagerUtil.saveOffset(topic,groupId,offsetRanges);
    })
  }
}
