package com.yiran.gmall.realtime.dim

import com.alibaba.fastjson.JSON
import com.yiran.gmall.Constansts
import com.yiran.gmall.realtime.bean.BaseCategory3
import com.yiran.gmall.realtime.util.{MyKafkaUtil, OffsetManagerUtil}
import org.apache.hadoop.conf.Configuration
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.phoenix.spark.toProductRDDFunctions
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Author: Felix
  * Desc: 读取商品分类维度数据到Hbase
  */
object BaseCategory3App {

  def main(args: Array[String]): Unit = {

    val sparkConf: SparkConf = new SparkConf().setMaster("local[4]").setAppName("BaseCategory3App")

    val ssc = new StreamingContext(sparkConf, Seconds(5))
    val topic = Constansts.ODS_BASE_CATEGORY3
    val groupId = Constansts.DIM_BASE_CATEGORY3_GROUP


    /////////////////////  偏移量处理///////////////////////////
    val offset: Map[TopicPartition, Long] = OffsetManagerUtil.getOffset(topic, groupId)

    var inputDstream: InputDStream[ConsumerRecord[String, String]] = null
    // 判断如果从redis中读取当前最新偏移量 则用该偏移量加载kafka中的数据  否则直接用kafka读出默认最新的数据
    if (offset != null && offset.size > 0) {
      inputDstream = MyKafkaUtil.getKafkaStream(topic, ssc, offset, groupId)
    } else {
      inputDstream = MyKafkaUtil.getKafkaStream(topic, ssc, groupId)
    }

    //取得偏移量步长
    var offsetRanges: Array[OffsetRange] = null
    val inputGetOffsetDstream: DStream[ConsumerRecord[String, String]] = inputDstream.transform { rdd =>
      offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      rdd
    }

    //转换结构
    val objectDstream: DStream[BaseCategory3] = inputGetOffsetDstream.map {
      record =>{
        val jsonStr: String = record.value()
        val obj: BaseCategory3 = JSON.parseObject(jsonStr, classOf[BaseCategory3])
        obj
      }
    }

    //保存到Hbase
    objectDstream.foreachRDD{
      rdd=>{
        rdd.saveToPhoenix("GMALL0223_BASE_CATEGORY3",
          Seq("ID", "NAME", "CATEGORY2_ID" )
          ,new Configuration,Some("hadoop102,hadoop103,hadoop104:2181"))
        OffsetManagerUtil.saveOffset(topic,groupId, offsetRanges)
      }
    }

    ssc.start()
    ssc.awaitTermination()

  }
}

