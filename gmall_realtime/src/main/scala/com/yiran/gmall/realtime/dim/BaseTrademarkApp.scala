package com.yiran.gmall.realtime.dim

import com.alibaba.fastjson.JSON
import com.yiran.gmall.Constansts
import com.yiran.gmall.realtime.bean.BaseTrademark
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
  * Date: 2020/10/30
  * Desc: 从Kafka中读取品牌维度数据，保存到Hbase
  */
object BaseTrademarkApp {
  def main(args: Array[String]): Unit = {

    val sparkConf: SparkConf = new SparkConf().setMaster("local[4]").setAppName("BaseTrademarkApp")

    val ssc = new StreamingContext(sparkConf, Seconds(5))
    val topic = Constansts.ODS_BASE_TRADEMARK
    val groupId = Constansts.DIM_BASE_TRADEMARK_GROUP


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
    val inputGetOffsetDstream: DStream[ConsumerRecord[String, String]] = inputDstream.transform {
      rdd =>{
        offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        rdd
      }
    }

    val objectDstream: DStream[BaseTrademark] = inputGetOffsetDstream.map {
      record =>{
        val jsonStr: String = record.value()
        val obj: BaseTrademark = JSON.parseObject(jsonStr, classOf[BaseTrademark])
        obj
      }
    }
    //保存到Hbase
    objectDstream.foreachRDD{rdd=>
      rdd.saveToPhoenix("GMALL0223_BASE_TRADEMARK",Seq("ID", "TM_NAME"  )
        ,new Configuration,Some("hadoop102,hadoop103,hadoop104:2181"))
      OffsetManagerUtil.saveOffset(topic,groupId, offsetRanges)
    }

    ssc.start()
    ssc.awaitTermination()
  }

}
