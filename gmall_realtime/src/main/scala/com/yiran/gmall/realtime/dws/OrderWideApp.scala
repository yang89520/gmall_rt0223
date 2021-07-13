package com.yiran.gmall.realtime.dws

import com.alibaba.fastjson.JSON
import com.alibaba.fastjson.serializer.SerializeConfig
import com.yiran.gmall.Constansts
import com.yiran.gmall.realtime.bean.{OrderDetail, OrderInfo, OrderWide}
import com.yiran.gmall.realtime.util.{MyKafkaSink, MyKafkaUtil, MyRedisUtil, OffsetManagerUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis

import java.lang
import java.util.Properties
import scala.collection.mutable.ListBuffer

/**
 * 从kafka的dwd_order_detail读取数据
 */
object OrderWideApp {



  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("orderWideApp").setMaster("local[*]")
    val ssc = new StreamingContext(conf, Seconds(5))
    /*1 获取样例类对象流*/
    val orderDetailWithKeyVDStream: (DStream[OrderDetail],Array[OffsetRange])  = getObjDStream[OrderDetail](Constansts.DWD_ORDER_DETAIL, Constansts.DWD_ORDER_DETAIL_GROUP, ssc)
    val orderInfoWithKeyVDStream: (DStream[OrderInfo],Array[OffsetRange]) = getObjDStream[OrderInfo](Constansts.DWD_ORDER_INFO, Constansts.DWD_ORDER_INFO_GROUP, ssc)
    val orderDetailWithKeyDStream: DStream[OrderDetail] = orderDetailWithKeyVDStream._1
    val orderInfoWithKeyDStream: DStream[OrderInfo] = orderInfoWithKeyVDStream._1
    val offsetRangeDetail: Array[OffsetRange] = orderDetailWithKeyVDStream._2
    val offsetRangeInfo: Array[OffsetRange] = orderInfoWithKeyVDStream._2
    /*2 双流join orderDetailDStream join orderInfoDStream*/
    //1 开窗################
    val orderInfoWindowDStream: DStream[OrderInfo] = orderInfoWithKeyDStream.window(Seconds(20), Seconds(5))
    val orderDetailWindowDStream: DStream[OrderDetail] = orderDetailWithKeyDStream.window(Seconds(20), Seconds(5))
    //2 转为kv##############
    //通过Key join order_info id  order_detail order_id
    val orderInfoWithIdDStream: DStream[(Long, OrderInfo)] = orderInfoWindowDStream.map {
      case orderInfo => (orderInfo.id, orderInfo)
    }
    val orderDetailWithIdDStream: DStream[(Long, OrderDetail)] = orderDetailWindowDStream.map {
      case orderDetail => (orderDetail.order_id, orderDetail)
    }
    //3 双流join###############
    //使用join内连接保证留下的数据都是非null的
    val orderWithJoinDStream: DStream[(Long, (OrderInfo, OrderDetail))] = orderInfoWithIdDStream.join(orderDetailWithIdDStream)

    //4 对join后的窗口流去重 使用redis 的set天然去重
    val orderWideDStream: DStream[OrderWide] = orderWithJoinDStream.mapPartitions(
      orderIter => {
        val jedis: Jedis = MyRedisUtil.getJedisClient()
        val OrderList: List[(Long, (OrderInfo, OrderDetail))] = orderIter.toList
        //定义一个集合存放去重后的结果
        val resBuffer: ListBuffer[OrderWide] = new ListBuffer[OrderWide]
        //for (orderJoin:(Long, (OrderInfo, OrderDetail)) <- OrderList) {
        for ((orderId, (orderInfo, orderDetail)) <- OrderList) {
          val orderKey: String = "order_join" + orderId
          //保存order_id 和 order_detail的 id
          val response: lang.Long = jedis.sadd(orderKey, orderDetail.id.toString)
          //首次插入
          if (response == 1L) {
            resBuffer.append(new OrderWide(orderInfo, orderDetail))
          }
        }
        jedis.close()
        resBuffer.toIterator
      }
    )
    //porderWithDStream.print(10)

    /*3 实付分摊*/
    /**
     * 思路：
     *    使用缓存redis
     *    获取明细累加
     *    累加明细写回
     *    判断是否是最后一条 计算实时分摊 写回
     *
     */

    val orderWideSplitDStream: DStream[OrderWide] = orderWideDStream.mapPartitions( //DStream[OrderWide]
      orderWideIter => {
        //redis client
        val jedis: Jedis = MyRedisUtil.getJedisClient()
        val orderWideList: List[OrderWide] = orderWideIter.toList
        for (orderWide: OrderWide <- orderWideList) {
          //缓存累计值
          var orderOriginSum: Double = 0D //明细
          var orderSplitSum: Double = 0D //实付分摊

          //计算当条明细金额：数量*单价
          val detailAmount: Double = orderWide.sku_price * orderWide.sku_price

          //#################1 查询明细累积################
          //构建key
          var orderOriginSumKey = "order_origin_sum:" + orderWide.order_id

          val orderOriginSumStr: String = jedis.get(orderOriginSumKey)
          //注意：从Redis中获取字符串，都要做非空判断
          if (orderOriginSumStr != null && orderOriginSumStr.size > 0) {
            orderOriginSum = orderOriginSumStr.toDouble
          }

          //#################2 查询实付分摊累积###############
          var orderSplitSumKey = "order_split_sum:" + orderWide.order_id

          val orderSplitSumStr: String = jedis.get(orderOriginSumKey)
          //注意：从Redis中获取字符串，都要做非空判断
          if (orderSplitSumStr != null && orderSplitSumStr.size > 0) {
            orderSplitSum = orderSplitSumStr.toDouble
          }

          //#################3 判断是否是这个订单的最后一条 计算实时分摊金额################
          //最后一条了：原始总金额可以 - 当前累积总金额 等于当前明细算出来的当条明细金额
          if (detailAmount == orderWide.original_total_amount - orderOriginSum) {
            //业务：实付分摊金额 = 实付总金额 * (数量 * 单价) / 原始总金额
            orderWide.final_detail_amount = Math.round((orderWide.final_total_amount - orderSplitSum) * 100d) / 100d
          }
          //非最后一条
          else {
            //业务：实付分摊金额 = 实付总金额 - 其他明细已计算好的 【实付分摊金额】合计
            orderWide.final_detail_amount = Math.round((orderWide.final_total_amount * detailAmount / orderWide.original_total_amount) * 100) / 100d
          }

          //#################4 分摊结果写回###############
          //原子操作 设置过期时间，用于未来数据延迟，还能读取缓存更新数据
          jedis.setex(orderOriginSumStr, 600, (orderOriginSum + detailAmount).toString)
          //关闭连接
          jedis.setex(orderSplitSumKey, 600, (orderSplitSum + orderWide.final_detail_amount).toString)
        }
        jedis.close()
        orderWideList.toIterator
      }
    )
    orderWideSplitDStream.print(1000)
    /**
     * 注意：
     *    转为sparksql和DStream的print操作都会消费掉数据，导致之后的数据再消费没数据，可以使用缓存cache
     */
    orderWideSplitDStream.cache()
    println("---------------------------------")
    orderWideSplitDStream.print(1000)

    val spark: SparkSession = SparkSession.builder().appName("spark_sql_orderWide").getOrCreate()
    import spark.implicits._
    orderWideSplitDStream.foreachRDD(
      rdd=>{
        val df: DataFrame = rdd.toDF()
        //输出到clickhouse
        df.write.mode(SaveMode.Append)
          .option("batchsize", "100")
          .option("isolationLevel", "NONE") // 设置事务
          .option("numPartitions", "4") // 设置并发
          .option("driver","ru.yandex.clickhouse.ClickHouseDriver")
          .jdbc("jdbc:clickhouse://hadoop202:8123/default","t_order_wide_0523",new Properties())

        //将数据写回到Kafka dws_order_wide
        rdd.foreach{
          orderWide=>{
            MyKafkaSink.send("dws_order_wide",JSON.toJSONString(orderWide,new SerializeConfig(true)))
          }
        }

        //提交偏移量
        OffsetManagerUtil.saveOffset(Constansts.DWD_ORDER_INFO,Constansts.DWD_ORDER_INFO_GROUP,offsetRangeInfo)
        OffsetManagerUtil.saveOffset(Constansts.DWD_ORDER_DETAIL,Constansts.DWD_ORDER_DETAIL_GROUP,offsetRangeDetail)
      }
    )
  }

  def getObjDStream[T](topic: String, groupId: String, ssc: StreamingContext): (DStream[T],Array[OffsetRange]) = {

    //获取偏移量
    val offsetMap: Map[TopicPartition, Long] = OffsetManagerUtil.getOffset(topic,groupId)


    var orderInfoRecordDStream: InputDStream[ConsumerRecord[String, String]] = null
    if(offsetMap!= null && offsetMap.size > 0){
      orderInfoRecordDStream = MyKafkaUtil.getKafkaStream(topic,ssc,offsetMap,groupId)
    }else{
      orderInfoRecordDStream = MyKafkaUtil.getKafkaStream(topic,ssc,groupId)
    }

    var offsetRanges: Array[OffsetRange] = Array.empty[OffsetRange]
    val offsertDStream: DStream[ConsumerRecord[String, String]] = orderInfoRecordDStream.transform {
      rdd => {
        offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        rdd
      }
    }

    val resDStream: DStream[T] = offsertDStream.map {
      record => {
        val orderDetailStr: String = record.value()
        val orderDetail: T = JSON.parseObject(orderDetailStr, classOf[T])
        orderDetail
      }
    }
    (resDStream,offsetRanges)
  }
}
