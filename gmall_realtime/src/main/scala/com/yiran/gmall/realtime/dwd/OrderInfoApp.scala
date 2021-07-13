package com.yiran.gmall.realtime.dwd

import com.alibaba.fastjson.serializer.SerializeConfig
import com.alibaba.fastjson.{JSON, JSONArray, JSONObject}
import com.yiran.gmall.Constansts
import com.yiran.gmall.realtime.bean.{OrderInfo, ProvinceInfo, UserInfo}
import com.yiran.gmall.realtime.util.{MyEsUtil, MyKafkaSink, MyKafkaUtil, MyRedisUtil, OffsetManagerUtil, PhoenixUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.hadoop.conf.Configuration
import redis.clients.jedis.Jedis

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

/**
 * dwd层 从kafka中拿到数据进行处理
 */
object OrderInfoApp {
  def main(args: Array[String]): Unit = {
    //spark相关连接对象
    //topic groupId
    val conf: SparkConf = new SparkConf().setAppName("canalApp").setMaster("local[*]")
    val ssc = new StreamingContext(conf, Seconds(5))
    val topic: String = Constansts.ODS_ORDER_INFO
    val groupId: String = Constansts.ODS_ORDER_INFO_GROUP


    //redis读取偏移量
    val offsertMap: Map[TopicPartition, Long] = OffsetManagerUtil.getOffset(topic, groupId)


    //判断是否读取到偏移量，读取到则从偏移量位置开始消费，没有读取到则从头开始消费
    var recordDStream: InputDStream[ConsumerRecord[String, String]] = null
    if (offsertMap != null && offsertMap.size > 0) {
      recordDStream = MyKafkaUtil.getKafkaStream(topic, ssc, offsertMap, groupId)
    } else {
      recordDStream = MyKafkaUtil.getKafkaStream(topic, ssc, groupId)
    }
    var offsetRanges: Array[OffsetRange] = Array.empty[OffsetRange]
    //从kafka实时获取当前的偏移量
    val offserDSatream: DStream[ConsumerRecord[String, String]] = recordDStream.transform(
      rdd => {
        offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        rdd
      }
    )
    //对offserDStream进行转换，转换为DStraeam[OrderInfo]
    val orderDStream: DStream[OrderInfo] = offserDSatream.map(
      record => {
        val jsonStr: String = record.value()
        //var create_date: String, //创建日期
        //由create_time分割出日期create_date和小时create_hour
        val orderInfo: OrderInfo = JSON.parseObject(jsonStr, classOf[OrderInfo])
        val create_time: Array[String] = orderInfo.create_time.split(" ")
        val create_date: String = create_time(0)
        val create_hour: String = create_time(1).split(":")(0)
        orderInfo.create_date = create_date
        orderInfo.create_hour = create_hour
        orderInfo
      }
    )

    /**
     * ===================2.判断是否为首单  ====================
     判断是否是首单 是保存1 否保存0 1和0 都是状态，类似拉链表是追加的
      - 是否需要过滤？
        真实业务中是不能随意过滤掉数据的，因为可能其他的业务还需要数据
      -用什么算子？
        因为需要查询phoenix，所以直接排除使用map
        又因为后续要对当前计算的DStream进行缓存，所以还需要流对象，考虑使用mapPartition
    */
    /*
    val orderInfoWithFirstFlagDStream: DStream[OrderInfo] = orderDStream.mapPartitions(
      orderInfoIter => {
        val orderInfoList: List[OrderInfo] = orderInfoIter.toList
        val userIdList: List[Long] = orderInfoList.map(_.user_id)
        //当前批次分区内首次购买的用户id List
        //因为是否是第一次下单是记录状态的 拉链表 所以这里不能拿去重值取判断
        val firstUserIdList: List[String] = orderInfoList.groupBy(_.user_id).map {
          case (userId, orderInfoIters) => (userId, orderInfoIters.count(_ => true))
        }.filter(_._2 == 1).map(_._1.toString).toList
        //首次购买人数大于0则执行查询
        if (firstUserIdList.length > 0) {
          //可以直接将集合放到字符串中 坑1 拼接首次下单字符串
          val sql =
            s"""
               |select * from ods_user_info
               |where user_id in (${firstUserIdList})
               |""".stripMargin
          println(sql)
          //phoenix查询的结果字段都是大写 坑2
          val userStatusList: List[JSONObject] = PhoenixUtil.queryList(sql)
          //已经下单过的用户
          val consumedUserIdList: List[String] = userStatusList.map(_.getString("USER_ID"))
          //首次phoenix没有任何记录
          if (consumedUserIdList.length>0) {
            firstUserIdList.toBuffer.--=(consumedUserIdList)
          }
          for (orderInfo <- orderInfoList) {
            if (firstUserIdList.contains(orderInfo.user_id.toString)) {
              orderInfo.if_first_order = "1"
            } else {
              orderInfo.if_first_order = "0"
            }
          }
        }
        orderInfoList.toIterator
      }
    )
    */
    //方案2  以分区为单位，将整个分区的数据拼接一条SQL进行一次查询
    val orderInfoWithFirstFlagDStream: DStream[OrderInfo] = orderDStream.mapPartitions {
      orderInfoItr => {
        //当前一个分区中所有的订单的集合
        val orderInfoList: List[OrderInfo] = orderInfoItr.toList
        //获取当前分区中获取下订单的用户
        val userIdList: List[Long] = orderInfoList.map(_.user_id)

        //根据用户集合到Phoenix中查询，看看哪些用户下过单   坑1  字符串拼接
        var sql: String =
          s"select user_id,if_consumed from user_status0523 where user_id in('${userIdList.mkString("','")}')"

        //执行sql从Phoenix获取数据
        val userStatusList: List[JSONObject] = PhoenixUtil.queryList(sql)
        //获取消费过的用户id   坑2  大小写
        val consumedUserIdList: List[String] = userStatusList.map(_.getString("USER_ID"))
        for (orderInfo <- orderInfoList) {
          //坑3    类型转换
          if (consumedUserIdList.contains(orderInfo.user_id.toString)) {
            orderInfo.if_first_order = "0"
          } else {
            orderInfo.if_first_order = "1"
          }
        }
        orderInfoList.toIterator
      }
    }
    //orderInfoWithFirstFlagDStream.print(1000)

    /**
    ===================4.同一批次中状态修正  ====================
    应该将同一采集周期的同一用户的最早的订单标记为首单，其它都改为非首单
    	同一采集周期的同一用户-----按用户分组（groupByKey）
    	最早的订单-----排序，取最早（sortwith）
    	标记为首单-----具体业务代码
     */
    //z转为kv结构 以用户id作为key，方便聚合用户id判断是否是首次
    val orderInfoKVDStream: DStream[(Long, OrderInfo)] = orderInfoWithFirstFlagDStream.map(
      orderInfo => (orderInfo.user_id, orderInfo)
    )
    val groupByuserIdDStream: DStream[(Long, Iterable[OrderInfo])] = orderInfoKVDStream.groupByKey
    // 校正后的DStream
    val orderInfoupdateDStream: DStream[OrderInfo] = groupByuserIdDStream.flatMap {
      case (userId, orderInfoIter) => {
        val orderInfoList: List[OrderInfo] = orderInfoIter.toList
        //
        if (orderInfoList != null && orderInfoList.size > 0) {
          //将用户下单信息按照create_time的升序排序
          //todo 为什么使用sortWith
          val sortedOrderInfoList: List[OrderInfo] = orderInfoList.sortWith {
            (orderInfo1, orderInfo2) => {
              orderInfo1.create_time < orderInfo2.create_time
            }
          }
          //取出集合中的第一个元素,就是当前批次最先的下单时间，如果=1即首单
          if (sortedOrderInfoList(0).if_first_order == "1") {
            for (i <- 1 until sortedOrderInfoList.size) {
              sortedOrderInfoList(i).if_first_order = "0"
            }
          }
          //为何不妨到里面if括号，因为这是当前外层if的返回值，不放这则没有返回值？
          sortedOrderInfoList
        } else { //否则就是之前已经保存了首单信息在hbase了，当前批次都是0
          orderInfoList
        }
      }
    }

    /**
     * ===================5.和省份维度表进行关联====================
     * 算子选择：
     *    在sql中就是使用order_info join base_province表
     *    sparkStreaming 主键join不太好实现
     *    因为之前OrderInfo 封装成了样例类 并且存在province_id这个主键
     *    所以可以 将 省份表封装为样例类 ，通过hbase查出省份表数据，
     *    通过判断省份样例类中有没有province_id 判断是否能关联上，也就实现了join的功能
     *    因为需要rdd进行操作
     */
    //采集周期进行处理，在driver端执行
    val orderInfoWithProvinceDStream: DStream[OrderInfo] = orderInfoupdateDStream.transform(
      rdd => {

        //sql
        val sql =
          """
            |select id,name,area_code,iso_code
            |from gmall0223_province_info
            |""".stripMargin
        //phoenix query
        val provinceInfoList: List[JSONObject] = PhoenixUtil.queryList(sql)
        val provinceInfoMap: Map[String, ProvinceInfo] = provinceInfoList.map {
          jsonObj => {
            val provinceInfo: ProvinceInfo = JSON.toJavaObject(jsonObj, classOf[ProvinceInfo])
            (provinceInfo.id, provinceInfo)
          }
          // 坑1 转为map为了更方便的获取键值对的值
        }.toMap
        //广播省份样例类对象的map，用于在Executor分区对OrderInfo进行和省份表的id join
        val bdMap: Broadcast[Map[String, ProvinceInfo]] = ssc.sparkContext.broadcast(provinceInfoMap)
        rdd.map {
          orderInfo => {
            //坑2 OrderInfo的province_id是Long
            val proInfo: ProvinceInfo = bdMap.value.getOrElse(orderInfo.province_id.toString, null)
            if (proInfo != null) {
              orderInfo.province_name = proInfo.name
              orderInfo.province_area_code = proInfo.area_code
              orderInfo.province_iso_code = proInfo.iso_code
            }
            orderInfo
          }
        }
      }
    )

    /**
     * ===================6.和用户维度表进行关联====================
     */
    val orderInfoWithUserDStream: DStream[OrderInfo] = orderInfoWithProvinceDStream.mapPartitions(
      orderInfo => {
        val orderInfoList: List[OrderInfo] = orderInfo.toList
        val userIdList: List[Long] = orderInfoList.map(_.user_id)
        //查找user_info表中所有在当前批次的用户，按照UserInfo样例类字段顺序
        val sql: String =
          s"""
             |select id,
             |user_level,
             |birthday,
             |gender,
             |age_group,
             |gender_name
             |from gmall0223_user_info
             |where id in (${userIdList.mkString(",")})
             |""".stripMargin
        //user_info表
        val jsonUserList: List[JSONObject] = PhoenixUtil.queryList(sql)
        //封装成UserInfo样例类
        val userMap: Map[String, UserInfo] = jsonUserList.map(
          jsonObj => {
            val userInfo: UserInfo = JSON.toJavaObject(jsonObj, classOf[UserInfo])
            (userInfo.id, userInfo)
          }
        ).toMap
        for (orderInfo <- orderInfoList) {
          //可以匹配上 user_id 的userInfo
          val userInfoObj: UserInfo = userMap.getOrElse(orderInfo.user_id.toString, null)
          if (userInfoObj != null) {
            orderInfo.user_age_group = userInfoObj.age_group
            orderInfo.user_gender = userInfoObj.gender
          }
        }
        orderInfoList.toIterator
      }
    )

    /**
     * ===================3.维护首单用户状态|||保存订单到ES中  ====================
     * 如果当前用户为首单用户（第一次消费），那么我们进行首单标记之后，应该将用户的消费状态保存到Hbase中，等下次这个用户
     * 再下单的时候，就不是首单了
     */
    import org.apache.phoenix.spark._
    orderInfoWithUserDStream.foreachRDD(
      rdd=>{
        //第一次rdd的算子转换
        val firstOrderInfo: RDD[OrderInfo] = rdd.filter(_.if_first_order == 1)
        val firstRDD: RDD[(String, String)] = firstOrderInfo.map {
          //注意转换user_id的类型
          case orderInfo => (orderInfo.user_id.toString, "1")
        }
        //firstorder存到hbase
        firstRDD.saveToPhoenix("USER_STATUS0223",
          Seq("USER_ID","IF_CONSUMED"),
          new Configuration(),
          Some("hadoop102:2181,hadoop103:2181,hadoop104:2181")
        )
        //第二次的算子转换
        //数据保存到ES，保存什么？用什么存？
        //
        rdd.foreachPartition(
          orderInfo=>{
            val orderList: List[OrderInfo] = orderInfo.toList
            val orderTupleList: List[(String, OrderInfo)] = orderList.map(
              orderInfo => {
                (orderInfo.id.toString, orderInfo)
              }
            )
            //用于拼接日期的字符串到 ES 的index里
            val formatter: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd")
            val nowTime: LocalDateTime = LocalDateTime.now()
            var dt = nowTime.format(formatter) //String
            MyEsUtil.putIndex(orderTupleList,"gmall0223_order_info_" + dt)

            //写回到kafka的dwd层
            for (orderInfo <- orderList) {
              MyKafkaSink.send(Constansts.DWD_ORDER_INFO,JSON.toJSONString(orderInfo,new SerializeConfig(true)))
            }
          }
        )
      }
    )
  }

}
