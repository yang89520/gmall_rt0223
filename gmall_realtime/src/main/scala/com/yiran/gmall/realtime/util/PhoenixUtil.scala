package com.yiran.gmall.realtime.util

import com.alibaba.fastjson.JSONObject

import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet, ResultSetMetaData}
import java.util
import scala.collection.mutable.ListBuffer

/**
 * 用于从Phoenix中查询数据的工具
 */
object PhoenixUtil {
  def queryList(sql:String):List[JSONObject]={
    //注册驱动
    Class.forName("org.apache.phoenix.jdbc.PhoenixDriver")
    //建立连接
    val conn: Connection = DriverManager.getConnection("jdbc:phoenix:hadoop102,hadoop103,hadoop104:2181")
    //创建数据库操作对象
    val ps: PreparedStatement = conn.prepareStatement(sql)
    //处理结果集
    val resList = ListBuffer[JSONObject]()
    val resultSet: ResultSet = ps.executeQuery()
    val metaData: ResultSetMetaData = resultSet.getMetaData
    while (resultSet.next()){
      val userStatusJsonObj = new JSONObject()
      for(i <-1 to  metaData.getColumnCount){
        userStatusJsonObj.put(metaData.getColumnName(i),resultSet.getObject(i))
      }
      resList.append(userStatusJsonObj)
    }
    //释放资源
    ps.close()
    conn.close()
    resList.toList
  }
  def upsert(sql:String):List[JSONObject]={
    //注册驱动
    Class.forName("org.apache.phoenix.jdbc.PhoenixDriver")
    //建立连接
    val conn: Connection = DriverManager.getConnection("jdbc:phoenix:hadoop102,hadoop103,hadoop104:2181")
    //创建数据库操作对象
    val ps: PreparedStatement = conn.prepareStatement(sql)
    //处理结果集
    val resList = ListBuffer[JSONObject]()
    val resultSet: Boolean = ps.execute()
//    val metaData: ResultSetMetaData = resultSet.getMetaData
//    while (resultSet.next()){
//      val userStatusJsonObj = new JSONObject()
//      for(i <-1 to  metaData.getColumnCount){
//        userStatusJsonObj.put(metaData.getColumnName(i),resultSet.getObject(i))
//      }
//      resList.append(userStatusJsonObj)
//    }
    if (resultSet){
      println("插入成功")
    }else{
      println("插入失败")
    }
    //释放资源
    ps.close()
    conn.close()
    resList.toList
  }

  def main(args: Array[String]): Unit = {
//    val list: List[JSONObject] = queryList("select * from user_status0223")
//    println(list)

    //create tables
    upsert("create table gmall0223_province_info (id varchar primary key,info.name varchar,info.area_code varchar,info.iso_code varchar)SALT_BUCKETS = 3 ")
    upsert("create table gmall0223_user_info (id varchar primary key ,user_level varchar,  birthday varchar,  gender varchar, age_group varchar , gender_name varchar)SALT_BUCKETS = 3 ")
  }
}
