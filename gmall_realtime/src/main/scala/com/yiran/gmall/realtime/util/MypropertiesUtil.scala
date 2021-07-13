package com.yiran.gmall.realtime.util


import java.io.InputStreamReader
import java.nio.charset.StandardCharsets
import java.util.Properties

/**
 * load(文件名字符串) 文件在Resource里
 */
object MypropertiesUtil {
  def load(propName:String):Properties={
    val prop = new Properties()
    prop.load(new InputStreamReader(
      Thread.currentThread().getContextClassLoader.getResourceAsStream(propName)
      ,StandardCharsets.UTF_8))
    prop
  }

  def main(args: Array[String]): Unit = {
    val prop: Properties = MypropertiesUtil.load("config.properties")
    println(prop.getProperty("kafka.broker.list"))
  }
}
