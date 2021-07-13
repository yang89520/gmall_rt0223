package com.yiran.gmall.realtime.util

import redis.clients.jedis.{Jedis, JedisPool, JedisPoolConfig}

object MyRedisUtil {
  //定义连接池对象
  private var jedisPool:JedisPool = _

  def build():Unit={
    val prop = MypropertiesUtil.load("config.properties")
    val host = prop.getProperty("redis.host")
    val port = prop.getProperty("redis.port")

    val jedisPoolConfig: JedisPoolConfig = new JedisPoolConfig
    jedisPoolConfig.setMaxTotal(100)  //最大连接数
    jedisPoolConfig.setMaxIdle(20)   //最大空闲
    jedisPoolConfig.setMinIdle(20)     //最小空闲
    jedisPoolConfig.setBlockWhenExhausted(true)  //忙碌时是否等待
    jedisPoolConfig.setMaxWaitMillis(5000)//忙碌时等待时长 毫秒
    jedisPoolConfig.setTestOnBorrow(true) //每次获得连接的进行测试

    jedisPool = new JedisPool(jedisPoolConfig, host, port.toInt)
  }

  def getJedisClient():Jedis = {
    if (jedisPool==null)
      build()
    jedisPool.getResource //从连接池拿到一个Jedis连接
  }
  //test
  def main(args: Array[String]): Unit = {
    val jedis = getJedisClient()
    println(jedis.ping())
  }
}
