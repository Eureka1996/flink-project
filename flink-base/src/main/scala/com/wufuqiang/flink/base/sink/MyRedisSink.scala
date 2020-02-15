package com.wufuqiang.flink.base.sink

import com.wufuqiang.flink.base.entries.SensorReading
import org.apache.flink.streaming.connectors.redis.RedisSink
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig

/**
  * @ author wufuqiang
  **/
class MyRedisSink {

  def getRedisSink():RedisSink[SensorReading] = {
    val conf = new FlinkJedisPoolConfig.Builder()
      .setHost("localhost").setPort(6379).build()
    new RedisSink(conf,new MyRedisMapper())
  }

}
