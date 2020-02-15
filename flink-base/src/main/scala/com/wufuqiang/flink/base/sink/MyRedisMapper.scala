package com.wufuqiang.flink.base.sink

import com.wufuqiang.flink.base.entries.SensorReading
import org.apache.flink.streaming.connectors.redis.common.mapper.{RedisCommand, RedisCommandDescription, RedisMapper}

/**
  * @ author wufuqiang
  **/
class MyRedisMapper extends RedisMapper[SensorReading] {

  //定义保存数据到redis的命令
  override def getCommandDescription: RedisCommandDescription = {
    new RedisCommandDescription(RedisCommand.HSET,"sensor")
  }

  //定义保存到redis的value
  override def getValueFromData(t: SensorReading): String = t.temperature.toString

  //定义保存到redis的key
  override def getKeyFromData(t: SensorReading): String = t.id
}
