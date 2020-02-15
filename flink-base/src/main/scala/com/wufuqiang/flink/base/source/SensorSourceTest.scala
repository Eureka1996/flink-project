package com.wufuqiang.flink.base.source

import com.wufuqiang.flink.base.entries.SensorReading
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._
/**
  * @ author wufuqiang
  **/
object SensorSourceTest {
  def main(args:Array[String]):Unit={
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val rawStream:DataStream[SensorReading] = env.addSource(new SensorSource)
    val resultStream = rawStream.keyBy(_.id)
      .reduce((i1,i2)=>SensorReading(i1.id,i1.timestamp,i2.temperature))
//      .sum("temperature")
    resultStream.print("wufuqiang")
    env.execute("SensorSourceTest")
  }

}
