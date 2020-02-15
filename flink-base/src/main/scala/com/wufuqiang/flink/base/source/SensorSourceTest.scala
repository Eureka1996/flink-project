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
    val resultStream = rawStream.keyBy(0)
      .sum(2)
    resultStream.print("wufuqiang")
    env.execute("SensorSourceTest")
  }

}
