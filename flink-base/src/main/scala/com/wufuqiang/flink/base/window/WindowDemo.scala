package com.wufuqiang.flink.base.window

import com.wufuqiang.flink.base.source.SensorSource
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment,_}
import org.apache.flink.streaming.api.windowing.time.Time

/**
  * @ author wufuqiang
  **/
object WindowDemo {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val rawStream = env.addSource(new SensorSource)
    val minTemperatureStream = rawStream.keyBy(_.id).timeWindow(Time.seconds(10)).sum("temperature").filter(_.id == "sensor_3")
    minTemperatureStream.print("minTemp")
    env.execute("WindowDemo")
  }
}
