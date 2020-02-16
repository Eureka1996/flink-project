package com.wufuqiang.flink.base.process

import com.wufuqiang.flink.base.entries.SensorReading
import com.wufuqiang.flink.base.source.SensorSource
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment,_}
import org.apache.flink.streaming.api.windowing.time.Time

/**
  * @ author wufuqiang
  **/
object MyProcessDemo {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    //设置使用事件时间
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val rawStream = env.addSource(new SensorSource).assignTimestampsAndWatermarks(
      new BoundedOutOfOrdernessTimestampExtractor[SensorReading](Time.seconds(1)) {
        override def extractTimestamp(element: SensorReading): Long = {
          element.timestamp
        }
    })
    val processedStream = rawStream.keyBy(_.id).process(new MyProcess())
    processedStream.print("processed")
    env.execute("MyProcessDemo")
  }
}
