package com.wufuqiang.flink.base.process

import com.wufuqiang.flink.base.source.SensorSource
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment,_}

/**
  * @ author wufuqiang
  **/
object FreezingAlertDemo {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    //设置使用事件时间
//    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val rawStream = env.addSource(new SensorSource)
    val tag:String = "low temperature alert"
    val processedStream = rawStream.process(new FreezingAlert(59.3,tag))
    processedStream.print("processed")
    processedStream.getSideOutput(new OutputTag[String](tag)).print(tag)
    env.execute("MyProcessDemo")
  }

}
