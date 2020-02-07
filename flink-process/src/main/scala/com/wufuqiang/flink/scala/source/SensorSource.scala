package com.wufuqiang.flink.scala.source

import org.apache.flink.streaming.api.functions.source.SourceFunction

/**
  * @ author wufuqiang
  * @ date 2019/12/19/019 - 23:21
  **/
class SensorSource extends SourceFunction[SensorReading] {
  //定义一个flag，表示数据源是否正常运行
  var running:Boolean = true

  //取消数据源生成数据
  override def cancel(): Unit = {
    running = false
  }

  //正常生成数据
  override def run(sourceContext: SourceFunction.SourceContext[SensorReading]): Unit = ???
}
