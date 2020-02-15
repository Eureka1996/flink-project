package com.wufuqiang.flink.base.source

import com.wufuqiang.flink.base.entries.SensorReading
import org.apache.flink.streaming.api.functions.source.SourceFunction

import scala.util.Random

/**
  * @ author wufuqiang
  **/
class SensorSource extends SourceFunction[SensorReading]{
  //表示数据源是否正常生成
  var running:Boolean = true

  //取消数据源的生成
  override def cancel(): Unit = {
    running = false
  }

  //正常生成数据源
  override def run(sourceContext: SourceFunction.SourceContext[SensorReading]): Unit = {
    val rand = new Random()

    var curTmp = 1.to(10).map(
      i => ("sensor_"+i,60+rand.nextGaussian())
    )

    while(running){
      curTmp.map(t=>{
        val sensorLog = new SensorReading(t._1,System.currentTimeMillis(),t._2+rand.nextGaussian())
        sourceContext.collect(sensorLog)
      })
      Thread.sleep(5000)
    }

  }
}
