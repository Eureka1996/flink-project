package com.wufuqiang.flink.base.assigner

import com.wufuqiang.flink.base.entries.SensorReading
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.watermark.Watermark

/**
  * @ author wufuqiang
  **/
class MyPeriodicAssigner extends AssignerWithPeriodicWatermarks[SensorReading] {
  val bound: Long =  60 * 1000 //允许的延时时间为1分钟
  var maxTs: Long =  Long.MinValue //观察到的最大时间戳
  override def getCurrentWatermark: Watermark = {
    new Watermark(maxTs - bound) //表示maxTs-bound之前的数据已经全部到齐
  }

  override def extractTimestamp(t: SensorReading, l: Long): Long = {
    maxTs = maxTs.max(t.timestamp)
    t.timestamp
  }
}
