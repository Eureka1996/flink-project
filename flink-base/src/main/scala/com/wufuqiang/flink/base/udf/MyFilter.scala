package com.wufuqiang.flink.base.udf

import com.wufuqiang.flink.base.entries.SensorReading
import org.apache.flink.api.common.functions.FilterFunction

/**
  * @ author wufuqiang
  **/
class MyFilter extends FilterFunction[SensorReading]{
  override def filter(t: SensorReading): Boolean = {
      t.id.startsWith("sensor_1")
  }
}
