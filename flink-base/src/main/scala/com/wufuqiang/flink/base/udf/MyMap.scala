package com.wufuqiang.flink.base.udf

import com.wufuqiang.flink.base.entries.SensorReading
import org.apache.flink.api.common.functions.{IterationRuntimeContext, RichMapFunction, RuntimeContext}
import org.apache.flink.configuration.Configuration

/**
  * @ author wufuqiang
  **/
class MyMap extends RichMapFunction[SensorReading,String]{
  var subTaskIndex = 0
  override def map(in: SensorReading): String = {
    in.toString
  }

  override def close(): Unit = super.close()

  override def open(parameters: Configuration): Unit = {
    subTaskIndex = getRuntimeContext.getIndexOfThisSubtask
  }
}
