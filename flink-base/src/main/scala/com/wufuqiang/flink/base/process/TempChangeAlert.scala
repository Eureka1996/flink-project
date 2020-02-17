package com.wufuqiang.flink.base.process

import com.wufuqiang.flink.base.entries.SensorReading
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.util.Collector

/**
  * @ author wufuqiang
  **/
class TempChangeAlert(threshold:Double) extends KeyedProcessFunction[String,SensorReading,(String,Double,Double,Double)] {
  lazy val lastTempState:ValueState[Double] = getRuntimeContext.getState(new ValueStateDescriptor[Double]("lastTemp",classOf[Double],Double.MinValue))
  override def processElement(value: SensorReading, ctx: KeyedProcessFunction[String, SensorReading, (String, Double, Double,Double)]#Context, out: Collector[(String, Double, Double,Double)]): Unit = {
    val curTemp = value.temperature
    val preTemp = lastTempState.value
    lastTempState.update(curTemp)
    if(preTemp > Double.MinValue && Math.abs(curTemp-preTemp) >= threshold){
      out.collect((value.id,preTemp,curTemp,curTemp-preTemp))
    }
  }
}
