package com.wufuqiang.flink.base.process

import com.wufuqiang.flink.base.entries.SensorReading
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.util.Collector

/**
  * @ author wufuqiang
  **/
class MyProcess extends KeyedProcessFunction[String,SensorReading,String]{
  //定义一个状态，保存上一条数据信息
  lazy val lastTemp:ValueState[Double] = getRuntimeContext.getState(new ValueStateDescriptor[Double]("lastTemp",classOf[Double]))
  //定义一个状态，保存定时器的时间戳
  lazy val currentTimer:ValueState[Long] = getRuntimeContext.getState(new ValueStateDescriptor[Long]("currentTimer",classOf[Long]))

  override def processElement(value: SensorReading, ctx: KeyedProcessFunction[String, SensorReading, String]#Context, out: Collector[String]): Unit = {
    val preTemp = lastTemp.value()
    //更新状态管理中的值
    lastTemp.update(value.temperature)

    val curTimerTs = currentTimer.value()

    //若温度上升且没有设置过定时器，则定义一个定时器，定时器指定的是时间戳
    if(value.temperature > preTemp && curTimerTs == 0){
      val timerTs = ctx.timerService().currentProcessingTime() + 10000
      ctx.timerService().registerEventTimeTimer(timerTs)
      currentTimer.update(timerTs)
    }else if(value.temperature < preTemp || preTemp == 0.0){
      //若温度下降，或是第一条数据，则删除定时器并清空状态
      ctx.timerService().deleteEventTimeTimer(curTimerTs)
      currentTimer.clear()
    }
  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[String, SensorReading, String]#OnTimerContext, out: Collector[String]): Unit = {
      out.collect(ctx.getCurrentKey+"温度持续升高。warning!warning!")
      currentTimer.clear()
  }
}
