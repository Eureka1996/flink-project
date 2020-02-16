package com.wufuqiang.flink.base.process

import com.wufuqiang.flink.base.entries.SensorReading
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala.OutputTag
import org.apache.flink.util.Collector
import org.apache.flink.streaming.api.scala._

/**
  * @ author wufuqiang
  **/
/**
  * 第二个泛型为主输出流类型
  */
class FreezingAlert(temp:Double,tag:String) extends ProcessFunction[SensorReading,SensorReading] {
  val temperature:Double = temp
  val outputTag:String = tag
  lazy val alertOutput:OutputTag[String] = new OutputTag[String](outputTag)
  override def processElement(value: SensorReading, ctx: ProcessFunction[SensorReading, SensorReading]#Context, out: Collector[SensorReading]): Unit = {
      if(value.temperature<this.temperature){
        //测输出流
        ctx.output(alertOutput,"传感器"+value.id+"处于低温报警状态")
      }else{
        //主输出流
        out.collect(value)
      }
  }
}
