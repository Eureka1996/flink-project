package com.wufuqiang.flink.base.mulstream

import com.wufuqiang.flink.base.entries.SensorReading
import com.wufuqiang.flink.base.source.SensorSource
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment,_}

/**
  * @ author wufuqiang
  **/
object SplitSelectStream {
  def main(args:Array[String]):Unit={
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val rawStream:DataStream[SensorReading] = env.addSource(new SensorSource)
    val splitStream = rawStream.split(item=>{
      if(item.temperature>61.5){
        Seq("high")
      }else{
        Seq("normal")
      }
    })
    val highStream = splitStream.select("high")
    val normalStream = splitStream.select("normal")

    val lowStream = normalStream.filter(item => item.temperature < 59.5)
      .map(item=>(item.id,item.temperature))

    //union之前丙个流的类型必须是一样，connect可以不一样，在之后的coMap中再去调整成为一样的
    //connect只能操作两个流，union可以操作多个
    val connectStream = lowStream.connect(highStream)
    val coMapStream = connectStream.map(
      item1 => item1,
      item2 => item2
    )

    highStream.print("high")
    normalStream.print("normal")
    lowStream.print("low")
    coMapStream.print("coMap")
    env.execute("SplitSelectStreamTest")
  }
}
