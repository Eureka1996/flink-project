package com.wufuqiang.flink.base.wc

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._

/**
  * @ author wufuqiang
  **/
object StreamWorldCount {
  def main(args:Array[String]):Unit ={
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val rawStream = env.socketTextStream("localhost",9999)

    val wcStream = rawStream.flatMap(_.split(" ")).map((_,1)).keyBy(0).sum(1)
    wcStream.print()
    env.execute("WorldCount")
  }

}
