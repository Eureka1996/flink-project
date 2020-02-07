package com.wufuqiang.flink.scala.source

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010
import org.apache.kafka.common.serialization.StringDeserializer

/**
  * @ author wufuqiang
  * @ date 2019/12/17/017 - 22:45
  **/

case class SensorReading(id:String,timestamp:Long,temperature:Double)

object SourceTest {
  def main(args:Array[String]):Unit={

    val properties = new Properties()
    properties.setProperty("bootstrap.servers","localhost:9092")
    properties.setProperty("group.id","consumer-group")
    properties.setProperty("key.deserializer","org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("value.deserializer","org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("auto.offset.reset","latest")

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val stream = env.addSource(new FlinkKafkaConsumer010[String]("sensor",new SimpleStringSchema(),properties))
    stream.print().setParallelism(1)

    env.execute("SourceText")
  }
}
