package com.wufuqiang.flink.table

import com.wufuqiang.flink.base.entries.SensorReading
import com.wufuqiang.flink.base.source.SensorSource
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.table.api.{Table, Tumble}
import org.apache.flink.table.api.scala.StreamTableEnvironment


/**
  * @ author wufuqiang
  **/
object TableDemo {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val rawStream = env.addSource(new SensorSource).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[SensorReading](Time.seconds(5)) {
      override def extractTimestamp(element: SensorReading): Long = element.timestamp
    })

    val tableEnv:StreamTableEnvironment = StreamTableEnvironment.create(env)


//    tableEnv.fromDataStream(rawStream,'tid,'timestamp.rowtime,'ttemperature)

    val resultTable:Table = tableEnv.scan("sensor")
      .window(Tumble.over("timestamp"))
      .groupBy('id, 'tt).select('id, 'id.count)
    val resultDStream = resultTable.toRetractStream[(String,Long)]
    resultDStream.print()

    env.execute("TableDemo")
  }

}
