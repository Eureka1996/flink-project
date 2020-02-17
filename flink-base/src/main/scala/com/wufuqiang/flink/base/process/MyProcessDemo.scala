package com.wufuqiang.flink.base.process

import com.wufuqiang.flink.base.entries.SensorReading
import com.wufuqiang.flink.base.source.SensorSource
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend
import org.apache.flink.runtime.state.StateBackend
import org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.windowing.time.Time

/**
  * @ author wufuqiang
  **/
object MyProcessDemo {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    env.enableCheckpointing(1000*60)
    env.setStateBackend(new RocksDBStateBackend("D:\\wufuqiangbd\\ideaProjects\\flink-project\\flink-base\\src\\main\\resources\\checkpoint"))

    val checkpointConfig = env.getCheckpointConfig
    checkpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
    checkpointConfig.setCheckpointTimeout(30*1000)
    checkpointConfig.setFailOnCheckpointingErrors(false)
    checkpointConfig.setMinPauseBetweenCheckpoints(5*1000)
    checkpointConfig.enableExternalizedCheckpoints(ExternalizedCheckpointCleanup.DELETE_ON_CANCELLATION)

    env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3,10*1000))
    env.setRestartStrategy(RestartStrategies.failureRateRestart(
      3,
      org.apache.flink.api.common.time.Time.seconds(300),
      org.apache.flink.api.common.time.Time.seconds(10)))


    //设置使用事件时间
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val rawStream = env.addSource(new SensorSource).assignTimestampsAndWatermarks(
      new BoundedOutOfOrdernessTimestampExtractor[SensorReading](Time.seconds(1)) {
        override def extractTimestamp(element: SensorReading): Long = {
          element.timestamp
        }
    })
    val keyedByStream = rawStream.keyBy(_.id)
    val processedStream = keyedByStream.process(new MyProcess())
    val processedStream2 = keyedByStream.process(new TempChangeAlert(1.5))
    //输出的最终结果类型、State的类型
    keyedByStream.flatMapWithState[(String,Double,Double,Double),Double]{
      //如果没有状态的话，也就是没有数据来过，那么就将当前数据温度值存入
      case(input:SensorReading,None) => (List.empty,Some(input.temperature))

      case(input:SensorReading,lastTemp: Some[Double]) =>
        val diff = (input.temperature - lastTemp.get).abs
        if(diff > 2){
          (List((input.id,lastTemp.get,input.temperature,diff)),Some(input.temperature))
        }else{
          (List.empty,Some(input.temperature))
        }
    }.print("flapMapWithState")

    processedStream.print("processed")
    processedStream2.print("TempChangeAlert")
    env.execute("MyProcessDemo")
  }
}
