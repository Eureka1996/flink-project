package com.wufuqiang.flink.base.sink

import java.util

import com.wufuqiang.flink.base.entries.SensorReading
import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.flink.streaming.connectors.elasticsearch.{ElasticsearchSinkFunction, RequestIndexer}
import org.elasticsearch.client.Requests


/**
  * @ author wufuqiang
  **/
class MyElasticsearchSinkFunction extends ElasticsearchSinkFunction[SensorReading]{
  override def process(t: SensorReading, runtimeContext: RuntimeContext, requestIndexer: RequestIndexer): Unit = {
    val json = new util.HashMap[String,String]()
    json.put("sensor_id",t.id)
    json.put("sensor_timestamp",t.timestamp.toString)
    json.put("sensor_temperature",t.temperature.toString)
    //创建index request，准备发送数据
    val indexRequest = Requests.indexRequest().index("sensor")
      .`type`("readingdata")
      .source(json)

    //利用index发送请求，写入数据
    requestIndexer.add(indexRequest)
  }
}
