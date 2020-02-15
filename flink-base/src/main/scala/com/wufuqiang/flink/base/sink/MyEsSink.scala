package com.wufuqiang.flink.base.sink

import java.util

import com.wufuqiang.flink.base.entries.SensorReading
import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.flink.streaming.connectors.elasticsearch.{ElasticsearchSinkFunction, RequestIndexer}
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink
import org.apache.http.HttpHost

/**
  * @ author wufuqiang
  **/
class MyEsSink(esServices:String) {

  val services = esServices
  val httpHosts = new util.ArrayList[HttpHost]()
  //services格式为：host1:port,host2:port
  services.split(",").foreach(item=>{
    val hostPort = item.split(":")
    httpHosts.add(new HttpHost(hostPort(0),hostPort(1).toInt))
  })

  def getesSink():ElasticsearchSink[SensorReading]={
    val esSinkBuilder = new ElasticsearchSink.Builder[SensorReading](
      this.httpHosts,new MyElasticsearchSinkFunction()
    )
    esSinkBuilder.build()
  }

}
