package com.wufuqiang.flink.base.source

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer010, FlinkKafkaProducer010}

/**
  * @ author wufuqiang
  **/

class KafkaSource(serversParam:String,groupIdParam:String,topicParam:String) {
  var servers = serversParam
  var topic = topicParam
  var groupId = groupIdParam
  var kafkaProperties:Properties = _

  def getSource(): FlinkKafkaConsumer010[String] ={
    val kafkaPros = new Properties()
    kafkaPros.setProperty("bootstrap.servers",servers)
    kafkaPros.setProperty("group.id",groupId)
    kafkaPros.setProperty("key.deserializer","org.apache.kafka.common.serialization.StringDeserializer")
    kafkaPros.setProperty("value.deserializer","org.apache.kafka.common.serialization.StringDeserializer")
    kafkaPros.setProperty("auto.offset.reset","latest")
    this.kafkaProperties = kafkaPros
    new FlinkKafkaConsumer010[String](topic,new SimpleStringSchema(),kafkaPros)
  }

  def getSink():FlinkKafkaProducer010[String]={
    new FlinkKafkaProducer010[String](servers,topic,new SimpleStringSchema())
  }



}
