package com.wufuqiang.flink.base.entries

import lombok.{Data, ToString}

/**
  * @ author wufuqiang
  **/
case class SensorReading(id:String,timestamp:Long,temperature:Double)