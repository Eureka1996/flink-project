package com.wufuqiang.flink.base.wc

import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.api.scala._

/**
  * @ author wufuqiang
  **/

/**
  * 批处理进行单词计数
  */
object WorldCount {
  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val filePath = "D:\\wufuqiangbd\\ideaProjects\\flink-project\\flink-base\\src\\main\\resources\\world_count.txt"
    val rawStream = env.readTextFile(filePath)

    val wcStream = rawStream.flatMap(_.split(" ")).map((_,1)).groupBy(0).sum(1)

    wcStream.print()


//    env.execute("WorldCount")
  }
}
