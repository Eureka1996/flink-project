package com.wufuqiang.flink.base.sink

import java.sql
import java.sql.{DriverManager, PreparedStatement}

import com.wufuqiang.flink.base.entries.SensorReading
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}

/**
  * @ author wufuqiang
  **/
class MyJdbcSink(url:String,user:String,password:String) extends RichSinkFunction[SensorReading] {
  var connection:sql.Connection = _
  var insertStmt: PreparedStatement = _
  var updateStmt: PreparedStatement = _

  //初始化，创建连接和预编译语句
  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
    this.connection = DriverManager.getConnection("jdbc:mysql://localhost:3306/test","root","123456")
    this.insertStmt = this.connection.prepareStatement("INSERT INTO tablename(id,temp) values(?,?)")
    this.updateStmt = this.connection.prepareStatement("UPDATE tablename set temp = ? where id = ?")
  }

  //调用连接，执行sql
  override def invoke(value: SensorReading, context: SinkFunction.Context[_]): Unit = {
    updateStmt.setDouble(1,value.temperature)
    updateStmt.setString(2,value.id)
    updateStmt.execute()
    if(updateStmt.getUpdateCount == 0){
      insertStmt.setString(1,value.id)
      insertStmt.setDouble(2,value.temperature)
      insertStmt.execute()
    }

  }

  override def close(): Unit = {
    this.insertStmt.close()
    this.updateStmt.close()
    this.connection.close()
  }
}
