package com.wufuqiang.flink.main;

import com.wufuqiang.flink.entries.MysqlItem;
import com.wufuqiang.flink.source.SourceFromMysql;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class MysqlSourceTest {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<MysqlItem> mysqlItemDataStreamSource = env.addSource(new SourceFromMysql("select id,item_id,title,tag,content  from item_9033 where id <=  10"));
        mysqlItemDataStreamSource.print();
//        mysqlItemDataStreamSource.writeAsText("hdfs://clusterA:8020/tmp/eureka/ft-mysql", FileSystem.WriteMode.OVERWRITE);

        try {
            env.execute("MysqlSource");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
