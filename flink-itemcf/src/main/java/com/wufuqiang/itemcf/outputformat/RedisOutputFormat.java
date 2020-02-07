package com.wufuqiang.itemcf.outputformat;

import com.wufuqiang.itemcf.utils.MyJedisCluster;
import org.apache.flink.api.common.io.OutputFormat;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;

import java.io.IOException;

public class RedisOutputFormat implements OutputFormat<Tuple2<String,String[]>> {


    MyJedisCluster myJedisCluster;
    String redisNodes = null;

    public RedisOutputFormat(String redisNodes){
        this.redisNodes = redisNodes;
    }


    @Override
    public void configure(Configuration parameters) {

    }

    @Override
    public void open(int taskNumber, int numTasks) throws IOException {
        myJedisCluster = MyJedisCluster.getMyJedisCluster(redisNodes);

    }

    @Override
    public void writeRecord(Tuple2<String, String[]> record) throws IOException {

        myJedisCluster.pubToRedis(record.f0,record.f1);
    }

    @Override
    public void close() throws IOException {
//        myJedisCluster.closeJediCluster();
    }


}
