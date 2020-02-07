package com.wufuqiang.flink.main;

import org.apache.flink.api.java.io.TextInputFormat;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class ReadFileContinue {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<String> rawDs = env.readTextFile("/Users/wufuqiang/IdeaProjects/flink-project/flink-process/src/main/resources/source");

        rawDs.print();

        try {
            env.execute("ReadFileContinue");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
