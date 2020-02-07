package com.wufuqiang.flink;

import com.wufuqiang.flink.entries.ActionLog;
import com.wufuqiang.flink.source.FlinkSourceUtils;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;

public class FlinkConsumerKafka {
    private static Logger logger = LoggerFactory.getLogger(ProcessFunction.class);
    public static void main(String[] args) throws IOException {

        String propertiesPathStr = "";
        if(args.length < 1){
            propertiesPathStr = "/Users/wufuqiang/IdeaProjects/flink-project/flink-process/src/main/resources/flink-process.properties";
        }else{
            propertiesPathStr = args[0];
        }

        ParameterTool parameterTool = ParameterTool.fromPropertiesFile(propertiesPathStr);
        String kafkaBrokers = parameterTool.get("flink.process.kafka.brokers");
        String kafkaGroupId = parameterTool.get("flink.process.groupid");
        String kafkaTopic = parameterTool.get("flink.process.kafka.topic");

        FlinkKafkaConsumer010<String> kafkaConsumer = FlinkSourceUtils.getKafkaConsumer(kafkaBrokers, Arrays.asList(kafkaTopic),kafkaGroupId);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<String> dataStream = env.addSource(kafkaConsumer);

        DataStream<ActionLog> actionLogDataStream = dataStream.map(new MapFunction<String, ActionLog>() {
            @Override
            public ActionLog map(String value) throws Exception {
                return ActionLog.getInstance(value);
            }
        }).filter(new FilterFunction<ActionLog>() {
            @Override
            public boolean filter(ActionLog value) throws Exception {
                if(value == null){
                    return false;
                }
                return true;
            }
        });

        DataStream<Tuple3<String, String, Integer>> sum = actionLogDataStream.map(new MapFunction<ActionLog, Tuple3<String, String, Integer>>() {
            @Override
            public Tuple3<String, String, Integer> map(ActionLog value) throws Exception {

                return new Tuple3<String,String,Integer>(value.getSceneId(),value.getAction(),1);
            }
        }).keyBy(0,1).timeWindow(Time.seconds(10)).sum(2);

        sum.print();

        try {
            env.execute("ProcessFunctionTest");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
