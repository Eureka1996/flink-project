package com.wufuqiang.flink;

import com.wufuqiang.flink.entries.ActionLog;
import com.wufuqiang.flink.entries.CountWithTimestamp;
import com.wufuqiang.flink.myprocess.CountWithTimeoutFunction;
import com.wufuqiang.flink.source.FlinkSourceUtils;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;

public class ProcessFunctionTest {
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

        FlinkKafkaConsumer010<String> kafkaConsumer = FlinkSourceUtils
                .getKafkaConsumer(
                        kafkaBrokers,
                        Arrays.asList(kafkaTopic),
                        kafkaGroupId);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<String> dataStream = env.addSource(kafkaConsumer);

        DataStream<Tuple2<String,String>> actionLogDataStream = dataStream.map(new MapFunction<String, ActionLog>() {
            @Override
            public ActionLog map(String value) throws Exception {
                return ActionLog.getInstance(value);
            }
        }).flatMap(new FlatMapFunction<ActionLog, Tuple2<String,String>>() {
            @Override
            public void flatMap(ActionLog value, Collector<Tuple2<String,String>> out) throws Exception {
                if(value != null){
                    out.collect(new Tuple2<>(value.getSceneId(),value.getAction()));
                }
            }
        });

        actionLogDataStream.keyBy(0)
                .process(new CountWithTimeoutFunction())
                .print()
                ;



        try {
            env.execute("ProcessFunctionTest");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
