package com.wufuqiang.flink.source;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;

import java.util.List;
import java.util.Properties;

public class FlinkSourceUtils {

    public static FlinkKafkaConsumer010<String>  getKafkaConsumer(String brokers, List<String> topics, String groupid){
        Properties kafkaProperties = new Properties();
        kafkaProperties.setProperty("bootstrap.servers",brokers);
        kafkaProperties.setProperty("group.id",groupid);
        FlinkKafkaConsumer010<String> kafkaConsumer = new FlinkKafkaConsumer010<>(topics, new SimpleStringSchema(), kafkaProperties);
        kafkaConsumer.setStartFromLatest();
        return kafkaConsumer;
    }

}
