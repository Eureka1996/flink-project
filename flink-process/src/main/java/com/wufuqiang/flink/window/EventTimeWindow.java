package com.wufuqiang.flink.window;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

public class EventTimeWindow {
    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.getConfig().setAutoWatermarkInterval(3);

        DataStream<String> rawTxt = env.socketTextStream("localhost", 9999)
                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<String>(Time.seconds(10)) {
                    @Override
                    public long extractTimestamp(String s) {
                        String eventTimeString = s.split(" ")[0];
                        return Long.parseLong(eventTimeString);
                    }
                });

        rawTxt.map(new MapFunction<String, Tuple3<String,String,Integer>>() {

            @Override
            public Tuple3<String,String, Integer> map(String value) throws Exception {
                //eventTime,sceneId,value100
                String[] splited = value.split(" ");
                return new Tuple3<String,String,Integer>(splited[0],splited[1],Integer.parseInt(splited[2]));
            }
        }).keyBy(1).window(TumblingEventTimeWindows.of(Time.seconds(5))).reduce(new ReduceFunction<Tuple3<String, String, Integer>>() {
            @Override
            public Tuple3<String, String, Integer> reduce(Tuple3<String, String, Integer> value1, Tuple3<String, String, Integer> value2) throws Exception {

                return new Tuple3<String,String,Integer>(value1.f0,value1.f1,value1.f2+value2.f2);
            }
        }).map(new MapFunction<Tuple3<String, String, Integer>, Tuple2<String,Integer>>() {
            @Override
            public Tuple2<String, Integer> map(Tuple3<String, String, Integer> value) throws Exception {
                return new Tuple2<String,Integer>(value.f1,value.f2);
            }
        }).print();


        try {
            env.execute("Event time , watermark");
        } catch (Exception e) {
            e.printStackTrace();
        }


    }
}
