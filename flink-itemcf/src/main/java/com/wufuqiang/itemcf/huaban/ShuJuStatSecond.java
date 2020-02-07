package com.wufuqiang.itemcf.huaban;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import org.apache.flink.util.Collector;

public class ShuJuStatSecond {
    public static void main(String[] args) {
        int dayStart = Integer.parseInt(args[0]);
        int dayEnd = Integer.parseInt(args[1]);

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        BatchTableEnvironment tableEnvironment = TableEnvironment.getTableEnvironment(env);

        DataSet<String> rawDs = null;

        String pathFormat = "/recom/petal/218/1874/764/%d";
        for (int i = dayStart; i <= dayEnd; i++) {
            String pathStr = String.format(pathFormat, i);
            System.out.println("pathStr:" + pathStr);
            if (rawDs == null) {
                rawDs = env.readTextFile(pathStr);
            } else {
                rawDs.union(env.readTextFile(pathStr));
            }
        }

        DataSet<Tuple2<String, String>> huaBanDs = rawDs.flatMap(new FlatMapFunction<String, Tuple2<String, String>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<String, String>> out) throws Exception {
                try {
                    JSONObject jsonObject = JSONObject.parseObject(value);
                    String itemId = jsonObject.get("itemId").toString();
                    Object[] boardsLists = jsonObject.getJSONArray("boardsList").toArray();
                    for (Object obj : boardsLists) {
                        JSONObject newJson = (JSONObject) obj;
                        String boardId = newJson.get("boardId").toString();
                        out.collect(new Tuple2<>(itemId, boardId));
                    }

                } catch (Exception e) {
                    System.out.println("json:" + value);
                }
            }
        }).distinct();

        DataSet<Tuple1<String>> itemIdDs = huaBanDs.flatMap(new FlatMapFunction<Tuple2<String, String>, Tuple1<String>>() {
            @Override
            public void flatMap(Tuple2<String, String> value, Collector<Tuple1<String>> out) throws Exception {
                out.collect(new Tuple1<>(value.f0));
            }
        });

        DataSet<Tuple1<String>> boardIdDs = huaBanDs.flatMap(new FlatMapFunction<Tuple2<String, String>, Tuple1<String>>() {
            @Override
            public void flatMap(Tuple2<String, String> value, Collector<Tuple1<String>> out) throws Exception {
                out.collect(new Tuple1<>(value.f1));
            }
        });

        itemIdDs.coGroup(huaBanDs).where(0).equalTo(0).with(new CoGroupFunction<Tuple1<String>, Tuple2<String, String>, Object>() {
            @Override
            public void coGroup(Iterable<Tuple1<String>> first, Iterable<Tuple2<String, String>> second, Collector<Object> out) throws Exception {

            }
        });
                
                


        try {
            env.execute("ShuJuStat");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
