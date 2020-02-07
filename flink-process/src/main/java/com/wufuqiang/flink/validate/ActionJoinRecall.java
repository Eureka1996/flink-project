package com.wufuqiang.flink.validate;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.MapOperator;
import org.apache.flink.api.java.tuple.Tuple1;

public class ActionJoinRecall {

    public static void main(String[] args) {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSet<String> actionRawDs = env.readTextFile(args[0]);
        DataSet<String> recallRawDs = env.readTextFile(args[1]);

        DataSet<Tuple1<String>> actionUserIdDs = actionRawDs.map(new MapFunction<String, Tuple1<String>>() {
            @Override
            public Tuple1<String> map(String value) throws Exception {
                String userId = "";
                try {
                    JSONObject jsonObject = JSONObject.parseObject(value);
                    userId = jsonObject.getString("userId");
                } catch (Exception e) {

                }
                return new Tuple1<>(userId);
            }
        });

        recallRawDs.map(new MapFunction<String, Tuple1<String>>() {
            @Override
            public Tuple1<String> map(String value) throws Exception {
                String userId = "";
                try{
                    JSONObject jsonObject = JSONObject.parseObject(value);
                    jsonObject.getString("");
                }catch(Exception e){

                }
                return new Tuple1<>(userId);
            }
        });


        try {
            env.execute("ActionJoinRecall");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


}
