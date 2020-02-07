package com.wufuqiang.itemcf.validate;

import com.wufuqiang.itemcf.utils.PetalDataSetUtils;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import org.apache.flink.table.sinks.CsvTableSink;
import org.apache.flink.util.Collector;

public class CheckValues {
    /**
     * check all value is censored
     * @param args
     */
    public static void main(String[] args) {


        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();


        DataSet<Tuple1<String>> censoredDs = PetalDataSetUtils.getCensored(env, "hdfs://clusterA:8020/itemcf/huabanStandardPicture/", "pinId");

        DataSet<String> redisKVDs = env.readTextFile(args[0]);//"hdfs://clusterA:8020/itemcf/countStat/firstalgo/218/1874/764/20191112-1");
        DataSet<String> valuesDs = redisKVDs.map(new MapFunction<String, String>() {
            @Override
            public String map(String value) throws Exception {

                return value.split("_")[1];
            }
        }).flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String value, Collector<String> out) throws Exception {
                String[] valueSplited = value.split(",");
                for (String itemScore : valueSplited) {
                    out.collect(itemScore.split(":")[0]);
                }
            }
        }).distinct();

        BatchTableEnvironment tableEnvironment = TableEnvironment.getTableEnvironment(env);
        //获取每个itemid被多少个userid点击过
        tableEnvironment.registerDataSet("CensoredItemIdTable",censoredDs);
        Table censoredItemIdTable = tableEnvironment.scan("CensoredItemIdTable");

        tableEnvironment.registerDataSet("RedisValusTable",valuesDs);
        Table redisValuesTable = tableEnvironment.scan("RedisValusTable");

        redisValuesTable.minus(censoredItemIdTable).writeToSink(new CsvTableSink("hdfs://clusterA:8020/tmp/eureka/checkvalue","/",24, FileSystem.WriteMode.OVERWRITE));


        try {
            env.execute("CheckAllValueIsCensored");
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
