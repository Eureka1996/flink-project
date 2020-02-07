package com.wufuqiang.flink.sink.hdfs2redis;

import com.wufuqiang.flink.sink.outputformat.RedisOutputFormat;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.util.Collector;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;

public class Hdfs2RedisVersion5 {

    public static void main(String[] args) {


        String propertiesPathStr = args[0];

        ParameterTool parameterTool = null;
        try {
            parameterTool = ParameterTool.fromPropertiesFile("./"+propertiesPathStr);
        } catch (IOException e) {
            e.printStackTrace();
        }

        String businessId = parameterTool.get("itemcf.businessids").split(",")[0];
        String sceneId = parameterTool.get("itemcf.sceneids").split(",")[0];
        String itemSetId = parameterTool.get("itemcf.itemsetids").split(",")[0];

        String today = parameterTool.get("itemcf.day.begin");
        if("yesterday".equals(today)){
            SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
            Calendar calendar = Calendar.getInstance();
            calendar.setTime(new Date());
            calendar.add(Calendar.DAY_OF_MONTH,-1);
            Date yesterdayDay = calendar.getTime();
            today = sdf.format(yesterdayDay);
        }

        int dayNum = Integer.valueOf(parameterTool.get("itemcf.day.numbers"));
        String redisKey = parameterTool.get("itemcf.redis.key.prefix");
        int filterValueLength = Integer.parseInt(parameterTool.get("itemcf.redis.value.length.minimum"));
        int cutValueLength = Integer.parseInt(parameterTool.get("itemcf.redis.value.length.maximum"));

        String sourcePathPrefix = parameterTool.get("itemcf.datas.outputpath.prefix");
        String redisKeysPrefix = parameterTool.get("itemcf.redis.key.cachepath.prefix");


        String sourcePath = String.format("%s/%s/%s/%s/%s",sourcePathPrefix,businessId,sceneId,itemSetId,today+"-"+dayNum);
        String redisKeysPath = String.format("%s/%s/%s/%s/%s",redisKeysPrefix,businessId,sceneId,itemSetId,today+"-"+dayNum);

        System.out.println("sourecePath:"+sourcePath);
        System.out.println("redisKeysPath:"+redisKeysPath);

        String redisClusterNodes = parameterTool.get("itemcf.redis.cluster.nodes");
        System.out.println("redisClusterNodes:"+redisClusterNodes);

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<String> rawDs = env.readTextFile(sourcePath);
        /**
         * 写到redis的数据要求：
         *      key:为  recom:item_764:p2p: + itemid
         *      value是一个list,每个元素为itemid:score
         *      value中元素按分数score从大到小排
         *      过滤掉value size小于20的
         *      value size大于400的进行截断
         *      加过期时间7天
         */

        FlatMapOperator<String, Tuple2<String, String[]>> putToRedisDs = rawDs.flatMap(new FlatMapFunction<String, Tuple2<String, String[]>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<String, String[]>> out) throws Exception {

                String[] keyValue = value.split("_");
                String[] values = keyValue[1].split(",");

                if(values.length<filterValueLength){
                    return;
                }
                //拼接redis key recom:item_764:p2p:1532901063
                String key = redisKey+ keyValue[0];
                //对list size大于400的进行截断
                int length = values.length > cutValueLength ? cutValueLength : values.length;
                String[] valuesCuted = Arrays.copyOfRange(values, 0, length);
                out.collect(new Tuple2<>(key, valuesCuted));
            }
        });

        putToRedisDs.flatMap(new FlatMapFunction<Tuple2<String, String[]>, String>() {
            @Override
            public void flatMap(Tuple2<String, String[]> value, Collector<String> out) throws Exception {
                out.collect(value.f0);
            }
        }).writeAsText(redisKeysPath, FileSystem.WriteMode.OVERWRITE);
        putToRedisDs.setParallelism(1).output(new RedisOutputFormat(redisClusterNodes));


        try {
            env.execute("Hdfs To Redis");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
