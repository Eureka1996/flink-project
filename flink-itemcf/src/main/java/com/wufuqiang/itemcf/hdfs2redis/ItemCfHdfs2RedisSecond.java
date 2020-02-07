package com.wufuqiang.itemcf.hdfs2redis;

import com.wufuqiang.itemcf.outputformat.RedisOutputFormat;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.util.Collector;

import java.io.IOException;
import java.util.Arrays;

public class ItemCfHdfs2RedisSecond {

    public static void main(String[] args) {
        String businessId = args[0];
        String sceneId = args[1];
        String itemSetId = args[2];
        String today = args[3];
        int dayNum = Integer.valueOf(args[4]);
        String redisKey = args[5];
        int filterValueLength = Integer.parseInt(args[6]);
        int cutValueLength = Integer.parseInt(args[7]);

        String sourcePath = String.format("hdfs://clusterA/itemcf/countStat/%s/%s/%s/%s",businessId,sceneId,itemSetId,today+"-"+dayNum);
        String redisKeysPath = String.format("hdfs://clusterA/itemcf/rediskey/%s/%s/%s/%s",businessId,sceneId,itemSetId,today+"-"+dayNum);
        String redisKeyValuePath = String.format("hdfs://clusterA/itemcf/rediskeyvalue/%s/%s/%s/%s",businessId,sceneId,itemSetId,today+"-"+dayNum);
        if(args.length >= 9){
            String fileNum = args[8];
            sourcePath = sourcePath+"/"+fileNum;
            redisKeysPath = redisKeysPath+"-onebyone" + "/"+fileNum;
            redisKeyValuePath = redisKeyValuePath + "-onebyone" + "/"+fileNum;
        }

        System.out.println("sourecePath:"+sourcePath);
        System.out.println("redisKeysPath:"+redisKeysPath);
        System.out.println("redisKeyValuePath:"+redisKeyValuePath);


        ParameterTool parameterTool = null;
        try {
            parameterTool = ParameterTool.fromPropertiesFile("./itemcf.properties");
        } catch (IOException e) {
            e.printStackTrace();
        }

        String redisClusterNodes = parameterTool.get("itemcf-redis-cluster-nodes");//"tencent-recom-rediscluster01:6380,tencent-recom-rediscluster02:6380,tencent-recom-rediscluster03:6380,tencent-recom-rediscluster04:6380,tencent-recom-rediscluster05:6380";
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
                String[] split = value.split(",");
                //过滤掉key结尾不为0的
//                if(!split[0].endsWith("0")){
//                    return;
//                }
                //过滤掉value size小于20的
                if(split.length<filterValueLength+1){
                    return;
                }
                //拼接redis key recom:item_764:p2p:1532901063
                String key = redisKey+ split[0].substring(1);//"recom:item_764:cf:" + split[0].substring(1); //recom:item_764:p2p:1000050399
                //对list size大于400的进行截断
                int length = cutValueLength > split.length - 1 ? split.length - 1 : cutValueLength;
                if (!(split.length - 1 > cutValueLength)) {
                    String lastValue = split[split.length - 1];
                    lastValue = lastValue.substring(0, lastValue.length() - 1);
                    split[split.length - 1] = lastValue;
                }
                String[] values = Arrays.copyOfRange(split, 1, length + 1);
                out.collect(new Tuple2<>(key, values));
            }
        });

        putToRedisDs.flatMap(new FlatMapFunction<Tuple2<String, String[]>, String>() {
            @Override
            public void flatMap(Tuple2<String, String[]> value, Collector<String> out) throws Exception {
                out.collect(value.f0);
            }
        }).writeAsText(redisKeysPath, FileSystem.WriteMode.OVERWRITE);

        putToRedisDs.writeAsText(redisKeyValuePath,FileSystem.WriteMode.OVERWRITE);

        putToRedisDs.setParallelism(1).output(new RedisOutputFormat(redisClusterNodes));


        try {
            env.execute("Hdfs To Redis");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
