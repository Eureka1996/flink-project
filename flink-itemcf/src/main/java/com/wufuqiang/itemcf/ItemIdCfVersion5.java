package com.wufuqiang.itemcf;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.fs.hdfs.HadoopFileSystem;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * 不进行任何过滤，对数据进行全量的计算
 * 根据距离判定共现值的大小
 */
public class ItemIdCfVersion5 {

    public static void main(String[] args) throws ParseException {
        String propertiesPathStr = args[0];
        ParameterTool parameterTool = null;
        try {
            parameterTool = ParameterTool.fromPropertiesFile("./"+propertiesPathStr);
        } catch (IOException e) {
            e.printStackTrace();
        }
        String businessIdStr = parameterTool.get("itemcf.businessids");
        String sceneIdStr = parameterTool.get("itemcf.sceneids");
        String itemSetIdStr = parameterTool.get("itemcf.itemsetids");
        String numDayStr = parameterTool.get("itemcf.day.numbers");
        int n = Integer.parseInt(parameterTool.get("itemcf.itemid.distances"));
        String datasOutputPrefix = parameterTool.get("itemcf.datas.outputpath.prefix");
        String subregionOutputPrefix=parameterTool.get("itemcf.datas.subregion.outputpath.prefix");
        String todayStr = parameterTool.get("itemcf.day.begin");
        if("yesterday".equals(todayStr)){
            Date current = new Date();
            SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
            String now = sdf.format(current);
            todayStr = getYesterday(now);
        }

        String businessIdStrs[] = businessIdStr.split(",");
        String sceneIdStrs[] = sceneIdStr.split(",");
        String itemSetIdStrs[] = itemSetIdStr.split(",");


        int numDay = Integer.parseInt(numDayStr);
        String outputPathStr = String.format("%s/%s/%s/%s/%s",datasOutputPrefix,businessIdStrs[0],sceneIdStrs[0],itemSetIdStrs[0],todayStr+"-"+numDayStr);
        String quDuanOutputStr = String.format("%s/%s/%s/%s/%s",subregionOutputPrefix,businessIdStrs[0],sceneIdStrs[0],itemSetIdStrs[0],todayStr+"-"+numDayStr);
        System.out.println("data outputPath:"+outputPathStr);
        System.out.println("subregion outputStr:"+quDuanOutputStr);

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        BatchTableEnvironment tableEnvironment = TableEnvironment.getTableEnvironment(env);

        DataSet<String> rawDs = null;

        FileSystem fileSystem = null;
        String uri = "hdfs://clusterA:8020";
        try {
            fileSystem = HadoopFileSystem.get(new URI(uri));
        } catch (IOException e) {
            e.printStackTrace();
        } catch (URISyntaxException e) {
            e.printStackTrace();
        }
        String pathFormat = "%s/recom/action/%s/%s/%s/%s";

        int sceneIdNums = sceneIdStrs.length;

        //读取数据，合并成一个DataSet<String>
        for(int i = 0; i < sceneIdNums; i++){
            String businessId = businessIdStrs[i];
            String sceneId = sceneIdStrs[i];
            String itemSetId = itemSetIdStrs[i];
            String today = todayStr;
            for(int j = 0 ; j<numDay;j++){
                String actionPathStr = String.format(pathFormat,uri,businessId,sceneId,itemSetId,today);
                try {
                    boolean exists = fileSystem.exists(new Path(actionPathStr));
                    System.out.println(actionPathStr+",isExists:"+exists);
                    if(exists){
                        DataSet<String> oneActionRawDs = env.readTextFile(actionPathStr);
                        if(rawDs == null){
                            rawDs = oneActionRawDs;
                        }else{
                            rawDs = rawDs.union(oneActionRawDs);
                        }
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                }
                today = getYesterday(today);
            }
        }


        if(rawDs == null){
            System.out.println("rawDs is null.");
            return;
        }

        //将json解析,取出userId,itemId,actionTime，并去重
        DataSet<Tuple3<String, String,Long>> userIdItemIdDs = rawDs.flatMap(new FlatMapFunction<String, Tuple3<String, String,Long>>() {
            @Override
            public void flatMap(String value, Collector<Tuple3<String, String,Long>> out) throws Exception {
                try{
                    JSONObject jsonObject = JSONObject.parseObject(value);
                    String action = "";
                    String userId = "";
                    String itemId = "";
                    long actionTime = 0l;
                    action = jsonObject.get("action") == null ? "" : jsonObject.get("action").toString();
                    if(!"detailPageShow".equals(action)){
                        return;
                    }
                    userId = jsonObject.get("userId") == null ? "" : jsonObject.get("userId").toString();
                    itemId = jsonObject.get("itemId") == null ? "" : jsonObject.get("itemId").toString();
                    actionTime =  jsonObject.get("actionTime") == null ? 0l : (long)Double.parseDouble(jsonObject.get("actionTime").toString());
                    if(!("".equals(userId) || "".equals(itemId))){
                        out.collect(new Tuple3<>(userId,itemId,actionTime));
                    }
                }catch (Exception e){
                    System.out.println("json error:"+ value);
                    e.printStackTrace();
                }
            }
        }).distinct();

        //通过userId进行分组，并进行itemId的两两组合，形成itemId-itemId-1组合。
        //通过itemId-itemId进行分组，并进行sum计算，即共现值
        DataSet<Tuple3<String, String, Double>> itemItemScoreDs = userIdItemIdDs.groupBy(0).sortGroup(2,Order.ASCENDING).reduceGroup(new GroupReduceFunction<Tuple3<String,String,Long>, Tuple3<String, String, Double>>() {
            @Override
            public void reduce(Iterable<Tuple3<String,String,Long>> values, Collector<Tuple3<String, String, Double>> out) throws Exception {
                List<String> itemId = new ArrayList<String>();
                for (Tuple3<String,String,Long> value : values) {
                    itemId.add(value.f1);
                }
                int len = itemId.size();
                Map<String,Double[]> itemIdScoreMap = new HashMap<String,Double[]>();
                for (int i = 0; i < len-1 ; i++) {
                    for (int j = i+1; j < len; j++) {
                        String oneItemId = itemId.get(i);
                        String anotherItemId = itemId.get(j);
                        double score = 0;
                        if(!oneItemId.equals(anotherItemId)){
                            int distance = j-i;
                            if(distance <= n){
                                score = 1;//out.collect(new Tuple3<>(oneItemId, anotherItemId, 1));
                            }else{
                                score = Math.exp(1.0*(n-distance)/(2*n+1));//out.collect(new Tuple3<>(oneItemId,another))
                            }
                            String unionItemId = oneItemId + "_" + anotherItemId;

                            boolean isContained = itemIdScoreMap.containsKey(unionItemId);
                            if(isContained){
                                Double scoreArrTmp[] = itemIdScoreMap.get(unionItemId);
                                if(scoreArrTmp[0] < score){
                                    scoreArrTmp[0] = score;
                                    out.collect(new Tuple3<>(oneItemId,anotherItemId,score));
                                }
                                if(scoreArrTmp[1] < score){
                                    scoreArrTmp[1] = score;
                                    out.collect(new Tuple3<>(anotherItemId,oneItemId,score));
                                }

                            }else{
                                itemIdScoreMap.put(unionItemId,new Double[]{score,score});
                                out.collect(new Tuple3<>(oneItemId,anotherItemId,score));
                                out.collect(new Tuple3<>(anotherItemId,oneItemId,score));
                            }
                        }
                    }
                }
            }

        }).groupBy(0, 1).sum(2);

        tableEnvironment.registerDataSet("UserItemTable",userIdItemIdDs);
        tableEnvironment.registerDataSet("ItemItemScoreTable",itemItemScoreDs);
        Table userItemTable = tableEnvironment.scan("UserItemTable").as("userId,itemId");
        Table itemItemScoreTable = tableEnvironment.scan("ItemItemScoreTable").as("oneItemId,anotherItemId,togetherShowCount");
        Table itemUserCountTable = userItemTable.groupBy("itemId").select("itemId,userId.count as userIdCount");
        Table unionTable = itemItemScoreTable.join(itemUserCountTable)
                .where("oneItemId = itemId")
                .select("oneItemId,anotherItemId,togetherShowCount,userIdCount as firstCount")
                .join(itemUserCountTable)
                .where("anotherItemId = itemId")
                .select("oneItemId,anotherItemId,togetherShowCount,firstCount,userIdCount as secondCount");

        DataSet<Row> unionDataSet = tableEnvironment.toDataSet(unionTable, Row.class);

        DataSet<Tuple3<String, String, Double>> resultScoreDs = unionDataSet.flatMap(new FlatMapFunction<Row, Tuple3<String, String, Double>>() {
            @Override
            public void flatMap(Row value, Collector<Tuple3<String, String, Double>> out) throws Exception {
                String oneItemId = (String) value.getField(0);
                String anotherItemId = (String)value.getField(1);
                double togetherShowCount = (double)value.getField(2);
                long firstCount = (long)value.getField(3);
                long secondCount = (long)value.getField(4);
                double score = 1.0*togetherShowCount/Math.sqrt(1.0*firstCount*secondCount);
                out.collect(new Tuple3<>(oneItemId,anotherItemId,score));
            }
        });

        DataSet<Tuple3<String, String, Integer>> itemValueSizeDs = resultScoreDs.groupBy(0).sortGroup(2, Order.DESCENDING).reduceGroup(new GroupReduceFunction<Tuple3<String, String, Double>, Tuple3<String, String, Integer>>() {
            @Override
            public void reduce(Iterable<Tuple3<String, String, Double>> values, Collector<Tuple3<String, String, Integer>> out) throws Exception {
                String oneItemId = "";
                boolean flag = true;
                List<String> itemScore = new ArrayList<>();
                for (Tuple3<String, String, Double> value : values) {

                    if (flag) {
                        oneItemId = value.f0;
                        flag = false;
                    }
                    String anotherItemId = value.f1;
                    String score = value.f2.toString();
                    itemScore.add(anotherItemId + ":" + score);
                }
                out.collect(new Tuple3<>(oneItemId, String.join(",", itemScore), itemScore.size()));

            }
        });

        itemValueSizeDs.flatMap(new FlatMapFunction<Tuple3<String, String, Integer>, Tuple2<String,Integer>>() {
            @Override
            public void flatMap(Tuple3<String, String, Integer> value, Collector<Tuple2<String, Integer>> out) throws Exception {
                int size = value.f2;
                if(size>=400){
                    out.collect(new Tuple2<>("400+",1));
                }else if(size>=300){
                    out.collect(new Tuple2<>("300+",1));
                }else if(size>=200){
                    out.collect(new Tuple2<>("200+",1));
                }else if(size>=100){
                    out.collect(new Tuple2<>("100+",1));
                }else if(size>=90){
                    out.collect(new Tuple2<>("90+",1));
                }else if(size>=80){
                    out.collect(new Tuple2<>("80+",1));
                }else if(size>=70){
                    out.collect(new Tuple2<>("70+",1));
                }else if(size>=60){
                    out.collect(new Tuple2<>("60+",1));
                }else if(size>=50){
                    out.collect(new Tuple2<>("50+",1));
                }else if(size>=40){
                    out.collect(new Tuple2<>("40+",1));
                }else if(size>=30){
                    out.collect(new Tuple2<>("30+",1));
                }else if(size>=20){
                    out.collect(new Tuple2<>("20+",1));
                }else if(size>=10){
                    out.collect(new Tuple2<>("10+",1));
                }else if(size>=0){
                    out.collect(new Tuple2<>("00+",1));
                }else {
                    out.collect(new Tuple2<>("00-",1));
                }
            }
        }).groupBy(0).sum(1).writeAsText(quDuanOutputStr, FileSystem.WriteMode.OVERWRITE);

        itemValueSizeDs.flatMap(new FlatMapFunction<Tuple3<String, String, Integer>, String>() {
            @Override
            public void flatMap(Tuple3<String, String, Integer> value, Collector<String> out) throws Exception {
                out.collect(String.format("%s_%s",value.f0,value.f1));
            }
        }).writeAsText(outputPathStr, FileSystem.WriteMode.OVERWRITE);


        try {
            env.execute("ItemIdCf");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static String getYesterday(String today) throws ParseException {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
        Date todayDay = sdf.parse(today);
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(todayDay);
        calendar.add(Calendar.DAY_OF_MONTH,-1);
        Date yesterdayDay = calendar.getTime();
        String yesterday = sdf.format(yesterdayDay);
        return yesterday;
    }
}
