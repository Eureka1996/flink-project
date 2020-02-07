package com.wufuqiang.itemcf.utils;

import com.alibaba.fastjson.JSONObject;
import com.wufuqiang.itemcf.entries.PetalI2IRelation;
import com.wufuqiang.itemcf.entries.PetalItem;
import org.apache.flink.api.common.functions.*;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.fs.hdfs.HadoopFileSystem;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import org.apache.flink.util.Collector;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class PetalDataSetUtils {


    public static DataSet<String> readTextFile(ExecutionEnvironment env ,
                                               String businessIdStr,
                                               String sceneIdStr,
                                               String itemSetIdStr,
                                               String whichday,
                                               String numDayStr) throws ParseException {

        String businessIdStrs[] = businessIdStr.split("-");
        String sceneIdStrs[] = sceneIdStr.split("-");
        String itemSetIdStrs[] = itemSetIdStr.split("-");

        int numDay = Integer.parseInt(numDayStr);

        FileSystem fileSystem = null;
        try {
            fileSystem = HadoopFileSystem.get(new URI("hdfs://clusterA:8020"));
        } catch (IOException e) {
            e.printStackTrace();
        } catch (URISyntaxException e) {
            e.printStackTrace();
        }

        String pathFormat = "hdfs://clusterA:8020/recom/action/%s/%s/%s/%s";
        int sceneIdNums = sceneIdStrs.length;
        DataSet<String> rawDs = null;
        //读取数据，合并成一个DataSet<String>
        for(int i = 0 ; i < sceneIdNums;i++){
            String businessId = businessIdStrs[i];
            String sceneId = sceneIdStrs[i];
            String itemSetId = itemSetIdStrs[i];
            String today = whichday;
            for(int j = 0 ; j<numDay;j++){
                String actionPathStr = String.format(pathFormat,businessId,sceneId,itemSetId,today);
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
                today = TimeUtils.getYesterday(today);
            }
        }
        return rawDs;
    }

    /**
     * 获取审核通过的itemid
     * @param env
     * @param filePath:审核图片的存放路径
     * @param field:要提取的itemid字段
     * @return
     */
    public static DataSet<Tuple1<String>> getCensored(ExecutionEnvironment env , String filePath, String field){
        DataSet<String> censoredPinsRawDs = env.readTextFile(filePath);
        DataSet<Tuple1<String>> censoredPinsDs = censoredPinsRawDs.flatMap(new FlatMapFunction<String, Tuple1<String>>() {
            @Override
            public void flatMap(String value, Collector<Tuple1<String>> out) throws Exception {
                JSONObject jsonObject = JSONObject.parseObject(value);
                //{"pinId":"819440383","fileId":"272114"}
                String pinId = jsonObject.get(field).toString();
                out.collect(new Tuple1<>(pinId));
            }
        });
        return censoredPinsDs;
    }

    /**
     * 读取action日志，解析成PetalItem
     * @param rawDs:Source
     * @return
     */
    public static DataSet<PetalItem> getPetalItemDs(DataSet<String> rawDs){
        DataSet<PetalItem> petalItemDs = rawDs.flatMap(new FlatMapFunction<String, PetalItem>() {
            @Override
            public void flatMap(String value, Collector<PetalItem> out) throws Exception {
                try {
                    JSONObject jsonObject = JSONObject.parseObject(value);
                    String action = "";
                    String userId = "";
                    String itemId = "";
                    long actionTime = 0l;
                    action = jsonObject.get("action") == null ? "" : jsonObject.get("action").toString();
                    if (!"detailPageShow".equals(action)) {
                        return;
                    }
                    userId = jsonObject.get("userId") == null ? "" : jsonObject.get("userId").toString();
                    itemId = jsonObject.get("itemId") == null ? "" : jsonObject.get("itemId").toString();
                    actionTime =  jsonObject.get("actionTime") == null ? 0l : (long)Double.parseDouble(jsonObject.get("actionTime").toString());
                    if (!("".equals(userId) || "".equals(itemId))) {
                        out.collect(new PetalItem(userId, itemId,actionTime));
                    }
                } catch (Exception e) {
                    System.out.println("json error:" + value);
                    e.printStackTrace();
                }
            }
        }).distinct();
        return petalItemDs;
    }

    public static DataSet<PetalItem> putCensoredFlag(DataSet<PetalItem> petalItemDs , DataSet<Tuple1<String>> censoredDs){
        DataSet<PetalItem> censoredFlagDs = petalItemDs.leftOuterJoin(censoredDs).where("itemId").equalTo(0).with(new JoinFunction<PetalItem, Tuple1<String>, PetalItem>() {
            @Override
            public PetalItem join(PetalItem first, Tuple1<String> second) throws Exception {
                if (second != null && second.f0 !=null) {
                    first.setFlag(1);
                }
                return first;
            }
        });
        return censoredFlagDs;
    }

    /**
     * 计算每个itemid-itemid的共现值
     * @param userItemDs：每一个userId,itemId
     * @return itemid-itemid-score(共现值)
     */
    public static DataSet<PetalI2IRelation> getAppearValue(DataSet<PetalItem> userItemDs){
        DataSet<PetalI2IRelation> i2iRelationDs = userItemDs.groupBy("userId").reduceGroup(new GroupReduceFunction<PetalItem, Tuple4<String, String, Integer, Double>>() {
            @Override
            public void reduce(Iterable<PetalItem> values, Collector<Tuple4<String, String, Integer, Double>> out) throws Exception {
                List<Tuple2<String, Integer>> itemId = new ArrayList<>();
                for (PetalItem value : values) {
                    itemId.add(new Tuple2<>(value.getItemId(), value.getFlag()));
                }
                for (int i = 0; i < itemId.size(); i++) {
                    for (int j = 0; j < itemId.size(); j++) {
                        Tuple2<String, Integer> oneItemId = itemId.get(i);
                        Tuple2<String, Integer> anotherItemId = itemId.get(j);
                        if (!oneItemId.f0.equals(anotherItemId.f0)) {
                            out.collect(new Tuple4<>(oneItemId.f0, anotherItemId.f0, anotherItemId.f1, 1.0));
                        }
                    }
                }
            }
        }).groupBy(0, 1, 2).sum(3).map(new MapFunction<Tuple4<String, String, Integer, Double>, PetalI2IRelation>() {
            @Override
            public PetalI2IRelation map(Tuple4<String, String, Integer, Double> value) throws Exception {
                return new PetalI2IRelation(value.f0, value.f1, value.f3, value.f2);
            }
        });
        return i2iRelationDs;

    }

    public static DataSet<PetalI2IRelation> getAppearValueWithDistance(DataSet<PetalItem> userItemDs,int dis){

        DataSet<PetalI2IRelation> appearValueDs = userItemDs.groupBy("userId")
                .sortGroup("actionTime", Order.ASCENDING)
                .reduceGroup(new GroupReduceFunction<PetalItem, Tuple4<String, String, Integer, Double>>() {
                    @Override
                    public void reduce(Iterable<PetalItem> values, Collector<Tuple4<String, String, Integer, Double>> out) throws Exception {
                        List<Tuple2<String, Integer>> itemId = new ArrayList<>();
                        for (PetalItem value : values) {
                            itemId.add(new Tuple2<>(value.getItemId(), value.getFlag()));
                        }
                        int len = itemId.size();
                        Map<String, Double[]> itemIdScoreMap = new HashMap<>();
                        for (int i = 0; i < len - 1; i++) {
                            for (int j = i + 1; j < len; j++) {
                                Tuple2<String, Integer> oneItemId = itemId.get(i);
                                Tuple2<String, Integer> anotherItemId = itemId.get(j);
                                double score = 0;
                                if (!oneItemId.f0.equals(anotherItemId.f0)) {
                                    int distance = j - i;
                                    if (distance <= dis) {
                                        score = 1;
                                    } else {
                                        score = Math.exp(1.0 * (dis - distance) / (2 * dis + 1));
                                    }
                                    String unionItemId = String.format("%s_%s", oneItemId.f0, anotherItemId.f0);
                                    boolean isContained = itemIdScoreMap.containsKey(unionItemId);
                                    if (isContained) {
                                        Double scoreArrTmp[] = itemIdScoreMap.get(unionItemId);
                                        if (scoreArrTmp[0] < score) {
                                            scoreArrTmp[0] = score;
                                            out.collect(new Tuple4<>(oneItemId.f0, anotherItemId.f0, anotherItemId.f1, score));
                                        }
                                        if (scoreArrTmp[1] < score) {
                                            scoreArrTmp[1] = score;
                                            out.collect(new Tuple4<>(anotherItemId.f0, oneItemId.f0, oneItemId.f1, score));
                                        }
                                    } else {
                                        itemIdScoreMap.put(unionItemId, new Double[]{score, score});
                                        out.collect(new Tuple4<>(oneItemId.f0, anotherItemId.f0, anotherItemId.f1, score));
                                        out.collect(new Tuple4<>(anotherItemId.f0, oneItemId.f0, oneItemId.f1, score));
                                    }
                                }
                            }
                        }
                    }
                }).groupBy(0, 1, 2).
                        sum(3)
                .map(new MapFunction<Tuple4<String, String, Integer, Double>, PetalI2IRelation>() {
                    @Override
                    public PetalI2IRelation map(Tuple4<String, String, Integer, Double> value) throws Exception {
                        return new PetalI2IRelation(value.f0, value.f1, value.f3, value.f2);
                    }
                });

        return appearValueDs;
    }

    public static DataSet<PetalI2IRelation> calcScore(ExecutionEnvironment env ,DataSet<PetalI2IRelation> itemItemScoreDs,DataSet<PetalItem> userItemDs){

        BatchTableEnvironment tableEnvironment = TableEnvironment.getTableEnvironment(env);
        //获取每个itemid被多少个userid点击过
        tableEnvironment.registerDataSet("UserItemTable",userItemDs);
        Table userItemTable = tableEnvironment.scan("UserItemTable");
        Table itemUserCountTable = userItemTable.groupBy("itemId").select("itemId,userId.count as userIdCount");

        tableEnvironment.registerDataSet("ItemItemScoreTable",itemItemScoreDs);
        Table itemItemScoreTable = tableEnvironment.scan("ItemItemScoreTable");

        Table unionTable = itemItemScoreTable.join(itemUserCountTable)
                .where("itemIdOne = itemId")
                .select("itemIdOne,itemIdOther,appearValue,userIdCount as userCountOne,flag,score")
                .join(itemUserCountTable)
                .where("itemIdOther = itemId")
                .select("itemIdOne,itemIdOther,appearValue,userCountOne,userIdCount as userCountOther,flag,score");

        DataSet<PetalI2IRelation> unionDataSet = tableEnvironment.toDataSet(unionTable, PetalI2IRelation.class);
        DataSet<PetalI2IRelation> scoreDataSet = unionDataSet.map(new MapFunction<PetalI2IRelation, PetalI2IRelation>() {
            @Override
            public PetalI2IRelation map(PetalI2IRelation value) throws Exception {
                double score = 1.0 * value.getAppearValue() / Math.sqrt(1.0 * value.getUserCountOne() * value.getUserCountOther());
                value.setScore(score);
                return value;
            }
        });
        return scoreDataSet;
    }

    /**
     * 过滤掉不在审核范围内的itemid
     * @param scoreDataSet
     * @return
     */
    public static DataSet<PetalI2IRelation> filterOut(DataSet<PetalI2IRelation> scoreDataSet){
        return scoreDataSet.filter(new FilterFunction<PetalI2IRelation>() {
            @Override
            public boolean filter(PetalI2IRelation value) throws Exception {
                if(value.getFlag() == 1){
                    return true;
                }
                return false;
            }
        });
    }

    public static DataSet<Tuple2<String, Integer>> classifyValueSize(DataSet<Tuple3<String, String, Integer>> itemValueSizeDs){
        DataSet<Tuple2<String, Integer>> classfiedValueSizeDs = itemValueSizeDs.flatMap(new FlatMapFunction<Tuple3<String, String, Integer>, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(Tuple3<String, String, Integer> value, Collector<Tuple2<String, Integer>> out) throws Exception {
                int size = value.f2;
                if (size >= 400) {
                    out.collect(new Tuple2<>("400+", 1));
                } else if (size >= 300) {
                    out.collect(new Tuple2<>("300+", 1));
                } else if (size >= 200) {
                    out.collect(new Tuple2<>("200+", 1));
                } else if (size >= 100) {
                    out.collect(new Tuple2<>("100+", 1));
                } else if (size >= 90) {
                    out.collect(new Tuple2<>("90+", 1));
                } else if (size >= 80) {
                    out.collect(new Tuple2<>("80+", 1));
                } else if (size >= 70) {
                    out.collect(new Tuple2<>("70+", 1));
                } else if (size >= 60) {
                    out.collect(new Tuple2<>("60+", 1));
                } else if (size >= 50) {
                    out.collect(new Tuple2<>("50+", 1));
                } else if (size >= 40) {
                    out.collect(new Tuple2<>("40+", 1));
                } else if (size >= 30) {
                    out.collect(new Tuple2<>("30+", 1));
                } else if (size >= 20) {
                    out.collect(new Tuple2<>("20+", 1));
                } else if (size >= 10) {
                    out.collect(new Tuple2<>("10+", 1));
                } else if (size >= 0) {
                    out.collect(new Tuple2<>("00+", 1));
                } else {
                    out.collect(new Tuple2<>("00-", 1));
                }
            }
        }).groupBy(0).sum(1);

        return classfiedValueSizeDs;
    }



}
