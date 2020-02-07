package com.wufuqiang.statistics.utils;

import com.alibaba.fastjson.JSONObject;
import com.wufuqiang.commons.Constants;
import com.wufuqiang.statistics.entries.OfflineActionLog;
import com.wufuqiang.statistics.entries.OfflineNginxRecallLog;
import org.apache.commons.lang.StringUtils;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.fs.hdfs.HadoopFileSystem;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.Types;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import org.apache.flink.table.sinks.CsvTableSink;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.table.sources.CsvTableSource;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.text.DecimalFormat;

public class OfflineDataSetUtils {

    //空值的默认值
    private static String INVALID = "RECOMDASHBOARDINVALID";
    private static FileSystem fileSystem;
    private static Logger logger = LoggerFactory.getLogger(OfflineDataSetUtils.class);

    static {
        try {
            fileSystem = HadoopFileSystem.get(new URI("hdfs://clusterA:8020"));
        } catch (IOException e) {
            e.printStackTrace();
        } catch (URISyntaxException e) {
            e.printStackTrace();
        }
    }


    public static DataSet<String> readTextFile(ExecutionEnvironment env, String filePath) throws URISyntaxException, IOException {
        boolean exists = fileSystem.exists(new Path(filePath));
        System.out.println(filePath+",isExists:"+exists);
        DataSet<String> rawDs = null;
        if(exists){
            Configuration configuration = new Configuration();
            configuration.setBoolean("recursive.file.enumeration",true);
            rawDs = env.readTextFile(filePath).withParameters(configuration);
        }
        return rawDs;
    }

    public static DataSet<OfflineActionLog> getActionLogDs(DataSet<String> actionRawDs){
        //解析actionlog json,过滤掉不符合要求的，生成对应的OfflineActionLog对象，只取出sceneId,userId,itemId,action
        DataSet<OfflineActionLog> actionLogDs = actionRawDs.flatMap(new FlatMapFunction<String, OfflineActionLog>() {
            @Override
            public void flatMap(String value, Collector<OfflineActionLog> out) throws Exception {

                JSONObject jsonObject = null;
                try{
                    jsonObject = JSONObject.parseObject(value);
                }catch (Exception e){
                    logger.info("action error json:"+value);
                }

                if(jsonObject == null){
                    return ;
                }
                String userId = INVALID ;
                String itemId = INVALID ;
                String action = INVALID ;
                String contextExist = "0";
                String sceneId = INVALID;

                try{
                    contextExist = jsonObject.get("contextExist").toString();
                }catch(Exception e ){
                    e.printStackTrace();
                }
                if(!("1".equals(contextExist))){
                    return;
                }
                try{
                    action = jsonObject.get("action").toString();
                }catch(Exception e){
                    e.printStackTrace();
                }
                try{
                    userId = jsonObject.get("userId").toString();
                    itemId = jsonObject.get("itemId").toString();
                    sceneId = jsonObject.get("sceneId").toString();
                    out.collect(new OfflineActionLog(sceneId,userId,itemId,action));
                }catch(Exception e){
                    e.printStackTrace();
                }
            }
        });

        return actionLogDs;
    }

    public static DataSet<OfflineNginxRecallLog> getRecallLogDs(DataSet<String> nginxRecallRawDs){
        //解析recalllog json，只取出sceneId/userId
        DataSet<OfflineNginxRecallLog> nginxRecallDs = nginxRecallRawDs.flatMap(new FlatMapFunction<String, OfflineNginxRecallLog>() {
            @Override
            public void flatMap(String value, Collector<OfflineNginxRecallLog> out) throws Exception {
                JSONObject jsonObject = null;
                try{
                    jsonObject = JSONObject.parseObject(value);
                }catch(Exception e){
                    logger.info("recall json error:"+value);
                }

                if (jsonObject == null) {
                    return;
                }
                String userId = INVALID;
                String sceneId = INVALID;

                try {
                    long timestamp = (long)Double.parseDouble(jsonObject.get("timestamp").toString())*1000;
                    String request = jsonObject.get("request").toString();
                    request = request.substring(0, request.lastIndexOf(" "));
                    String[] firstSplits = request.split("\\?");
                    String[] splits = firstSplits[1].split("&");

                    for (String split : splits) {
                        if (split.startsWith("userID")) {
                            try {
                                int i = split.indexOf("=");
                                userId = split.substring(i+1);
                            } catch (Exception e) {
                                userId = INVALID;
                            }
                        } else if (split.startsWith("sceneID")) {
                            try {

                                int i = split.indexOf("=");
                                sceneId = split.substring(i+1).trim();
                            } catch (Exception e) {
                                sceneId =INVALID;
                            }
                        }
                    }
                    if(StringUtils.isNotBlank(sceneId) && StringUtils.isNotBlank(userId))
                        out.collect(new OfflineNginxRecallLog(sceneId, userId));
                } catch (Exception e) {
                    e.printStackTrace();
                }

            }
        });
        return nginxRecallDs;
    }

    public static DataSet<Tuple3<String, String, Double>> calcTransforErrorRatio(BatchTableEnvironment tableEnvironment,
                                                                               Table distinctActionLogTable,
                                                                               Table distinctNginxRecallLogTable,
                                                                               Table actionAllUvTable){


        //通过sceneId、userId将去重后的actionlog和nginxrecall进行join，并计算各个场景下的uv,用于计算用户行为传错率
        Table actionSceneIdUvTable = distinctActionLogTable.join(distinctNginxRecallLogTable)
                .where("actionSceneId=nRecallSceneId && actionUserId = nRecallUserId")
                .select("actionSceneId,actionUserId")
                .groupBy("actionSceneId")
                .select("actionSceneId,actionUserId.count as actionUv")
                .join(actionAllUvTable)
                .where("actionSceneId = actionAllSceneId")
                .select("actionSceneId,actionUv,actionAllUv");

        DataSet<Row> rowDataSet = tableEnvironment.toDataSet(actionSceneIdUvTable, Row.class);
        //用户行为传错率,1 - count(产生行为的用户list iner join 产生请求的用户list) / 行为总量(UV)
        DataSet<Tuple3<String, String, Double>> transforErrorRatioDs = rowDataSet.flatMap(new FlatMapFunction<Row, Tuple3<String, String, Double>>() {
            @Override
            public void flatMap(Row value, Collector<Tuple3<String, String, Double>> out) throws Exception {
                String sceneId = (String) value.getField(0);
                Long uv = (Long) value.getField(1);
                Long allActionUv = (Long)value.getField(2);
                double tranforErrorRatio = 0;
                if(allActionUv != 0){
                    tranforErrorRatio = 1 - 1.0 * uv / allActionUv;
                }
                if(tranforErrorRatio < 0.00001){
                    tranforErrorRatio = 0;
                }
                if (allActionUv != 0)
                    out.collect(new Tuple3<>(sceneId, "transforErrorRatio", tranforErrorRatio));

            }
        });

        return transforErrorRatioDs;
    }

    public static void cacheUserId(Table distinctActionLogTable,String filePath){
        TableSink sink = new CsvTableSink(filePath,"/",24, FileSystem.WriteMode.OVERWRITE);
        distinctActionLogTable.writeToSink(sink);
    }

    public static Table getUserId(BatchTableEnvironment tableEnvironment,String filePath) throws IOException {
        boolean exists = fileSystem.exists(new Path(filePath));
        Table actionUIdTable = null;
        if(exists){
            CsvTableSource csvTableSource = CsvTableSource.builder()
                    .path(filePath)
                    .field("yesSceneId", Types.STRING())
                    .field("yesUserId",Types.STRING())
                    .fieldDelimiter("/")
                    .build();

            actionUIdTable = tableEnvironment.fromTableSource(csvTableSource);
        }
        return actionUIdTable;
    }

    //用户波动率：1 - count(昨天产生行为的用户list iner join 今天产生行为的用户list ) / 行为总量(UV)
    public static DataSet<Tuple3<String, String, Double>> calcUserFluctuateRatio(BatchTableEnvironment tableEnvironment,
                                                                                 Table yesActionUIdTable,
                                                                                 Table todayActionUIdTable,
                                                                                 Table actionAllUvTable){
        if(yesActionUIdTable == null){
            return null;
        }
        Table yesterdayJoinTodayTable = yesActionUIdTable.join(todayActionUIdTable)
                .where("yesSceneId = actionSceneId && yesUserId = actionUserId")
                .groupBy("yesSceneId")
                .select("yesSceneId as sceneId,yesUserId.count as userIdCount")
                .leftOuterJoin(actionAllUvTable)
                .where("sceneId = actionSceneId")
                .select("sceneId,userIdCount,actionAllUv");

        DataSet<Row> yesterdayJoinTodayDs = tableEnvironment.toDataSet(yesterdayJoinTodayTable, Row.class);

        DataSet<Tuple3<String, String, Double>> userFluctuateRatioDs = yesterdayJoinTodayDs.flatMap(new FlatMapFunction<Row, Tuple3<String, String, Double>>() {
            @Override
            public void flatMap(Row value, Collector<Tuple3<String, String, Double>> out) throws Exception {
                String sceneId = (String) value.getField(0);
                Long userCount = (Long) value.getField(1);
                if (userCount == null) {
                    userCount = 0l;
                }
                Long actionAllUv = (Long) value.getField(2);

                if (actionAllUv != null) {
                    double userFluctuateRatio = 1 - 1.0 * userCount / actionAllUv;
                    out.collect(new Tuple3<>(sceneId, "userFluctuateRatio", userFluctuateRatio));
                }
            }
        });


        return userFluctuateRatioDs;
    }



    public static DataSet<Tuple5<String, String, Long, Long, Double>> calcUIdCTR(BatchTableEnvironment tableEnvironment,Table showDPShowTable,String field){
        Table showDPShowCountTable = showDPShowTable.groupBy(String.format("actionSceneId,%s,action",field))
                .select(String.format("actionSceneId,%s,action,action.count as actionCount",field));

        DataSet<Row> showDPShowCountDs = tableEnvironment.toDataSet(showDPShowCountTable, Row.class);
        DataSet<Tuple5<String, String, Long, Long, Double>> userIdCTRDs = showDPShowCountDs.groupBy(0, 1).reduceGroup(new GroupReduceFunction<Row, Tuple5<String, String, Long, Long, Double>>() {
            @Override
            public void reduce(Iterable<Row> values, Collector<Tuple5<String, String, Long, Long, Double>> out) throws Exception {
                String sceneId = "";
                String userId = "";
                String action = "";
                Long showCount = 0l;
                Long dpshowCount = 0l;
                for (Row value : values) {
                    sceneId = (String) value.getField(0);
                    userId = (String) value.getField(1);
                    action = (String) value.getField(2);
                    Long count = (Long) value.getField(3);
                    if (count == null) {
                        count = 0l;
                    }
                    if ("show".equals(action)) {
                        showCount = count;
                    } else if ("detailPageShow".equals(action)) {
                        dpshowCount = count;
                    }
                }

                Double ctr = 0.0;
                if (showCount == 0) {
                    ctr = Double.MAX_VALUE;
                } else {
                    ctr = 1.0 * dpshowCount / showCount;
                }
                out.collect(new Tuple5<>(sceneId, userId, showCount, dpshowCount, ctr));
            }
        });

        return userIdCTRDs;
    }

    public static Table calcUIdCTRAbnormal(BatchTableEnvironment tableEnvironment,Table showDPShowTable,double threshold,String field){

        DataSet<Tuple5<String, String, Long, Long, Double>> userIdCTRDs = calcUIdCTR(tableEnvironment,showDPShowTable,field);
        DataSet<Tuple5<String, String, Long, Long, Double>> userIdCRTAbnormalDs = userIdCTRDs.filter(new FilterFunction<Tuple5<String, String, Long, Long, Double>>() {
            @Override
            public boolean filter(Tuple5<String, String, Long, Long, Double> value) throws Exception {
                if (value.f4 > threshold) {
                    return true;
                }
                return false;
            }
        });


//        tableEnvironment.registerDataSet("UserIdCRTAbnormal",userIdCRTAbnormalDs);
        tableEnvironment.registerDataSet(field,userIdCRTAbnormalDs);
        Table userIdCRTAbnormalTable = tableEnvironment.scan(field).as("sceneId,userId,showCount,dpshowCount,ctr");
        return userIdCRTAbnormalTable.groupBy("sceneId").select("sceneId,userId.count as userIdCount,dpshowCount.sum as dpshowSum");
    }

    public static DataSet<Tuple3<String,String,Double>> calcAbnorCRTUserRatio(BatchTableEnvironment tableEnvironment,
                                                                              Table userIdCRTAbnormalTable,
                                                                              Table actionAllUvTable,
                                                                              String label){

        Table joinUserCountAllUvTable = userIdCRTAbnormalTable.join(actionAllUvTable)
                .where("sceneId = actionAllSceneId")
                .select("sceneId,userIdCount,actionAllUv");

        DataSet<Row> joinUserCountAllUvDs = tableEnvironment.toDataSet(joinUserCountAllUvTable, Row.class);
        return joinUserCountAllUvDs.map(new MapFunction<Row, Tuple3<String,String,Double>>() {
            @Override
            public Tuple3<String, String, Double> map(Row value) throws Exception {
                String sceneId = (String)value.getField(0);
                Long userIdCount = (Long)value.getField(1);
                Long actionAllUv = (Long)value.getField(2);
                double ratio = 0.0;
                if(actionAllUv == null){
                    ratio = 10;
                }else{
                    ratio = userIdCount/actionAllUv;
                }
                return new Tuple3<>(sceneId,label,ratio);
            }
        });

    }

    public static DataSet<Tuple3<String,String,Double>> calcAbnormalUserClickRatio(BatchTableEnvironment tableEnvironment,
                                                                                   Table userIdCRTAbnormalTable,
                                                                                   Table actionAllDpshowPvTable,
                                                                                   String label){

        Table joinDpshowTable = userIdCRTAbnormalTable.join(actionAllDpshowPvTable)
                .where("sceneId = actionSceneId")
                .select("sceneId,dpshowSum,dpshowCount");

        DataSet<Row> joinDpshowDs = tableEnvironment.toDataSet(joinDpshowTable, Row.class);
        return joinDpshowDs.map(new MapFunction<Row, Tuple3<String,String,Double>>() {
            @Override
            public Tuple3<String, String, Double> map(Row value) throws Exception {

                String sceneId = (String)value.getField(0);
                Long userIdCount = (Long)value.getField(1);
                Long actionAllUv = (Long)value.getField(2);
                double ratio = 0.0;
                if(actionAllUv == null){
                    ratio = 10;
                }else{
                    ratio = userIdCount/actionAllUv;
                }
                return new Tuple3<>(sceneId,label,ratio);
            }
        });
    }


    public static DataSet<Tuple3<String,String, String>> unionAllResult(DataSet<Tuple3<String,String, Double>> unionDs){
        DecimalFormat df = new DecimalFormat("0.00000");
        DataSet<Tuple3<String,String, String>> resultDs = unionDs.groupBy(0).reduceGroup(new GroupReduceFunction<Tuple3<String, String, Double>, Tuple3<String,String, String>>() {
            @Override
            public void reduce(Iterable<Tuple3<String, String, Double>> values, Collector<Tuple3<String,String, String>> out) throws Exception {

                String[] stats = new String[]{"0", "0", "0", "0", "0", "0"};
                String sceneId = "";
                for (Tuple3<String, String, Double> value : values) {
                    sceneId = value.f0;
                    if ("transforErrorRatio".equals(value.f1)) {
                        stats[0] = df.format(value.f2);
                    } else if ("abnormalUserRatio".equals(value.f1)) {
                        stats[1] = df.format(value.f2);
                    } else if ("abnormalUserClickRatio".equals(value.f1)) {
                        stats[2] = df.format(value.f2);
                    } else if ("abnormalItemRatio".equals(value.f1)) {
                        stats[3] = df.format(value.f2);
                    } else if ("abnormalItemClickRatio".equals(value.f1)) {
                        stats[4] = df.format(value.f2);
                    }else if("userFluctuateRatio".equals(value.f1)){
                        stats[5] = df.format(value.f2);
                    }
                }
                out.collect(new Tuple3<>(sceneId,"full", String.join(Constants.SPLIT, stats)));
            }
        });

        return resultDs;
    }

}
