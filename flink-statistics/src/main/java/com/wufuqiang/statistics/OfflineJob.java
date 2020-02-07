package com.wufuqiang.statistics;

import com._4paradigm.data.Constants;
import com._4paradigm.enties.OfflineActionLog;
import com._4paradigm.enties.OfflineNginxRecallLog;
import com._4paradigm.output.HBaseOutputFormat;
import com._4paradigm.snappy.HdfsSnappyFileInputFormat;
import com._4paradigm.utils.HBaseUtils;
import com._4paradigm.utils.PropertiesUtil;
import com.alibaba.fastjson.JSONObject;
import org.apache.commons.lang.StringUtils;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.fs.hdfs.HadoopFileSystem;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.Types;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import org.apache.flink.table.sinks.CsvTableSink;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.table.sources.CsvTableSource;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class OfflineJob {
    public static void main(String[] args) throws Exception {

        //传入两个参数，第一个表示天数，20190828，第二个表示小时，16
        if(args.length <= 0 || args.length>2){
            System.out.println("传入参数个数不正确。");
            return ;
        }

        String time = args[0];
        String tablename = PropertiesUtil.RECOM_DASHBOARD_OFFLINE_HOUR;
        String clickUvTablename = PropertiesUtil.RECOM_DASHBOARD_HOUR;
        String resultTimePath = "";

        //标记是不是天的计算
        boolean isDaylyCalc = false;

        //由于nginx-recall的同步数据没有进行小时的拆分。这里用开始时间和结束时间进行过滤
        long startTime = 0;
        long endTime = 0;

        long yesterday = 0;
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHH");
        //如果只有一个天的参数，就是天的计算
        if(args.length == 1){
            isDaylyCalc = true;
            tablename = PropertiesUtil.RECOM_DASHBOARD_OFFLINE_DAY;
            clickUvTablename = PropertiesUtil.RECOM_DASHBOARD_DAY;
            resultTimePath = time;
            time = time +"00";
            startTime = sdf.parse(time).getTime();
            endTime = startTime+24*60*60*1000;
            yesterday = startTime - 24*60*60*1000;
        }else{
            time = time + String.format("%02d",Integer.valueOf(args[1]));
            resultTimePath = time;
            startTime = sdf.parse(time).getTime();
            endTime = startTime+60*60*1000;
        }

        final long startTimestamp = startTime;
        final long endTimestamp = endTime;

        //创建hdfs文件系统FileSystem
        FileSystem fileSystem = null;
        String uri = "hdfs://clusterA:8020";//FileSystem.getDefaultFsUri().toString();
        try{
//            fileSystem = HadoopFileSystem.get(FileSystem.getDefaultFsUri());
            fileSystem = HadoopFileSystem.get(new URI(uri));
        }catch(Exception e){
            e.printStackTrace();
        }

        Logger logger = LoggerFactory.getLogger(OfflineJob.class);
        DecimalFormat df = new DecimalFormat("0.00000");

        //对应场景下的detailPageShow和uv,直接从hbase中取出（流式任务已经计算好）
        Map<String,long[]> actionClickUv = HBaseUtils.getInstance().getActionClickUv(clickUvTablename,startTimestamp);
        System.out.println("actionClickUv size:" + actionClickUv.size());

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        //设置重启机制，但好像并没有起作用
        env.setRestartStrategy(RestartStrategies.failureRateRestart(
                3,//一个时间段内的最大失败次数
                Time.of(5, TimeUnit.MINUTES), // 衡量失败次数的是时间段
                Time.of(10, TimeUnit.SECONDS) // 间隔
        ));
        BatchTableEnvironment tableEnvironment = TableEnvironment.getTableEnvironment(env);

        //actionlog在hdfs上的路径格式
        String actionlogformat = "%s/camus/action/action/%s/%02d/";
        //recalllog在hdfs上的路径格式
        String nginxrecallformat = "%s/camus/nginx_recall/nginx-recom-recall-log/%s/";

        DataSet<String> actionRawDs = null;

        //读取hdfs中actionlog
        if(isDaylyCalc){
            for(int i = 0; i < 24 ; i++){
                String actionlogstr = String.format(actionlogformat,uri,args[0],i);
                boolean actionlogExist = false;
                actionlogExist = fileSystem.exists(new Path(actionlogstr));
                if(actionlogExist){
                    System.out.println("actionlog path "+i+" :"+actionlogstr);
                    DataSet<String> oneActionDs = env.readFile(new HdfsSnappyFileInputFormat(new Path(actionlogstr)),actionlogstr);
                    if(actionRawDs == null){
                        actionRawDs = oneActionDs;
                    }else{
                        actionRawDs = actionRawDs.union(oneActionDs);
                    }
                }else{
                    System.out.println("actionlog path "+i+" :"+actionlogstr+",不存在");
                }
            }
        }else{
            String actionlogstr = String.format(actionlogformat,uri,args[0],Integer.valueOf(args[1]));
            boolean actionlogExist = fileSystem.exists(new Path(actionlogstr));
            if(actionlogExist){
                System.out.println("actionlog path: "+actionlogstr);
                actionRawDs = env.readFile(new HdfsSnappyFileInputFormat(new Path(actionlogstr)),actionlogstr);

            }else{
                System.out.println("actionlog path: "+actionlogstr+",不存在");
            }

        }
        if(actionRawDs == null){
            return ;
        }


        //读取hdfs中的recalllog
        String nginxrecalllogstr = String.format(nginxrecallformat,uri,args[0]);
        boolean nginxRecallExists = fileSystem.exists(new Path(nginxrecalllogstr));
        DataSet<String> nginxRecallRawDs = null;
        if(nginxRecallExists){
            nginxRecallRawDs = env.readFile(new HdfsSnappyFileInputFormat(new Path(nginxrecalllogstr)),nginxrecalllogstr);
        }
        if(nginxRecallRawDs == null){
            return ;
        }

        //hbase查询数据的命令，方便到hbase中去查询，看是否有数据存在
        System.out.println("get '"+tablename+"','12771"+Constants.SPLIT+(Long.MAX_VALUE - startTime)+"'");

        //空值的默认值
        String INVALID = "RECOMDASHBOARDINVALID";


        //解析actionlog json,过滤掉不符合要求的，生成对应的OfflineActionLog对象，只取出sceneId,userId,itemId,action
        DataSet<OfflineActionLog> actionLogDs = actionRawDs.flatMap(new FlatMapFunction<String, OfflineActionLog>() {
            @Override
            public void flatMap(String value, Collector<OfflineActionLog> out) throws Exception {

                JSONObject jsonObject = JSONObject.parseObject(value);
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

        //解析recalllog json，只取出sceneId/userId
        DataSet<OfflineNginxRecallLog> nginxRecallDs = nginxRecallRawDs.flatMap(new FlatMapFunction<String, OfflineNginxRecallLog>() {
            @Override
            public void flatMap(String value, Collector<OfflineNginxRecallLog> out) throws Exception {
                JSONObject jsonObject = JSONObject.parseObject(value);
                if (jsonObject == null) {
                    return;
                }
                String userId = INVALID;
                String sceneId = INVALID;

                try {
                    long timestamp = (long)Double.parseDouble(jsonObject.get("timestamp").toString())*1000;

                    //过滤掉不在指定时间窗口中的值
                    if(timestamp<startTimestamp || timestamp >= endTimestamp){
                        return;
                    }
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
                                logger.info("userId error + " + e.getMessage() + " json = " + value);
                            }

                        } else if (split.startsWith("sceneID")) {
                            try {

                                int i = split.indexOf("=");
                                sceneId = split.substring(i+1).trim();
                            } catch (Exception e) {
                                sceneId =INVALID;
                                logger.info("sceneId error " + e.getMessage() + " json = " + value);
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

        tableEnvironment.registerDataSet("offlineActionLog",actionLogDs);
        tableEnvironment.registerDataSet("offlineNginxRecallLog",nginxRecallDs);
        Table offlineActionLogTable = tableEnvironment.scan("offlineActionLog");
        Table offlineNginxRecallLogTable = tableEnvironment.scan("offlineNginxRecallLog");

        //去重action中重复的sceneId、userId
        Table distinctActionLogTable = offlineActionLogTable.groupBy("actionSceneId,actionUserId")
                .select("actionSceneId,actionUserId");

        //过滤recall中重复的sceneId、userId
        Table distinctNginxRecallLogTable = offlineNginxRecallLogTable.groupBy("nRecallSceneId,nRecallUserId")
                .select("nRecallSceneId,nRecallUserId");

        //通过sceneId、userId将去重后的actionlog和nginxrecall进行join，并计算各个场景下的uv,用于计算用户行为传错率
        Table actionSceneIdUvTable = distinctActionLogTable.join(distinctNginxRecallLogTable)
                .where("actionSceneId=nRecallSceneId && actionUserId = nRecallUserId")
                .select("actionSceneId,actionUserId")
                .groupBy("actionSceneId")
                .select("actionSceneId,actionUserId.count as actionUv");


        DataSet<Row> rowDataSet = tableEnvironment.toDataSet(actionSceneIdUvTable, Row.class);
        //用户行为传错率,1 - count(产生行为的用户list iner join 产生请求的用户list) / 行为总量(UV)
        DataSet<Tuple3<String, String, Double>> transforErrorRatioDs = rowDataSet.flatMap(new FlatMapFunction<Row, Tuple3<String, String, Double>>() {
            @Override
            public void flatMap(Row value, Collector<Tuple3<String, String, Double>> out) throws Exception {
                String sceneId = (String) value.getField(0);
                Long uv = (Long) value.getField(1);
                long allActionUv = actionClickUv.get(sceneId) == null ? 0 : actionClickUv.get(sceneId)[1];
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

        //计算各个场景下的itemUv
        Table itemUvBySceneIdTable = offlineActionLogTable.groupBy("actionSceneId,actionItemId")
                .select("actionSceneId,actionItemId")
                .groupBy("actionSceneId")
                .select("actionSceneId,actionItemId.count as itemUv");


        //用户波动率：1 - count(昨天产生行为的用户list iner join 今天产生行为的用户list ) / 行为总量(UV)
        DataSet<Tuple3<String,String,Double>> userFluctuateRatioDs = null;
        //存储当天的场景、用户，并读出前一天的场景、用户
        if(isDaylyCalc){
            String actionUserIdFormat = uri+"/tmp/dashboard/offline/actionUserId/%s.csv";
            String yesActionUserIdStr = String.format(actionUserIdFormat,Long.toString(yesterday));
            boolean yesActionUserIdExist = fileSystem.exists(new Path(yesActionUserIdStr));
            //若前一天的userId信息存在，则可以计算用户波动率。
            if(yesActionUserIdExist){
                //读出前一天的userId信息
                CsvTableSource csvTableSource = CsvTableSource.builder()
                        .path(yesActionUserIdStr)
                        .field("yesSceneId", Types.STRING())
                        .field("yesUserId",Types.STRING())
                        .fieldDelimiter("/")
                        .build();
                Table yesActionUserIdTable = tableEnvironment.fromTableSource(csvTableSource);
                //将前一天的和今天的userId进行join，计算各场景的uv
                Table yesSceneIdUserCountTable = yesActionUserIdTable.join(distinctActionLogTable)
                        .where("yesSceneId = actionSceneId && yesUserId = actionUserId")
                        .groupBy("yesSceneId")
                        .select("yesSceneId as sceneId,yesUserId.count as userIdCount");

                DataSet<Row> yesSceneIdUserCountDs = tableEnvironment.toDataSet(yesSceneIdUserCountTable, Row.class);
                //计算用户波动率，结果格式为场景、"userFluctuateRatio"、用户波动率
                userFluctuateRatioDs = yesSceneIdUserCountDs.flatMap(new FlatMapFunction<Row, Tuple3<String,String,Double>>() {
                    @Override
                    public void flatMap(Row value, Collector<Tuple3<String, String, Double>> out) throws Exception {
                        String sceneId = (String) value.getField(0);
                        long userCount = (long)value.getField(1);
                        long[] clickAndUv = actionClickUv.get(sceneId);
                        if (clickAndUv == null) {
                            logger.info("场景：" + sceneId + "行为总量UV和点击量PV为空");
                            return;
                        }
                        long uv = clickAndUv[1];

                        double userFluctuateRatio = 0;
                        if(uv != 0){
                            userFluctuateRatio = 1-1.0* userCount/uv;
                        }
                        if(userFluctuateRatio<0.00001){
                            userFluctuateRatio = 0;
                        }

                        out.collect(new Tuple3<>(sceneId,"userFluctuateRatio",userFluctuateRatio));

                    }
                });

            }else{
                System.out.println("缺失昨天的UserId信息，无法计算用户波动率");
            }

            String actionUserIdStr = String.format(actionUserIdFormat,Long.toString(startTime));
            boolean actionUserIdStrExist = fileSystem.exists(new Path(actionUserIdStr));
            if(actionUserIdStrExist){
                fileSystem.delete(new Path(actionUserIdStr),true);
            }
            TableSink sink = new CsvTableSink(actionUserIdStr,"/",24, FileSystem.WriteMode.OVERWRITE);
            distinctActionLogTable.writeToSink(sink);
        }




        Table showTable = offlineActionLogTable.where("action = 'show' ");
        Table detailPageShowTable = offlineActionLogTable.where("action = 'detailPageShow'");
        //计算action=show的各场景的、各用户的曝光量
        Table sceneUserShowTable = showTable.groupBy("actionSceneId,actionUserId")
                .select("actionSceneId as showSceneId,actionUserId as showUserId,action.count as showCount");

        //计算action=detailPageShow的各场景的、各用户的点击量
        Table sceneUserDetailTable = detailPageShowTable.groupBy("actionSceneId,actionUserId")
                .select("actionSceneId as detailSceneId,actionUserId as detailUserId,action.count as detailCount");

        Table showDetailCountTable = sceneUserShowTable.fullOuterJoin(sceneUserDetailTable,"showSceneId = detailSceneId && showUserId = detailUserId")
                .select("showSceneId as sceneId,showUserId as userId,showCount,detailCount");

        //计算各场景ctr异常的用户和点击
        Table userCtrAbnormalTable = showDetailCountTable.where("(showCount=0 && detailCount > 0) || detailCount/showCount >" + PropertiesUtil.CTR_THRESHOLD)
                .groupBy("sceneId")
                .select("sceneId,userId.count as userCount,detailCount.sum as detailSum");
        DataSet<Row> userCtrAbnormalDs = tableEnvironment.toDataSet(userCtrAbnormalTable, Row.class);

        //CTR异常用户
        DataSet<Tuple3<String, String, Double>> abnormalUserRatioDs = userCtrAbnormalDs.flatMap(new FlatMapFunction<Row, Tuple3<String, String, Double>>() {
            @Override
            public void flatMap(Row value, Collector<Tuple3<String, String, Double>> out) throws Exception {
                String sceneId = (String) value.getField(0);
                long userCount = (long) value.getField(1);
                long detailSum = (long) value.getField(2);
                long[] clickAndUv = actionClickUv.get(sceneId);
                if (clickAndUv == null) {
                    logger.info("场景：" + sceneId + "行为总量UV和点击量PV为空");
                    return;
                }
                long clickPv = clickAndUv[0];
                long uv = clickAndUv[1];

                double abnormalUserRatio = 0;
                if(uv!=0){
                    abnormalUserRatio = 1.0 * userCount / uv;
                }
                double abnormalUserClickRatio = 0;
                if(clickPv!=0){
                    abnormalUserClickRatio = 1.0 * detailSum / clickPv;
                }
                if(abnormalUserClickRatio< 0.00001){
                    abnormalUserClickRatio =0;
                }
                if(abnormalUserRatio < 0.00001){
                    abnormalUserRatio = 0;
                }

                out.collect(new Tuple3<>(sceneId, "abnormalUserRatio", abnormalUserRatio));
                out.collect(new Tuple3<>(sceneId, "abnormalUserClickRatio",abnormalUserClickRatio ));

            }
        });
        abnormalUserRatioDs.writeAsText(uri+"/dashboard/offline/online/userCtrAbnormal/"+resultTimePath, FileSystem.WriteMode.OVERWRITE);

        //计算action=show的各场景的、各物料的曝光量
        Table sceneItemShowTable = showTable.groupBy("actionSceneId,actionItemId")
                .select("actionSceneId as showSceneId,actionItemId as showItemId,action.count as showItemCount");
        //计算action=detailPageShow的各场景的、各物料的点击量
        Table sceneItemDetailTable = detailPageShowTable.groupBy("actionSceneId,actionItemId")
                .select("actionSceneId as detailSceneId,actionItemId as detailItemId,action.count as detailItemCount");
        Table showDetailItemCountTable = sceneItemShowTable.fullOuterJoin(sceneItemDetailTable, "showSceneId = detailSceneId && showItemId = detailItemId")
                .select("showSceneId as sceneId,showItemId as itemId,showItemCount,detailItemCount");

        //将show和detailPageShow join起来，并进行CTR计算，筛选出CTR异常的物料
        Table itemCtrAbnormalTable = showDetailItemCountTable.where("(showItemCount=0 && detailItemCount > 0) || detailItemCount/showItemCount >" + PropertiesUtil.CTR_THRESHOLD)
                .groupBy("sceneId")
                .select("sceneId,itemId.count as itemCount,detailItemCount.sum as detailItemSum");

        //与各场景下的item uv进行join
        Table itemCtrAbnormalWithUvTable = itemCtrAbnormalTable.join(itemUvBySceneIdTable)
                .where("sceneId = actionSceneId")
                .select("sceneId,itemCount,detailItemSum,itemUv");

        DataSet<Row> itemCtrAbnormalDs = tableEnvironment.toDataSet(itemCtrAbnormalWithUvTable, Row.class);
        DataSet<Tuple3<String, String, Double>> abnormalItemRationDs = itemCtrAbnormalDs.flatMap(new FlatMapFunction<Row, Tuple3<String, String, Double>>() {
            @Override
            public void flatMap(Row value, Collector<Tuple3<String, String, Double>> out) throws Exception {
                String sceneId = (String) value.getField(0);
                long itemCount = (long) value.getField(1);
                long itemDetailSum = (long) value.getField(2);
                long itemUv = (long) value.getField(3);
                long[] clickAndUv = actionClickUv.get(sceneId);
                if (clickAndUv == null) {
                    logger.info("场景：" + sceneId + "行为总量UV和点击量PV为空");
                    return;
                }
                long clickPv = clickAndUv[0];

                double abnormalItemRatio = 0;
                if(itemUv != 0){
                    abnormalItemRatio = 1.0 * itemCount / itemUv;
                }
                double abnormalItemClickRatio = 0;
                if(clickPv != 0){
                    abnormalItemClickRatio = 1.0 * itemDetailSum / clickPv;
                }

                if(abnormalItemClickRatio<0.00001){
                    abnormalItemClickRatio = 0;
                }
                if(abnormalItemRatio < 0.00001){
                    abnormalItemRatio = 0;
                }


                out.collect(new Tuple3<>(sceneId, "abnormalItemClickRatio", abnormalItemClickRatio));
                out.collect(new Tuple3<>(sceneId, "abnormalItemRatio", abnormalItemRatio));
            }
        });
        abnormalItemRationDs.writeAsText(uri+"/dashboard/offline/online/itemctrabnormal.txt", FileSystem.WriteMode.OVERWRITE);

        DataSet<Tuple3<String,String, Double>> unionDs = null;


        if(userFluctuateRatioDs == null){
            unionDs = abnormalUserRatioDs.union(abnormalItemRationDs).union(transforErrorRatioDs);
        }else{
            unionDs = abnormalUserRatioDs.union(abnormalItemRationDs).union(transforErrorRatioDs).union(userFluctuateRatioDs);
        }


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
        resultDs.output(new HBaseOutputFormat(tablename,time));
        String outputPath = uri+"/dashboard/offline/online/allAtatResult/"+resultTimePath;
        resultDs.writeAsText(outputPath, FileSystem.WriteMode.OVERWRITE);


        try {
            env.execute("Offline Job");
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
