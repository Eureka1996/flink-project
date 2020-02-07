package com.wufuqiang.statistics;

import com.wufuqiang.statistics.entries.OfflineActionLog;
import com.wufuqiang.statistics.entries.OfflineNginxRecallLog;
import com.wufuqiang.statistics.utils.OfflineDataSetUtils;
import com.wufuqiang.statistics.utils.OfflineUtils;
import com.wufuqiang.statistics.utils.TimeUtils;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OfflineJobVersion2 {
    public static void main(String[] args) throws Exception {

        Logger logger = LoggerFactory.getLogger(OfflineJobVersion2.class);
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        BatchTableEnvironment tableEnvironment = TableEnvironment.getTableEnvironment(env);

        String fileName = args[0];
        ParameterTool parameterTool = ParameterTool.fromPropertiesFile(fileName);

        String operationType = parameterTool.get("offline.operation.type");
        String day = parameterTool.get("offline.day.begin");
        String hour = parameterTool.get("offline.hour.begin");
        String[] dayHour = OfflineUtils.getDayHour(operationType,day,hour);
        day = dayHour[0];
        hour = dayHour[1];

        String actionlogPathPrefix = parameterTool.get("offline.actionlog.path.prefix");
        String actionFilePath = OfflineUtils.getFilePath(actionlogPathPrefix, operationType, day, hour);
        DataSet<String> actionRawDs = OfflineDataSetUtils.readTextFile(env,actionFilePath);
        if(actionRawDs == null){
            return ;
        }
        //解析actionlog json,过滤掉不符合要求的，生成对应的OfflineActionLog对象，只取出sceneId,userId,itemId,action
        DataSet<OfflineActionLog> actionLogDs = OfflineDataSetUtils.getActionLogDs(actionRawDs);
        tableEnvironment.registerDataSet("offlineActionLog",actionLogDs);
        Table offlineActionLogTable = tableEnvironment.scan("offlineActionLog");
        //去重action中重复的sceneId、userId
        Table distinctActionLogTable = offlineActionLogTable.groupBy("actionSceneId,actionUserId")
                .select("actionSceneId,actionUserId");

        //各场景下的行为总量uv
        Table actionAllUvTable = distinctActionLogTable.groupBy("actionSceneId")
                .select("actionSceneId as actionAllSceneId,actionUserId.count as actionAllUv");

        //各场景下的点击量pv
        Table actionAllDpshowPvTable = offlineActionLogTable.where("action='detailPageShow'")
                .groupBy("actionSceneId")
                .select("actionSceneId,action.count as dpshowCount");

        //计算各个场景下的itemUv
        Table itemUvBySceneIdTable = offlineActionLogTable.groupBy("actionSceneId,actionItemId")
                .select("actionSceneId,actionItemId")
                .groupBy("actionSceneId")
                .select("actionSceneId as actionAllSceneId,actionItemId.count as actionAllUv");

        Table showDPShowTable = offlineActionLogTable.where("action = 'show' || action='detailPageShow'");


        String userCtrThreshold = parameterTool.get("offline.user.crt.abnormal.threshold");
        Table userIdCRTAbnormalTable = OfflineDataSetUtils.calcUIdCTRAbnormal(
                tableEnvironment,
                showDPShowTable,
                Double.parseDouble(userCtrThreshold),"actionUserId");

        //CTR异常用户占比:count(用户CTR超过阈值的用户) / 行为总量(UV)
        DataSet<Tuple3<String, String, Double>> abnormalUserRatioDs =
                OfflineDataSetUtils.calcAbnorCRTUserRatio(tableEnvironment,
                        userIdCRTAbnormalTable,
                        actionAllUvTable,
                        "abnormalUserRatio");

        DataSet<Tuple3<String, String, Double>> calcAbnormalUserClickRatioDs =
                OfflineDataSetUtils.calcAbnormalUserClickRatio(tableEnvironment,
                        userIdCRTAbnormalTable,
                        actionAllDpshowPvTable,"abnormalUserClickRatio");

        String itemCtrThreshold = parameterTool.get("offline.item.crt.abnormal.threshold");
        Table itemIdCRTAbnormalTable = OfflineDataSetUtils.calcUIdCTRAbnormal(
                tableEnvironment,
                showDPShowTable,
                Double.parseDouble(itemCtrThreshold),"actionItemId");

        DataSet<Tuple3<String, String, Double>> abnormalItemRatioDs = OfflineDataSetUtils
                .calcAbnorCRTUserRatio(
                        tableEnvironment,
                        itemIdCRTAbnormalTable,
                        itemUvBySceneIdTable,
                        "abnormalItemRatio");

        DataSet<Tuple3<String, String, Double>> abnormalItemClickRatio = OfflineDataSetUtils
                .calcAbnormalUserClickRatio(
                        tableEnvironment,
                        itemIdCRTAbnormalTable,
                        actionAllDpshowPvTable, "abnormalItemClickRatio");


        //用户波动率
        DataSet<Tuple3<String, String, Double>> userFluctuateRatioDs = null;
        if("day".equals(operationType)){
            String actionUIdPathPrefix = parameterTool.get("offline.actionlog.day.actionUserId.path.prefix");
            String todayActionUIdPath = String.format("%s/%s.csv", actionUIdPathPrefix,day);
            //存储今天产生行为的用户
            OfflineDataSetUtils.cacheUserId(distinctActionLogTable,todayActionUIdPath);
            //读出昨天产生行为的用户
            String yesterday = TimeUtils.getYesterday(day);
            String yesActionUIdPath = String.format("%s/%s.csv", actionUIdPathPrefix, yesterday);
            Table yesActionUIdTable = OfflineDataSetUtils.getUserId(tableEnvironment,yesActionUIdPath);

            //用户波动率：1 - count(昨天产生行为的用户list iner join 今天产生行为的用户list ) / 行为总量(UV)
            userFluctuateRatioDs = OfflineDataSetUtils
                    .calcUserFluctuateRatio(tableEnvironment,
                            yesActionUIdTable,
                            distinctActionLogTable,
                            actionAllUvTable);
        }


        String recalllogPathPrefix = parameterTool.get("offline.recalllog.path.prefix");
        String recallFilePath = OfflineUtils.getFilePath(recalllogPathPrefix, operationType, day, hour);
        DataSet<String> nginxRecallRawDs = OfflineDataSetUtils.readTextFile(env,recallFilePath);
        if(nginxRecallRawDs == null){
            return ;
        }
        //解析recalllog json，只取出sceneId/userId
        DataSet<OfflineNginxRecallLog> nginxRecallDs = OfflineDataSetUtils.getRecallLogDs(nginxRecallRawDs);
        tableEnvironment.registerDataSet("offlineNginxRecallLog",nginxRecallDs);
        Table offlineNginxRecallLogTable = tableEnvironment.scan("offlineNginxRecallLog");
        //去重recall中重复的sceneId、userId
        Table distinctNginxRecallLogTable = offlineNginxRecallLogTable.groupBy("nRecallSceneId,nRecallUserId")
                .select("nRecallSceneId,nRecallUserId");

        //用户行为传错率,1 - count(产生行为的用户list iner join 产生请求的用户list) / 行为总量(UV)
        DataSet<Tuple3<String, String, Double>> transforErrorRatioDs =
                OfflineDataSetUtils.calcTransforErrorRatio(tableEnvironment,
                        distinctActionLogTable,
                        distinctNginxRecallLogTable,
                        actionAllUvTable);


        DataSet<Tuple3<String,String, Double>> unionDs = abnormalUserRatioDs.union(calcAbnormalUserClickRatioDs)
                .union(abnormalItemRatioDs)
                .union(abnormalItemClickRatio)
                .union(transforErrorRatioDs);


        if(userFluctuateRatioDs != null){
            unionDs = unionDs.union(userFluctuateRatioDs);
        }

        DataSet<Tuple3<String, String, String>> unionAllResultDs = OfflineDataSetUtils.unionAllResult(unionDs);

//        unionAllResultDs.writeAsText("hdfs://clusterA:8020/tmp/eureka/unionAllResultDs", FileSystem.WriteMode.OVERWRITE);

        String tablename = "";
        if("day".equals(operationType)){
            tablename = parameterTool.get("offline.result.day.hbase.tablename");
        }else if("hour".equals(operationType)){
            tablename = parameterTool.get("offline.result.hour.hbase.tablename");
        }else{
            return ;
        }
//        unionAllResultDs.output(new HBaseOutputFormat(tablename,String.format("%s%s",day,hour)));

        try {
            env.execute("Offline Job");
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
