package com.wufuqiang.auc;

import com.alibaba.fastjson.JSONObject;
import com.wufuqiang.auc.entries.ActionLog;
import com.wufuqiang.auc.entries.ItemWithRank;
import com.wufuqiang.auc.entries.LabelScoreItem;
import com.wufuqiang.auc.entries.UaucItemWithRank;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple5;
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
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

//predict_model_id为-2的，取recall_score
public class AucCalcSecond {
    public static void main(String[] args) {

        if(args.length<=0 || args.length>2){
            System.out.println("传入参数个数不正确。");
            return ;
        }
        String time = args[0];
        //用于存储最后的结果到hdfs中的路径
        String resultTimePath = "";
        boolean isDaylyCalc = false;
        String tablename = "";//PropertiesUtil.RECOM_DASHBOARD_AUC_HOUR;
        if(args.length == 1){
            isDaylyCalc = true;
            tablename = "";//PropertiesUtil.RECOM_DASHBOARD_AUC_DAY;
            resultTimePath = time;
            time = time+"00";

        }else{
            time = time + String.format("%02d",Integer.valueOf(args[1]));
            resultTimePath = time;
        }

        String INVALID = "RECOMDASHBOARDINVALID";

        String actionlogformat = "%s/camus/action/action/%s/%02d/";


        String recalllogformat = "%s/camus/recall.hourly/recommend_log_after_cleansing/%s/%02d/";

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSet<String> actionlogRawDs = null;
        DataSet<String> recalllogRawDs = null;


        FileSystem fileSystem = null;
        String uri = "hdfs://clusterA:8020";
        try {
            fileSystem = HadoopFileSystem.get(new URI(uri));
        } catch (IOException e) {
            e.printStackTrace();
        } catch (URISyntaxException e) {
            e.printStackTrace();
        }

        if(isDaylyCalc){

            for(int i = 0;i<24;i++){
                String actionlogstr = String.format(actionlogformat,uri, args[0], i);
                String recalllogstr = String.format(recalllogformat,uri, args[0], i);
                boolean actionlogExist = false;
                try {
                    actionlogExist = fileSystem.exists(new Path(actionlogstr));
                } catch (IOException e) {
                    e.printStackTrace();
                }
                if(actionlogExist){
                    System.out.println("actionlog path "+i+" :"+actionlogstr);
                    DataSet<String> oneActionDs = null;//env.readFile(new HdfsSnappyFileInputFormat(new Path(actionlogstr)),actionlogstr);
                    if(actionlogRawDs == null){
                        actionlogRawDs = oneActionDs;
                    }else{
                        actionlogRawDs = actionlogRawDs.union(oneActionDs);
                    }
                }else{
                    System.out.println("actionlog path "+i+" :"+actionlogstr+",不存在");
                }

                boolean recalllogExist = false;

                try {
                    recalllogExist = fileSystem.exists(new Path(recalllogstr));
                } catch (IOException e) {
                    e.printStackTrace();
                }
                if(recalllogExist){
                    System.out.println("recalllog path "+i+" :"+recalllogstr);
                    DataSource<String> oneRecallDs =null;// env.readFile(new HdfsSnappyFileInputFormat(new Path(recalllogstr)), recalllogstr);
                    if(recalllogRawDs == null){
                        recalllogRawDs = oneRecallDs;
                    }else{
                        recalllogRawDs = recalllogRawDs.union(oneRecallDs);
                    }
                }else{
                    System.out.println("recalllog path "+i+" :"+recalllogstr+",不存在");
                }

            }
        }else{
            String actionlogstr = String.format(actionlogformat,uri, args[0], Integer.valueOf(args[1]));
            String recalllogstr = String.format(recalllogformat,uri, args[0], Integer.valueOf(args[1]));

            boolean actionlogExist = false ;
            boolean recalllogExist = false ;

            try {
                actionlogExist = fileSystem.exists(new Path(actionlogstr));
                recalllogExist = fileSystem.exists(new Path(recalllogstr));
            } catch (IOException e) {
                e.printStackTrace();
            }

            if(actionlogExist && recalllogExist){
                actionlogRawDs = null;//env.readFile(new HdfsSnappyFileInputFormat(new Path(actionlogstr)),actionlogstr);
                recalllogRawDs = null;//env.readFile(new HdfsSnappyFileInputFormat(new Path(recalllogstr)),recalllogstr);

            }
            System.out.println("actionlog path:"+actionlogstr+",isExist:"+actionlogExist);
            System.out.println("recalllog path:"+recalllogstr+",isExist:"+recalllogExist);

        }

        if(actionlogRawDs == null || recalllogRawDs == null){
            return;
        }

        System.out.println("get '"+tablename+"','12771"+ "_"+getReverseTime(time)+"'");

        BatchTableEnvironment tableEnvironment = TableEnvironment.getTableEnvironment(env);


        //解析action log日志
        DataSet<ActionLog> actionLogDs = actionlogRawDs.flatMap(new FlatMapFunction<String, ActionLog>() {
            @Override
            public void flatMap(String value, Collector<ActionLog> out) throws Exception {
                JSONObject jsonObject = null;
                try{
                    jsonObject = JSONObject.parseObject(value);
                }catch(Exception e){

                }

                if (jsonObject == null) {
                    return;
                }
                String userId = INVALID;
                String itemId = INVALID;
                String action = INVALID;
                String contextExist = "0";
                String sceneId = INVALID;
                String recallStrategyId = INVALID;
                String channel = INVALID;
                String sortStrategyId = INVALID;
                String predictModelId = INVALID;


                try {
                    contextExist = jsonObject.get("contextExist").toString();
                } catch (Exception e) {
                    e.printStackTrace();
                }
                if (!("1".equals(contextExist))) {
                    return;
                }


                try {
                    action = jsonObject.get("action").toString();
                } catch (Exception e) {
                    e.printStackTrace();
                }

                if (!("show".equals(action) || "detailPageShow".equals(action))) {
                    return;
                }

                try {

                    userId = jsonObject.get("userId") == null ? INVALID : jsonObject.get("userId").toString();
                    itemId = jsonObject.get("itemId") == null ? INVALID : jsonObject.get("itemId").toString();
                    sceneId = jsonObject.get("sceneId") == null ? INVALID : jsonObject.get("sceneId").toString();
                    recallStrategyId = jsonObject.get("recall_strategy_id") == null ? INVALID : jsonObject.get("recall_strategy_id").toString();
                    channel = jsonObject.get("channel") == null ? INVALID : jsonObject.get("channel").toString();
                    sortStrategyId = jsonObject.get("sort_strategy_id") == null ? INVALID : jsonObject.get("sort_strategy_id").toString();
                    predictModelId = jsonObject.get("predict_model_id") == null ? INVALID : jsonObject.get("predict_model_id").toString();
                    Integer label = 0 ;
                    if("detailPageShow".equals(action)){
                        label = 1;
                    }

                    out.collect(new ActionLog(sceneId,userId,itemId,action,recallStrategyId,channel,sortStrategyId,predictModelId,label));

                } catch (Exception e) {
                    e.printStackTrace();
                }

            }
        });

        //解析recall log日志
        DataSet<Tuple5<String,String,String,String,String>> recallLogDs = recalllogRawDs.flatMap(new FlatMapFunction<String, Tuple5<String,String,String,String,String>>() {
            @Override
            public void flatMap(String value, Collector<Tuple5<String,String,String,String,String>> out) throws Exception {
                JSONObject jsonObject = null;
                try{
                    jsonObject = JSONObject.parseObject(value);
                }catch(Exception e){

                }
                if (jsonObject == null) {
                    return;
                }
                String sceneId = INVALID;
                String userId = INVALID;
                String itemId = INVALID;
                String score = "0.0";
                String recallScore = "0.0";
                try {
                    sceneId = jsonObject.get("sceneId") == null ? INVALID : jsonObject.get("sceneId").toString();
                    userId = jsonObject.get("userId") == null ? INVALID : jsonObject.get("userId").toString();
                    itemId = jsonObject.get("item_id") == null ? INVALID : jsonObject.get("item_id").toString();
                    score = jsonObject.get("score") == null ? "0.0" : jsonObject.get("score").toString();
                    recallScore = jsonObject.get("recall_score") == null ? "0.0" : jsonObject.get("recall_score").toString();
                    out.collect(new Tuple5(sceneId, userId ,itemId, score,recallScore));
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        });


        tableEnvironment.registerDataSet("actionlog",actionLogDs);
        tableEnvironment.registerDataSet("recalllog",recallLogDs);
        Table recalllogTable = tableEnvironment.scan("recalllog").as("recallSceneId,recallUserId,recallItemId,score,recallScore");
        Table actionlogTable = tableEnvironment.scan("actionlog");

        //过滤掉已经是detailPageShow的那些show日志
        Table labeledItemTable = tableEnvironment.sqlQuery("select actionSceneId,actionUserId,actionItemId,action,recallStrategyId,channel,sortStrategyId,predictModelId,label " +
                "from actionlog " +
                "where (action='show' and concat(actionSceneId,actionUserId,actionItemId) not in (select concat(actionSceneId,actionUserId,actionItemId) from actionlog where action = 'detailPageShow')) " +
                "or (action = 'detailPageShow') ");


        //将action和recall进行join,通过sceneid,userid,itemid
        Table labelScoreItemTable = labeledItemTable.join(recalllogTable)
                .where("actionSceneId=recallSceneId && actionUserId=recallUserId && actionItemId=recallItemId")
                .select("actionSceneId as sceneId,actionUserId as userId,recallStrategyId,channel,sortStrategyId,predictModelId,label,score,recallScore");

        DataSet<LabelScoreItem> labelScoreItemDataSet = tableEnvironment.toDataSet(labelScoreItemTable, LabelScoreItem.class);


        //将每一第记录拆分成5个渠道
        DataSet<ItemWithRank> fullItemByChannel = labelScoreItemDataSet.flatMap(new FlatMapFunction<LabelScoreItem, ItemWithRank>() {
            @Override
            public void flatMap(LabelScoreItem value, Collector<ItemWithRank> out) throws Exception {
                String sceneId = value.getSceneId();
                String userId = value.getUserId();
                String recallStrategyId = value.getRecallStrategyId();
                String channel = value.getChannel();
                String sortStrategyId = value.getSortStrategyId();
                String predictModelId = value.getPredictModelId();
                Integer label = value.getLabel();
                Double score = Double.parseDouble(value.getScore());
                if("-2".equals(predictModelId)){
                    score = Double.parseDouble(value.getRecallScore());
                }


                out.collect(new ItemWithRank(sceneId, userId, "full", label, score, 0.0));
                out.collect(new ItemWithRank(sceneId, userId, "recall_strategy_id," + recallStrategyId, label, score, 0.0));
                out.collect(new ItemWithRank(sceneId, userId, "channel," + channel, label, score, 0.0));
                out.collect(new ItemWithRank(sceneId, userId, "sort_strategy_id," + sortStrategyId, label, score, 0.0));
                out.collect(new ItemWithRank(sceneId, userId, "predict_model_id," + predictModelId, label, score, 0.0));

            }
        });

        //根据sceneId,channel进行分组，然后按score从小到大进行分组；
        DataSet<ItemWithRank> aucItemWithRankDs = fullItemByChannel.groupBy("sceneId", "channel").sortGroup("score", Order.ASCENDING).reduceGroup(new GroupReduceFunction<ItemWithRank, ItemWithRank>() {
            @Override
            public void reduce(Iterable<ItemWithRank> values, Collector<ItemWithRank> out) throws Exception {
                double count = 0;
                for (ItemWithRank item : values) {
                    count += 1;
                    item.setRank(count);
                    out.collect(item);
                }
            }
        }).groupBy("sceneId", "channel", "score").reduceGroup(new GroupReduceFunction<ItemWithRank, ItemWithRank>() {
            //对score相同的分数进行求平均值
            @Override
            public void reduce(Iterable<ItemWithRank> values, Collector<ItemWithRank> out) throws Exception {
                List<Integer> labels = new ArrayList<Integer>();
                String sceneId = "";
                String channel = "";
                String userId = "";
                Double score = 0.0;
                Long count = 0l;
                Double rankcount = 0.0;

                for (ItemWithRank item : values) {
                    if (count == 0) {
                        sceneId = item.getSceneId();
                        channel = item.getChannel();
                        score = item.getScore();
                    }
                    labels.add(item.getLabel());
                    count += 1;
                    rankcount += item.getRank();
                }
                for (Integer i : labels) {
                    out.collect(new ItemWithRank(sceneId, userId, channel, i, score, rankcount / count));
                }


            }
        });

        //根据sceneId,channel,userId进行分组，然后按score从小到大进行分组；
        DataSet<UaucItemWithRank> uaucItemWithRankDs = fullItemByChannel.groupBy("sceneId", "channel", "userId").sortGroup("score", Order.ASCENDING).reduceGroup(new GroupReduceFunction<ItemWithRank, UaucItemWithRank>() {
            @Override
            public void reduce(Iterable<ItemWithRank> values, Collector<UaucItemWithRank> out) throws Exception {
                double count = 0;
                long show = 0l;
                List<UaucItemWithRank> items = new ArrayList<UaucItemWithRank>();
                for (ItemWithRank item : values) {
                    count += 1;
                    show +=1;
                    item.setRank(count);
                    items.add(new UaucItemWithRank(item.getSceneId(), item.getUserId(), item.getChannel(), item.getLabel(), item.getScore(), item.getRank(), show));
                }

                //设置show个数
                for(UaucItemWithRank item : items){
                    item.setShow(show);
                    out.collect(item);
                }

            }
        }).groupBy("sceneId", "channel", "userId", "score").reduceGroup(new GroupReduceFunction<UaucItemWithRank, UaucItemWithRank>() {
            //对score相同的分数进行求平均值
            @Override
            public void reduce(Iterable<UaucItemWithRank> values, Collector<UaucItemWithRank> out) throws Exception {
                List<Integer> labels = new ArrayList<Integer>();
                String sceneId = "";
                String channel = "";
                String userId = "";
                Long show = 0l;
                Double score = 0.0;
                Long count = 0l;
                Double rankcount = 0.0;

                for (UaucItemWithRank item : values) {
                    if (count == 0) {
                        sceneId = item.getSceneId();
                        channel = item.getChannel();
                        userId = item.getUserId();
                        score = item.getScore();
                        show = item.getShow();
                    }
                    labels.add(item.getLabel());
                    count += 1;
                    rankcount += item.getRank();
                }
                for (Integer i : labels) {
                    out.collect(new UaucItemWithRank(sceneId, userId, channel, i, score, rankcount / count, show));
                }
            }
        });


        //进行auc的计算
        DataSet<Tuple3<String, String, Double>> aucResultDs = aucItemWithRankDs.groupBy("sceneId", "channel").reduceGroup(new GroupReduceFunction<ItemWithRank, Tuple3<String, String, Double>>() {
            @Override
            public void reduce(Iterable<ItemWithRank> values, Collector<Tuple3<String, String, Double>> out) throws Exception {
                long posCount = 0;
                long negCount = 0;
                double rankCount = 0;
                String sceneId = "";
                String channel = "";
                boolean flag = true;
                for (ItemWithRank value : values) {
                    if (flag) {
                        sceneId = value.getSceneId();
                        channel = value.getChannel();
                        flag = false;
                    }

                    int label = value.getLabel();
                    if (label == 0) {
                        negCount += 1;
                    } else if (label == 1) {
                        rankCount += value.getRank();
                        posCount += 1;
                    }
                }


                if(posCount != 0 && negCount != 0){
                    double auc = (rankCount - (posCount * (posCount + 1) / 2)) / (posCount * negCount);
                    out.collect(new Tuple3<>(sceneId, channel, auc));
                }else{
                    System.out.println("auc sceneId:"+sceneId+",channel:"+channel+",正样本个数："+posCount+",负样本个数："+negCount);
                }
            }
        });


        DataSet<Tuple3<String, String, Double>> uaucResultDs =
                uaucItemWithRankDs.groupBy("sceneId", "channel", "userId").reduceGroup(new GroupReduceFunction<UaucItemWithRank, Tuple5<String, String, String, Long, Double>>() {
                    //计算每一个用户的auc
            @Override
            public void reduce(Iterable<UaucItemWithRank> values, Collector<Tuple5<String, String, String, Long, Double>> out) throws Exception {
                long posCount = 0;
                long negCount = 0;
                double rankCount = 0;
                String sceneId = "";
                String channel = "";
                String userId = "";
                long show = 0;
                boolean flag = true;
                for (UaucItemWithRank item : values) {
                    if (flag) {
                        sceneId = item.getSceneId();
                        channel = item.getChannel();
                        userId = item.getUserId();
                        show = item.getShow();
                        flag = false;
                    }
                    int label = item.getLabel();
                    if (label == 0) {
                        negCount += 1;
                    } else if (label == 1) {
                        rankCount += item.getRank();
                        posCount += 1;
                    }
                }
                if(posCount != 0 && negCount != 0){
                    double auc = (rankCount - (posCount * (posCount + 1)) / 2) / (posCount * negCount);
                    out.collect(new Tuple5<>(sceneId, channel, userId, show, auc));
                }
            }
        }).groupBy(0, 1).reduceGroup(new GroupReduceFunction<Tuple5<String, String, String, Long, Double>, Tuple3<String, String, Double>>() {
            //计算uauc
            @Override
            public void reduce(Iterable<Tuple5<String, String, String, Long, Double>> values, Collector<Tuple3<String, String, Double>> out) throws Exception {
                long showAll = 0;
                double aucAll = 0.0;
                String sceneId = INVALID;
                String channel = INVALID;
                boolean flag = true;
                for (Tuple5<String, String, String, Long, Double> value : values) {
                    if (flag) {
                        sceneId = value.f0;
                        channel = value.f1;
                        flag = false;
                    }
                    showAll += value.f3;
                    aucAll += (value.f3 * value.f4);
                }
                if(showAll != 0){
                    out.collect(new Tuple3<>(sceneId, channel, aucAll / showAll));
                }

            }
        });

        DataSet<Tuple3<String, String, String>> aucAndUaucDs = aucResultDs.fullOuterJoin(uaucResultDs)
                .where(0, 1)
                .equalTo(0, 1)
                .with(new JoinFunction<Tuple3<String, String, Double>, Tuple3<String, String, Double>, Tuple3<String, String, String>>() {
            @Override
            public Tuple3<String, String, String> join(Tuple3<String, String, Double> first, Tuple3<String, String, Double> second) throws Exception {
                String sceneId = "";
                String channel = "";
                double auc = 0.0;
                double uauc = 0.0;
                if(first != null){
                    sceneId = first.f0;
                    channel = first.f1;
                    auc = first.f2;
                }else{
                    sceneId = second.f0;
                    channel = second.f1;
                }
                if(second != null){
                    uauc = second.f2;
                }
                return new Tuple3<>(sceneId,channel,Double.toString(auc)+"_"+Double.toString(uauc));
            }
        });

//        aucAndUaucDs.output(new HBaseOutputFormat(tablename,time));
        String outputPath = uri+"/dashboard/offline/test/aucAndUaucResult/"+resultTimePath;
        aucAndUaucDs.writeAsText(outputPath, FileSystem.WriteMode.OVERWRITE);
        System.out.println("outputPath:"+outputPath);

        try {
            env.execute("Flink Table Demo");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static String getReverseTime(String time){
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHH");
        Date parse = null;
        try {
            parse = sdf.parse(time);
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return Long.toString(Long.MAX_VALUE - parse.getTime());
    }


}
