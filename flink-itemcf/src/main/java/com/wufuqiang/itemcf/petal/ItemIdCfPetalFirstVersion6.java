package com.wufuqiang.itemcf.petal;

import com.wufuqiang.itemcf.entries.PetalI2IRelation;
import com.wufuqiang.itemcf.entries.PetalItem;
import com.wufuqiang.itemcf.utils.PetalDataSetUtils;
import com.wufuqiang.itemcf.utils.TimeUtils;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.util.Collector;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class ItemIdCfPetalFirstVersion6 {

    public static void main(String[] args) throws ParseException, IOException {


        String propFileName = args[0];
        ParameterTool parameterTool = ParameterTool.fromPropertiesFile(propFileName);

        String businessIdStr = parameterTool.get("itemcf.businessids");
        String sceneIdStr = parameterTool.get("itemcf.sceneids");
        String itemSetIdStr = parameterTool.get("itemcf.itemsetids");
        String numDayStr = parameterTool.get("itemcf.day.numbers");
        String todayStr = parameterTool.get("itemcf.day.begin");
        if("yesterday".equals(todayStr)){
            Date current = new Date();
            SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
            String now = sdf.format(current);
            todayStr = TimeUtils.getYesterday(now);
        }

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSet<String> rawDs = PetalDataSetUtils.readTextFile(env,businessIdStr,sceneIdStr,itemSetIdStr,todayStr,numDayStr);
        if(rawDs == null){
            System.out.println("rawDs is null.");
            return;
        }

        //将json解析,取出userId,itemId
        DataSet<PetalItem> userIdItemIdDs = PetalDataSetUtils.getPetalItemDs(rawDs);

        String legalItemIdPath = parameterTool.get("itemcf.huaban.legal.itemid.path");
        String itemidJsonname = parameterTool.get("itemcf.huaban.legal.itemid.jsonname");
        //获取审核的itemid
        DataSet<Tuple1<String>> censoredDs = PetalDataSetUtils.getCensored(env, legalItemIdPath, itemidJsonname);

        //给审核通过的itemid打上标签
        DataSet<PetalItem> petalItemDataSet = PetalDataSetUtils.putCensoredFlag(userIdItemIdDs, censoredDs);

        //形成itemid-itemid，并计算共现值
        DataSet<PetalI2IRelation> itemItemScoreDs = PetalDataSetUtils.getAppearValue(petalItemDataSet);

        DataSet<PetalI2IRelation> resultScoreDs = PetalDataSetUtils.calcScore(env,itemItemScoreDs,userIdItemIdDs);

        DataSet<PetalI2IRelation> censoredResultScoreDs = PetalDataSetUtils.filterOut(resultScoreDs);

        DataSet<Tuple3<String, String, Integer>> itemValueSizeDs = censoredResultScoreDs.groupBy("itemIdOne").sortGroup("score",Order.DESCENDING).reduceGroup(new GroupReduceFunction<PetalI2IRelation, Tuple3<String,String,Integer>>() {
            @Override
            public void reduce(Iterable<PetalI2IRelation> values, Collector<Tuple3<String, String, Integer>> out) throws Exception {
                String oneItemId = "";
                List<String> itemScore = new ArrayList<>();
                for (PetalI2IRelation value : values) {
                    oneItemId = value.getItemIdOne();
                    String anotherItemId = value.getItemIdOther();
                    String score = ((Double)value.getScore()).toString();
                    itemScore.add(anotherItemId + ":" + score);
                }
                out.collect(new Tuple3<>(oneItemId, String.join(",", itemScore), itemScore.size()));
            }
        });

        DataSet<Tuple2<String, Integer>> classifiedValueSizeDs = PetalDataSetUtils.classifyValueSize(itemValueSizeDs);

        String subregionOutputPrefix=parameterTool.get("itemcf.datas.subregion.outputpath.prefix");
        String quDuanOutputStr = String.format("%s/%s/%s/%s/%s",subregionOutputPrefix,businessIdStr,sceneIdStr,itemSetIdStr,todayStr+"-"+numDayStr);
        System.out.println("quDuanOutputStr:"+quDuanOutputStr);
        classifiedValueSizeDs.writeAsText(quDuanOutputStr, FileSystem.WriteMode.OVERWRITE);

        String datasOutputPrefix = parameterTool.get("itemcf.datas.outputpath.prefix");
        String outputPathStr = String.format("%s/%s/%s/%s/%s",datasOutputPrefix,businessIdStr,sceneIdStr,itemSetIdStr,todayStr+"-"+numDayStr);
        System.out.println("outputPath:"+outputPathStr);
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
}
