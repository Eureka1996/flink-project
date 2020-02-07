package com.wufuqiang.itemcf.huaban;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

public class ShuJuStatThird {

    //过滤出合格的图片
    public static void main(String[] args) throws ParseException {

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        BatchTableEnvironment tableEnvironment = TableEnvironment.getTableEnvironment(env);
        DataSet<String> rawDs = null;
        String uri = "hdfs://clusterA:8020";
        String pathFormat = uri+"/recom/petal/218/1874/764/%s";
        //计算哪一天开始的，之前的几天
        String today = args[0];
        //计算几天的数据量
        int dayNum = Integer.valueOf(args[1]);
        for(int i = 0 ; i< dayNum ;i++){
            String pathStr = String.format(pathFormat,today);
            System.out.println("pathStr:"+pathStr);
            if(rawDs == null){
                rawDs = env.readTextFile(pathStr);
            }else{
                rawDs = rawDs.union(env.readTextFile(pathStr));
            }
            today = getYesterday(today);
        }

        String outputPath = uri+"/tmp/huaban/countStat/"+args[0]+"-"+dayNum;
        String quDuanOutputPath = uri+"/tmp/huaban/huaBanQuDuan/"+args[0]+"-"+dayNum;
        System.out.println("quDuanOutputPath:"+quDuanOutputPath);
        System.out.println("outputPath:"+outputPath);
        
        DataSet<Tuple2<String,String>> huaBanDs = rawDs.flatMap(new FlatMapFunction<String, Tuple2<String,String>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<String,String>> out) throws Exception {
                try {
                    JSONObject jsonObject = JSONObject.parseObject(value);

                    String itemId = jsonObject.get("itemId").toString();
                    Object[] boardsLists = jsonObject.getJSONArray("boardsList").toArray();

                    for (Object obj : boardsLists) {
                        JSONObject newJson = (JSONObject) obj;
                        String boardId = newJson.get("boardId").toString();
                        out.collect(new Tuple2<>(itemId, boardId));
                    }

                } catch (Exception e) {
                    System.out.println("json:" + value);
                }
            }
        });

        DataSet<String> censoredPinsRawDs = env.readTextFile(uri+"/itemcf/huabanStandardPicture");
        //读取合格的图片
        DataSet<String> censoredPinsDs = censoredPinsRawDs.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String value, Collector<String> out) throws Exception {
                JSONObject jsonObject = JSONObject.parseObject(value);
                //{"pinId":"819440383","fileId":"272114"}
                String pinId = jsonObject.get("pinId").toString();
                out.collect(pinId);
            }
        });

        tableEnvironment.registerDataSet("CensoredPinsTable",censoredPinsDs);
        Table pinIdTable = tableEnvironment.scan("CensoredPinsTable").as("pinId");


        tableEnvironment.registerDataSet("HuaBan",huaBanDs);
        Table huaBanTable = tableEnvironment.scan("HuaBan").as("itemId,boardId").join(pinIdTable).where("itemId = pinId").select("itemId,boardId").distinct();

        Table boardIdTable = huaBanTable.groupBy("boardId").select("boardId as boardIdTemp,boardId.count as boardIdCount");
        Table itemIdTable = huaBanTable.groupBy("itemId").select("itemId as itemIdTemp,itemId.count as itemIdCount");
        Table scoreTable = huaBanTable.join(boardIdTable).where("boardId = boardIdTemp").select("itemId,boardId,boardIdCount")
                .join(itemIdTable).where("itemId = itemIdTemp").select("itemId,boardId,boardIdCount,itemIdCount");


        DataSet<Row> rowDataSet = tableEnvironment.toDataSet(scoreTable, Row.class);

        DataSet<Tuple3<String, String,Long>> p2PScoreDs = rowDataSet.groupBy(1).reduceGroup(new GroupReduceFunction<Row, Tuple3<String, String, Long>>() {
            @Override
            public void reduce(Iterable<Row> values, Collector<Tuple3<String, String, Long>> out) throws Exception {
                ArrayList<Tuple2<String, Long>> list = new ArrayList<Tuple2<String, Long>>();

                boolean flag = true;
                long boardIdCount = 0;
                for (Row value : values) {
                    if (flag) {
                        boardIdCount = (Long) value.getField(2);
                    }
                    list.add(new Tuple2<>((String) value.getField(0), (Long) value.getField(3)));
                }
                for (Tuple2<String, Long> first : list) {
                    for (Tuple2<String, Long> second : list) {
                        if(!first.f0.equals(second.f0)){
                            out.collect(new Tuple3<>(first.f0, second.f0, boardIdCount + second.f1));
                        }

                    }
                }

            }
        }).groupBy(0,1).sum(2);
        DataSet<Tuple3<String, String,Integer>> keyValueSizeDs = p2PScoreDs.groupBy(0).sortGroup(2, Order.DESCENDING).reduceGroup(new GroupReduceFunction<Tuple3<String, String, Long>, Tuple3<String, String,Integer>>() {
            @Override
            public void reduce(Iterable<Tuple3<String, String, Long>> values, Collector<Tuple3<String, String,Integer>> out) throws Exception {
                List<String> list = new ArrayList<>();
                String itemId = "";
                boolean flag = true;
                for (Tuple3<String, String, Long> value : values) {
                    if (flag) {
                        itemId = value.f0;
                        flag = false;
                    }
                    list.add(value.f1+":"+value.f2);
                }
                out.collect(new Tuple3<>(itemId, String.join(",",list),list.size()));
            }
        });

        keyValueSizeDs.flatMap(new FlatMapFunction<Tuple3<String, String, Integer>, Tuple2<String,Integer>>() {
            @Override
            public void flatMap(Tuple3<String, String, Integer> value, Collector<Tuple2<String, Integer>> out) throws Exception {
                Integer size = value.f2;
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
        }).groupBy(0).sum(1).writeAsText(quDuanOutputPath, FileSystem.WriteMode.OVERWRITE);

        DataSet<Tuple2<String, String>> keyValueDs = keyValueSizeDs.flatMap(new FlatMapFunction<Tuple3<String, String, Integer>, Tuple2<String, String>>() {
            @Override
            public void flatMap(Tuple3<String, String, Integer> value, Collector<Tuple2<String, String>> out) throws Exception {
                out.collect(new Tuple2<>(value.f0, value.f1));
            }
        });

        keyValueDs.writeAsText(outputPath, FileSystem.WriteMode.OVERWRITE);

        try {
            env.execute("ShuJuStat");
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
