package com.wufuqiang.statistics.action;

import org.apache.commons.lang.StringUtils;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;


public class ActionLogJobSecond {

    private static HBaseUtils hbaseUtils ;

    //存储渠道信息
    private static ConcurrentHashMap<String,ConcurrentHashMap<String,Set<String>>> allChannelInfo;
    private static ConcurrentHashMap<String,Object> locks ;

    static {
        hbaseUtils = HBaseUtils.getInstance();
        try {
            allChannelInfo = hbaseUtils.getAllChannelInfo(PropertiesUtil.DASHBOARD_CHANNEL_INFO);
        } catch (IOException e) {
            e.printStackTrace();
        }
        locks = new ConcurrentHashMap<>();
    }

    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        String checkpointPathStr = PropertiesUtil.CHECKPOINT_PATH_ACTIONLOGJOB;
        int checkpointTime = PropertiesUtil.CHECKPOINT_TIME;
        env.enableCheckpointing(checkpointTime);
        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
        Path checkPointPath = new Path(checkpointPathStr);
        StateBackend stateBackend = new FsStateBackend(checkPointPath,false);
        env.setStateBackend(stateBackend);
        CheckpointConfig checkpointConfig = env.getCheckpointConfig();
        checkpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        checkpointConfig.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        checkpointConfig.setFailOnCheckpointingErrors(false);
        checkpointConfig.setMinPauseBetweenCheckpoints(20*1000);
        checkpointConfig.setCheckpointTimeout(60*1000);
        env.setRestartStrategy(RestartStrategies.failureRateRestart(
                3,//一个时间段内的最大失败次数
                Time.of(5, TimeUnit.MINUTES), // 衡量失败次数的是时间段
                Time.of(10, TimeUnit.SECONDS) // 间隔
        ));

        String brokers = PropertiesUtil.KAFKA_PATH;

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", brokers);
        properties.setProperty("group.id", PropertiesUtil.KAFKA_ACTION_GROUPID);


        FlinkKafkaConsumer010<String> consumer = new FlinkKafkaConsumer010<>(PropertiesUtil.ACTIONLOG_TOPICS, new SimpleStringSchema(), properties);

        //指定消费kafka的方式
        consumer.setCommitOffsetsOnCheckpoints(true);
        DataStream<String> dataStream = env.addSource(consumer);

        DataStream<ActionLog> actionLogDataStream = dataStream.map(new MapFunction<String, ActionLog>() {

            @Override
            public ActionLog map(String value) throws Exception {
                return new ActionLog(value);
            }
        }).filter(new FilterFunction<ActionLog>() {
            @Override
            public boolean filter(ActionLog value) throws Exception {
                if(StringUtils.isBlank(value.sceneId) || StringUtils.isBlank(value.userId)){
                    return false;
                }
                if(value.actionTime <= 0){
                    return false;
                }
                return "1".equals(value.contextExist);
            }
        });

        actionLogDataStream
                .keyBy(new KeySelector<ActionLog, String>() {
                    @Override
                    public String getKey(ActionLog value) throws Exception {
                        return value.sceneId;
                    }
                })
                .map(new RichMapFunction<ActionLog, Object>() {

                    // action
                    MapState<TimestampWindowKey, CategoryMap> minuteMapState;
                    MapState<TimestampWindowKey,CategoryMap> minute15MapState;
                    MapState<TimestampWindowKey,CategoryMap> hourMapState;
                    MapState<TimestampWindowKey,CategoryMap> dayMapState;


                    MapState<String, Long> minuteMaxWindowEndtime;
                    MapState<String, Long> minute15MaxWindowEndtime;
                    MapState<String, Long> hourMaxWindowEndtime;
                    MapState<String, Long> dayMaxWindowEndtime;

                    MapState<String, TimestampWindowKey> minuteTriggerWindowLink ;
                    MapState<String, TimestampWindowKey> minute15TriggerWindowLink ;
                    MapState<String, TimestampWindowKey> hourTriggerWindowLink ;
                    MapState<String, TimestampWindowKey> dayTriggerWindowLink ;
                    MapState<String, Long> holdWindow;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);
                        //记录5分钟的窗口
                        minuteMapState = getRuntimeContext().getMapState(new MapStateDescriptor<>("minuteWindow", TimestampWindowKey.class, CategoryMap.class));
                        //记录15分钟的窗口
                        minute15MapState = getRuntimeContext().getMapState(new MapStateDescriptor<>("minute15Window",TimestampWindowKey.class,CategoryMap.class));
                        //记录1小时的窗口
                        hourMapState = getRuntimeContext().getMapState(new MapStateDescriptor<>("hourWindow", TimestampWindowKey.class,CategoryMap.class));
                        //记录1天的窗口
                        dayMapState = getRuntimeContext().getMapState(new MapStateDescriptor<>("dayWindow", TimestampWindowKey.class,CategoryMap.class));


                        //记录5分钟的最后一次操作时间
                        minuteMaxWindowEndtime = getRuntimeContext().getMapState(new MapStateDescriptor<>("actionMinute", String.class, Long.class));
                        minute15MaxWindowEndtime = getRuntimeContext().getMapState(new MapStateDescriptor<>("actionMinute15", String.class, Long.class));
                        hourMaxWindowEndtime = getRuntimeContext().getMapState(new MapStateDescriptor<>("actionHour", String.class, Long.class));
                        dayMaxWindowEndtime = getRuntimeContext().getMapState(new MapStateDescriptor<>("actionDay", String.class, Long.class));

                        //每个场景下有一个触发链表
                        minuteTriggerWindowLink = getRuntimeContext().getMapState(new MapStateDescriptor<>("minuteTriggerWindow",String.class, TimestampWindowKey.class));
                        minute15TriggerWindowLink = getRuntimeContext().getMapState(new MapStateDescriptor<>("minute15TriggerWindow",String.class, TimestampWindowKey.class));
                        hourTriggerWindowLink = getRuntimeContext().getMapState(new MapStateDescriptor<>("hourTriggerWindow",String.class, TimestampWindowKey.class));
                        dayTriggerWindowLink = getRuntimeContext().getMapState(new MapStateDescriptor<>("dayTriggerWindow",String.class, TimestampWindowKey.class));
                        holdWindow = getRuntimeContext().getMapState(new MapStateDescriptor<>("holdWindow",String.class, Long.class));
                    }

                    @Override
                    public Object map(ActionLog input) throws Exception {
                        if(input == null){
                            return input;
                        }

                        // channel info update
                        updateChannelInfo(input);

                        String sceneId = input.getSceneId();

                        handActionLog(input,"5min");
                        handActionLog(input,"15min");
                        handActionLog(input,"1hour");
                        handActionLog(input,"1day");

                        long current5MinTimestamp = (System.currentTimeMillis()/(5*60*1000))*(5*60*1000);
                        Long holdWindowTimestamp = holdWindow.get(sceneId);
                        if(holdWindowTimestamp == null){
                            holdWindow.put(sceneId,current5MinTimestamp);
                        }

                        if(holdWindow.contains(sceneId) && holdWindow.get(sceneId) < current5MinTimestamp){
                            holdWindow.put(sceneId,current5MinTimestamp);
                            trigger(sceneId,"5min");
                            trigger(sceneId,"15min");
                            trigger(sceneId,"1hour");
                            trigger(sceneId,"1day");
                        }

                        return input;
                    }

                    public void handActionLog(ActionLog input , String operateType) throws Exception {
                        String sceneId = input.sceneId;
                        Object lock = locks.get(sceneId);
                        if(lock == null){
                            lock = new Object();
                            locks.put(sceneId,lock);
                        }
                        //记录要处理的日志在哪个时间窗口
                        long timestampWindow = 0;
                        long lastOperateTime = 0;
                        //重新取回数据的hbase表
                        String hbaseTablename = "";
                        MapState<TimestampWindowKey,CategoryMap> statMapState = null;
                        MapState<String,TimestampWindowKey> link = null;

                        if("5min".equals(operateType)){
                            timestampWindow = (input.actionTime/(5*60*1000))*(5*60*1000);
                            statMapState = minuteMapState;
                            link = minuteTriggerWindowLink;
                            lastOperateTime = minuteMaxWindowEndtime.contains(sceneId)?minuteMaxWindowEndtime.get(sceneId):0;
                            hbaseTablename = PropertiesUtil.RECOM_DASHBOARD_MINUTE;
                        }else if("15min".equals(operateType)){
                            timestampWindow = (input.actionTime/(15*60*1000))*(15*60*1000);
                            statMapState = minute15MapState;
                            link = minute15TriggerWindowLink;
                            lastOperateTime = minute15MaxWindowEndtime.contains(sceneId)?minute15MaxWindowEndtime.get(sceneId):0;
                            hbaseTablename = PropertiesUtil.RECOM_DASHBOARD_MINUTE15;
                        }else if("1hour".equals(operateType)){
                            timestampWindow = (input.actionTime/(60*60*1000))*(60*60*1000);
                            statMapState = hourMapState;
                            link = hourTriggerWindowLink;
                            lastOperateTime = hourMaxWindowEndtime.contains(sceneId)?hourMaxWindowEndtime.get(sceneId):0;
                            hbaseTablename = PropertiesUtil.RECOM_DASHBOARD_HOUR;
                        }else if("1day".equals(operateType)){
                            timestampWindow = ((input.actionTime+8*60*60*1000)/(24*60*60*1000))*(24*60*60*1000)-8*60*60*1000;
                            statMapState = dayMapState;
                            link = dayTriggerWindowLink;
                            lastOperateTime = dayMaxWindowEndtime.contains(sceneId)?dayMaxWindowEndtime.get(sceneId):0;
                            hbaseTablename = PropertiesUtil.RECOM_DASHBOARD_DAY;
                        }
                        TimestampWindowKey windowKey = new TimestampWindowKey(sceneId,timestampWindow);
                        CategoryMap categoryMap = instanceCategoryMap(windowKey,statMapState,link,lastOperateTime,hbaseTablename);

                        if("5min".equals(operateType)){
                            categoryMap.addUv(input.userId);
                        }

                        categoryMap.addNewLog(input);
                    }

                    /**
                     *
                     * @param timestampWindowKey:过来的日志所对应的时间窗口
                     * @param mapState：存储数据的
                     * @param triggerWindowLink：
                     * @param lastOperateTime：
                     * @param tablename：hbase table
                     * @return
                     * @throws Exception
                     */
                    public CategoryMap instanceCategoryMap(TimestampWindowKey timestampWindowKey,
                                                           MapState<TimestampWindowKey,CategoryMap> mapState,
                                                           MapState<String,TimestampWindowKey> triggerWindowLink,
                                                           Long lastOperateTime,String tablename) throws Exception {
                        CategoryMap categoryMap = null;
                        String sceneId = timestampWindowKey.getSceneId();
                        long actionTime = timestampWindowKey.getTimestamp();
                        String rowKey = timestampWindowKey.getRowkey();
                        boolean isConstainWindowKey = mapState.contains(timestampWindowKey);

                        if(isConstainWindowKey){
                            categoryMap = mapState.get(timestampWindowKey);
                        }else{
                            Object lock = locks.get(sceneId);
                            if(lock == null){
                                lock = new Object();
                                locks.put(sceneId,lock);
                            }
                            synchronized(lock){
                                if(mapState.contains(timestampWindowKey)){
                                    categoryMap = mapState.get(timestampWindowKey);
                                    isConstainWindowKey = true;
                                }else{
//                                    if(actionTime > lastOperateTime){
//                                        categoryMap = new CategoryMap();
////                                        logger.info("新增一个CategoryMap："+rowKey);
//                                    }else{
//                                        String newRowKey = sceneId+Constants.SPLIT+(Long.MAX_VALUE-actionTime);
//                                        List<String[]> categoryMapList = hbaseUtils.getCategoryMap(tablename, newRowKey);
//                                        categoryMap = new CategoryMap(categoryMapList);
////                                        logger.info("从HBase中取回CategoryMap数据."+rowKey);
//                                    }
                                    String newRowKey = sceneId+Constants.SPLIT+(Long.MAX_VALUE-actionTime);
                                    List<String[]> categoryMapList = hbaseUtils.getCategoryMap(tablename, newRowKey);
                                    if(categoryMapList == null || categoryMapList.size() == 0){
                                        categoryMap = new CategoryMap();
                                    }else{
                                        categoryMap = new CategoryMap(categoryMapList);
                                    }

                                    mapState.put(timestampWindowKey,categoryMap);
                                    if(!triggerWindowLink.contains(sceneId)){
                                        triggerWindowLink.put(sceneId,new TimestampWindowKey());
                                    }
                                    TimestampWindowKeyLinkUtils.insert(triggerWindowLink.get(sceneId),timestampWindowKey);
                                }
                            }
                        }
                        return categoryMap;
                    }

                    public void trigger(String sceneId , String operateType) throws Exception {
                        String uvTablename = "";
                        String statTablename = "";
                        MapState<String,TimestampWindowKey> link = null;
                        MapState<TimestampWindowKey,CategoryMap> statMapState = null;
                        MapState<String,Long> maxWindowEndtime = null;
                        long needRemoveTimestamp = 0 ;

                        if("5min".equals(operateType)){
                            uvTablename = PropertiesUtil.RECOM_DASHBOARD_ACTION_UV_MINUTE;
                            statTablename = PropertiesUtil.RECOM_DASHBOARD_MINUTE;
                            link = minuteTriggerWindowLink;
                            statMapState = minuteMapState;
                            needRemoveTimestamp = ((System.currentTimeMillis()-PropertiesUtil.TIMEWINDOW_DELAY_MINUTE)/(5*60*1000))*(5*60*1000);
                            maxWindowEndtime = minuteMaxWindowEndtime;
                        }else if("15min".equals(operateType)){
                            uvTablename = PropertiesUtil.RECOM_DASHBOARD_ACTION_UV_MINUTE15;
                            statTablename = PropertiesUtil.RECOM_DASHBOARD_MINUTE15;
                            link = minute15TriggerWindowLink;
                            statMapState = minute15MapState;
                            needRemoveTimestamp = ((System.currentTimeMillis()-15*60*1000)/(15*60*1000))*(15*60*1000);
                            maxWindowEndtime = minute15MaxWindowEndtime;

                        }else if("1hour".equals(operateType)){
                            uvTablename = PropertiesUtil.RECOM_DASHBOARD_ACTION_UV_HOUR;
                            statTablename = PropertiesUtil.RECOM_DASHBOARD_HOUR;
                            link = hourTriggerWindowLink;
                            statMapState = hourMapState;
                            needRemoveTimestamp = ((System.currentTimeMillis()-60*60*1000)/(60*60*1000))*(60*60*1000);
                            maxWindowEndtime = hourMaxWindowEndtime;

                        }else if("1day".equals(operateType)){
                            uvTablename =PropertiesUtil.RECOM_DASHBOARD_ACTION_UV_DAY;
                            statTablename = PropertiesUtil.RECOM_DASHBOARD_DAY;
                            link = dayTriggerWindowLink;
                            statMapState = dayMapState;
                            needRemoveTimestamp = ((System.currentTimeMillis()-24*60*60*1000+8*60*60*1000)/(24*60*60*1000))*(24*60*60*1000)-8*60*60*1000;
                            maxWindowEndtime = dayMaxWindowEndtime;
                        }

//                        maxWindowEndtime.put(sceneId,needRemoveTimestamp);

                        TimestampWindowKey triggerWindow = link.get(sceneId).getNext();
                        while(triggerWindow != null){
                            CategoryMap categoryMap = statMapState.get(triggerWindow);
                            if(categoryMap == null){
//                                logger.info("CategoryMap is null.");
                                TimestampWindowKeyLinkUtils.remove(triggerWindow);
                                statMapState.remove(triggerWindow);
                                triggerWindow = triggerWindow.getNext();
                                continue;
                            }
                            String triggerSceneId = triggerWindow.getSceneId();
                            long triggerTimestamp = triggerWindow.getTimestamp();
                            String rowkey = triggerSceneId+Constants.SPLIT+triggerTimestamp;
                            if("5min".equals(operateType)){
                                hbaseUtils.putUserId(PropertiesUtil.RECOM_DASHBOARD_ACTION_UV_MINUTE,triggerSceneId+Constants.SPLIT+triggerTimestamp,"userId",categoryMap.getActionUsers());
                                long minute15Timestamp = (triggerTimestamp/(15*60*1000))*(15*60*1000);
                                hbaseUtils.putUserId(PropertiesUtil.RECOM_DASHBOARD_ACTION_UV_MINUTE15,triggerSceneId+Constants.SPLIT+minute15Timestamp,"userId",categoryMap.getActionUsers());
                                long hourTimestamp = (triggerTimestamp/(60*60*1000))*(60*60*1000);
                                hbaseUtils.putUserId(PropertiesUtil.RECOM_DASHBOARD_ACTION_UV_HOUR,triggerSceneId+Constants.SPLIT+hourTimestamp,"userId",categoryMap.getActionUsers());
                                long dayTimestamp = ((triggerTimestamp+8*60*60*1000)/(24*60*60*1000))*(24*60*60*1000)-8*60*60*1000;
                                hbaseUtils.putUserId(PropertiesUtil.RECOM_DASHBOARD_ACTION_UV_DAY,triggerSceneId+Constants.SPLIT+dayTimestamp,"userId",categoryMap.getActionUsers());

                            }
                            long uv = 0;

                            try {
                                uv = hbaseUtils.getUv(uvTablename,rowkey);
                            } catch (Throwable throwable) {
                                throwable.printStackTrace();
                            }

                            categoryMap.setUv(uv);
                            putCategoryMapToHBase(statTablename,rowkey,categoryMap);
                            hbaseUtils.updateRecall(statTablename,rowkey);
                            if(triggerTimestamp <= needRemoveTimestamp){
                                TimestampWindowKeyLinkUtils.remove(triggerWindow);
                                statMapState.remove(triggerWindow);
                            }

                            triggerWindow = triggerWindow.getNext();


                        }


                    }

                });

        env.execute("ActionLogJob");

    }



    public static int[] getMinuterAndHour(SceneIdAndTimeWindowKey sceneIdAndTimeWindowKey){
        long start = sceneIdAndTimeWindowKey.timeWindow.getStart();
        Calendar calendar = Calendar.getInstance();
        calendar.setTimeInMillis(start);

        int minuter = calendar.get(Calendar.MINUTE);
        int hour = calendar.get(Calendar.HOUR_OF_DAY);
        return new int[]{minuter,hour};
    }


    //将数据存储到HBase中
    public static void putCategoryMapToHBase(String tablename , String rowkey , CategoryMap categoryMap) throws IOException {
        if(categoryMap == null){
//            logger.info("categoryMap to HBase , categoryMap is null");
            return ;
        }
        long start = System.currentTimeMillis();
        List<String[]> calcResult = categoryMap.getCalcResult();
        hbaseUtils.addRecords(tablename,rowkey,calcResult);
        long end = System.currentTimeMillis();
//        logger.info("write actionlogjob categorymap to hbase spend time: "+(end - start) + ",categorymap count:"+categoryMap.count());
    }




    //更新channel info的信息
    public static void updateChannelInfo(ActionLog input) throws Exception {

        if(input == null || allChannelInfo == null){
            return ;
        }
        //当前这一条日志的channel info
        Map<String,String> inputChannelInfo = input.getChannelInfo();


        boolean isContain = allChannelInfo.containsKey(input.sceneId);
        ConcurrentHashMap<String,Set<String>> channelInfoBySceneId = null;
        if(isContain){
            channelInfoBySceneId = allChannelInfo.get(input.sceneId);
        }else{
            channelInfoBySceneId = new ConcurrentHashMap<>();
            allChannelInfo.put(input.sceneId,channelInfoBySceneId);
        }

        Map<String,String> needUpdate = new HashMap<String,String>();
        for(Map.Entry<String,String> entry:inputChannelInfo.entrySet()){
            String channelName = entry.getKey();
            String channelValue = entry.getValue();
            Set<String> channelValueSet = channelInfoBySceneId.get(channelName);
            if(channelValueSet == null){
                channelValueSet = new HashSet<String>();
                channelInfoBySceneId.put(channelName,channelValueSet);
            }
            int size = channelValueSet.size();
            channelValueSet.add(channelValue);
            int newSize = channelValueSet.size();
            if(newSize>size){
                needUpdate.put(channelName,String.join(",",channelValueSet));
            }
        }

        if(needUpdate.size()>0){
            hbaseUtils.updateChannelInfo(PropertiesUtil.DASHBOARD_CHANNEL_INFO,input.sceneId,needUpdate);
        }

    }

    public static String reverseTimestamp(String rowkey){
        String[] rowkeySplited = rowkey.split(Constants.SPLIT);
        String timestampStr = rowkeySplited[1];
        long timestampReverse = Long.MAX_VALUE-Long.valueOf(timestampStr);

        rowkey = rowkeySplited[0] + Constants.SPLIT + timestampReverse;
        return rowkey;
    }



    public static long getTimestamp(String time) throws ParseException {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmm");
        Date parse = sdf.parse(time);
        return parse.getTime();

    }

}


