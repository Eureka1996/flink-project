package com.wufuqiang.statistics.action;

import com._4paradigm.commonData.SceneIdAndTimeWindowKey;
import com._4paradigm.data.Constants;
import com._4paradigm.utils.HBaseUtils;
import com._4paradigm.utils.PropertiesUtil;
import com._4paradigm.utils.TriggerWindowLinkUtils;
import com.wufuqiang.commons.Constants;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;


public class ActionLogJob {

    private static Logger logger = LoggerFactory.getLogger(ActionLogJob.class);

    private static final long TIMEWINDOW_DELAY_MINUTE = 10 * Constants.ONE_MINUTE;

    private static HBaseUtils hbaseUtils ;

    private static long starttime;

    //存储渠道信息
    private static ConcurrentHashMap<String,ConcurrentHashMap<String,Set<String>>> allChannelInfo;
    private static ConcurrentHashMap<String,Object> locks ;


    static {
        hbaseUtils = HBaseUtils.getInstance();
        try {
            hbaseUtils.createTable(Arrays.asList(
                    PropertiesUtil.RECOM_DASHBOARD_MINUTE,
                    PropertiesUtil.RECOM_DASHBOARD_MINUTE15,
                    PropertiesUtil.RECOM_DASHBOARD_HOUR,
                    PropertiesUtil.RECOM_DASHBOARD_DAY,
                    PropertiesUtil.DASHBOARD_CHANNEL_INFO));

            hbaseUtils.createTable(Arrays.asList(
                    PropertiesUtil.RECOM_DASHBOARD_ACTION_UV_MINUTE,
                    PropertiesUtil.RECOM_DASHBOARD_ACTION_UV_MINUTE15,
                    PropertiesUtil.RECOM_DASHBOARD_ACTION_UV_HOUR,
                    PropertiesUtil.RECOM_DASHBOARD_ACTION_UV_DAY
            ),3*24*60*60);

            allChannelInfo = hbaseUtils.getAllChannelInfo(PropertiesUtil.DASHBOARD_CHANNEL_INFO);
            logger.info("allChannelInfo:"+allChannelInfo.size());
        } catch (IOException e) {
            logger.info("load channel info from "+PropertiesUtil.DASHBOARD_CHANNEL_INFO+" error");
            e.printStackTrace();
        }
        starttime = System.currentTimeMillis();
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
        checkpointConfig.setMinPauseBetweenCheckpoints(2*1000);
        env.setRestartStrategy(RestartStrategies.failureRateRestart(
                3,//一个时间段内的最大失败次数
                Time.of(5, TimeUnit.MINUTES), // 衡量失败次数的是时间段
                Time.of(10, TimeUnit.SECONDS) // 间隔
        ));

        String brokers = PropertiesUtil.KAFKA_PATH;
        String topics = PropertiesUtil.ACTIONLOG_TOPICS;
        List<String> topicsList = Arrays.asList(topics.split(","));

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", brokers);
        properties.setProperty("group.id", PropertiesUtil.KAFKA_ACTION_GROUPID);


        FlinkKafkaConsumer010<String> consumer = new FlinkKafkaConsumer010<>(topicsList, new SimpleStringSchema(), properties);

        //指定消费kafka的方式
//        consumer.setCommitOffsetsOnCheckpoints(true);
//        consumer.setStartFromLatest();
        consumer.setStartFromGroupOffsets();

        DataStream<String> dataStream = env.addSource(consumer);

        DataStream<ActionLog> actionLogDataStream = dataStream.map(new MapFunction<String, ActionLog>() {

            @Override
            public ActionLog map(String value) throws Exception {
                return new ActionLog(value);
            }
        }).filter(new FilterFunction<ActionLog>() {
            @Override
            public boolean filter(ActionLog value) throws Exception {
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
                    MapState<SceneIdAndTimeWindowKey, CategoryMap> minuteMapState;
                    MapState<SceneIdAndTimeWindowKey,CategoryMap> minute15MapState;
                    MapState<String, Long> minuteMaxWindowEndtime;
                    MapState<SceneIdAndTimeWindowKey,CategoryMap> hourMapState;
                    MapState<SceneIdAndTimeWindowKey,CategoryMap> dayMapState;
                    MapState<String, SceneIdAndTimeWindowKey> triggerWindowLink ;
                    MapState<String, SceneIdAndTimeWindowKey> holdWindow;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);
                        //记录5分钟的窗口
                        minuteMapState = getRuntimeContext().getMapState(new MapStateDescriptor<>("minuteWindow", SceneIdAndTimeWindowKey.class, CategoryMap.class));
                        //记录15分钟的窗口
                        minute15MapState = getRuntimeContext().getMapState(new MapStateDescriptor<SceneIdAndTimeWindowKey, CategoryMap>("minute15Window",SceneIdAndTimeWindowKey.class,CategoryMap.class));
                        //记录1小时的窗口
                        hourMapState = getRuntimeContext().getMapState(new MapStateDescriptor<SceneIdAndTimeWindowKey, CategoryMap>("hourWindow", SceneIdAndTimeWindowKey.class,CategoryMap.class));
                        //记录1天的窗口
                        dayMapState = getRuntimeContext().getMapState(new MapStateDescriptor<SceneIdAndTimeWindowKey, CategoryMap>("dayWindow", SceneIdAndTimeWindowKey.class,CategoryMap.class));
                        //记录5分钟的最后一次操作时间
                        minuteMaxWindowEndtime = getRuntimeContext().getMapState(new MapStateDescriptor<>("actionMinute", String.class, Long.class));

                        //每个场景下有一个触发链表
                        triggerWindowLink = getRuntimeContext().getMapState(new MapStateDescriptor<String, SceneIdAndTimeWindowKey>("triggerWindow",String.class, SceneIdAndTimeWindowKey.class));
                        holdWindow = getRuntimeContext().getMapState(new MapStateDescriptor<String, SceneIdAndTimeWindowKey>("holdWindow",String.class, SceneIdAndTimeWindowKey.class));
                    }

                    @Override
                    public Object map(ActionLog input) throws Exception {

                        // channel info update
                        updateChannelInfo(input);

                        long currenttime = System.currentTimeMillis();

                        //处理kafka累积的日志
                        if(currenttime - starttime < PropertiesUtil.CONSUME_KAFKA_CUMULATION){
                            restartStrategy(input,currenttime);
                            return input;
                        }

                        if(minuteMaxWindowEndtime.contains(input.sceneId) && minuteMaxWindowEndtime.get(input.sceneId) > input.actionTime){
                            logger.info("数据超时，被抛弃。");
                            return input;
                        }

                        handleNewActionLog(input);

                        trigger(input);

                        return input;
                    }

                    //处理每一条日志
                    public void handleNewActionLog(ActionLog input) throws Exception {
                        SceneIdAndTimeWindowKey sceneIdAndTimeWindowKey = new SceneIdAndTimeWindowKey();
                        sceneIdAndTimeWindowKey.setValue(input.sceneId,input.actionTime);

                        boolean isContainWindow = false;
                        isContainWindow = minuteMapState.contains(sceneIdAndTimeWindowKey);

                        CategoryMap categoryMap = new CategoryMap();

                        if(isContainWindow){
                            categoryMap = minuteMapState.get(sceneIdAndTimeWindowKey);
                        }

                        categoryMap.addNewLog(input);

                        if(!isContainWindow){
                            minuteMapState.put(sceneIdAndTimeWindowKey,categoryMap);
                            if(!triggerWindowLink.contains(input.sceneId)){
                                triggerWindowLink.put(input.sceneId,new SceneIdAndTimeWindowKey());
                                logger.info("triggerWindowLink add");
                            }
                            TriggerWindowLinkUtils.insert(triggerWindowLink.get(input.sceneId),sceneIdAndTimeWindowKey);
                            logger.info("actionlogjob add a TriggerWindow:"+sceneIdAndTimeWindowKey.getString()[0]);
                        }
                    }

                    public void trigger(ActionLog input) throws Exception{
                        // 要触发的窗口
                        SceneIdAndTimeWindowKey preSceneIdAndTimeWindowKey = new SceneIdAndTimeWindowKey();
                        preSceneIdAndTimeWindowKey.setValue(input.sceneId, input.actionTime - TIMEWINDOW_DELAY_MINUTE);

                        SceneIdAndTimeWindowKey triggerWindow = triggerWindowLink.get(input.sceneId).getNext();


                        while(triggerWindow != null){

                            if(triggerWindow.timeWindow.getStart() > preSceneIdAndTimeWindowKey.timeWindow.getStart()){
                                break;
                            }

                            CategoryMap categoryMap = minuteMapState.get(triggerWindow);

                            if(categoryMap == null){
                                logger.info("categoryMap is null.");
                                minuteMaxWindowEndtime.put(triggerWindow.sceneId, triggerWindow.timeWindow.getEnd());
                                TriggerWindowLinkUtils.remove(triggerWindowLink.get(input.sceneId),triggerWindow);
                                minuteMapState.remove(triggerWindow);
                                logger.info("holdwindow 5分钟mapState:"+minuteMapState.keys());
                                triggerWindow = triggerWindow.getNext();
                                continue;
                            }

                            String[] rowKeyTimes = triggerWindow.getString();
                            String rowKeyMinute = rowKeyTimes[0];
                            String rowKeyMinute15 = rowKeyTimes[1];
                            String rowKeyHour = rowKeyTimes[2];
                            String rowKeyDay = rowKeyTimes[3];

                            putCategoryMapToHBase(PropertiesUtil.RECOM_DASHBOARD_MINUTE,rowKeyMinute,categoryMap);

                            hbaseUtils.updateRecall(PropertiesUtil.RECOM_DASHBOARD_MINUTE,rowKeyMinute);

                            hbaseUtils.putUserId(PropertiesUtil.RECOM_DASHBOARD_ACTION_UV_MINUTE15,rowKeyMinute15,"userId",categoryMap.getActionUsers());
                            hbaseUtils.putUserId(PropertiesUtil.RECOM_DASHBOARD_ACTION_UV_HOUR,rowKeyHour,"userId",categoryMap.getActionUsers());
                            hbaseUtils.putUserId(PropertiesUtil.RECOM_DASHBOARD_ACTION_UV_DAY,rowKeyDay,"userId",categoryMap.getActionUsers());

                            logger.info("actionlogjob触发15分钟的操作");
                            merge(1,rowKeyMinute15,triggerWindow,categoryMap,minute15MapState);


                            logger.info("actionlogjob触发一小时的操作。");
                            merge(2,rowKeyHour,triggerWindow,categoryMap,hourMapState);

                            logger.info("actionlogjob触发一天的操作。");
                            merge(3,rowKeyDay,triggerWindow,categoryMap,dayMapState);

                            minuteMaxWindowEndtime.put(triggerWindow.sceneId, triggerWindow.timeWindow.getEnd());
                            TriggerWindowLinkUtils.remove(triggerWindowLink.get(input.sceneId),triggerWindow);
                            minuteMapState.remove(triggerWindow);

                            logger.info("5分钟mapState:"+minuteMapState.keys());

                            triggerWindow = triggerWindow.getNext();
                        }
                    }


//                    public CategoryMap instanceCategoryMap(ActionLog input,){
//
//                        return null;
//                    }

                    public void merge(int type, String rowKey , SceneIdAndTimeWindowKey triggerWindow , CategoryMap categoryMap, MapState<SceneIdAndTimeWindowKey,CategoryMap> mapState) throws Exception {

                        String tablename = "";
                        String uvTablename = "";
                        if(type == 1){
                            tablename = PropertiesUtil.RECOM_DASHBOARD_MINUTE15;
                            uvTablename = PropertiesUtil.RECOM_DASHBOARD_ACTION_UV_MINUTE15;
                        }else if(type ==2){
                            tablename = PropertiesUtil.RECOM_DASHBOARD_HOUR;
                            uvTablename = PropertiesUtil.RECOM_DASHBOARD_ACTION_UV_HOUR;
                        }else if(type == 3){
                            tablename = PropertiesUtil.RECOM_DASHBOARD_DAY;
                            uvTablename = PropertiesUtil.RECOM_DASHBOARD_ACTION_UV_DAY;
                        }

                        SceneIdAndTimeWindowKey newWindow =  new SceneIdAndTimeWindowKey();

                        String[] rowkeySplit = rowKey.split(Constants.SPLIT);
                        String sceneId = rowkeySplit[0];
                        long timestamp = Long.valueOf(rowkeySplit[1]);
                        newWindow.setValue(sceneId,timestamp);

                        CategoryMap newCategoryMap = null ;
                        boolean isContainWindowKey =  mapState.contains(newWindow);
                        if(isContainWindowKey){
                            newCategoryMap = mapState.get(newWindow);
                        }else{
                            newCategoryMap = new CategoryMap();
                        }
                        newCategoryMap.addCategoryMap(categoryMap);
                        long uv = 0;
                        try {
                            uv = hbaseUtils.getUv(uvTablename, rowKey);
                        } catch (Throwable throwable) {
                            throwable.printStackTrace();
                        }
                        newCategoryMap.setUv(uv);
                        putCategoryMapToHBase(tablename,rowKey,newCategoryMap);
                        hbaseUtils.updateRecall(tablename,rowKey);
                        if(!isContainWindowKey){
                            mapState.clear();
                            mapState.put(newWindow,newCategoryMap);
                            logger.info(tablename+",mapState:"+mapState.keys());
                        }
                    }

                    public void restartMerge(int type,String rowKey,SceneIdAndTimeWindowKey triggerWindow,CategoryMap categoryMap,MapState<SceneIdAndTimeWindowKey,CategoryMap> mapState) throws Exception {
                        String tablename = "";
                        String uvTablename = "";
                        if(type == 1){
                            tablename = PropertiesUtil.RECOM_DASHBOARD_MINUTE15;
                            uvTablename = PropertiesUtil.RECOM_DASHBOARD_ACTION_UV_MINUTE15;
                        }else if(type ==2){
                            tablename = PropertiesUtil.RECOM_DASHBOARD_HOUR;
                            uvTablename = PropertiesUtil.RECOM_DASHBOARD_ACTION_UV_HOUR;
                        }else if(type == 3){
                            tablename = PropertiesUtil.RECOM_DASHBOARD_DAY;
                            uvTablename = PropertiesUtil.RECOM_DASHBOARD_ACTION_UV_DAY;
                        }

                        SceneIdAndTimeWindowKey newWindow =  new SceneIdAndTimeWindowKey();

                        String[] rowkeySplit = rowKey.split(Constants.SPLIT);
                        String sceneId = rowkeySplit[0];
                        long timestamp = Long.valueOf(rowkeySplit[1]);
                        newWindow.setValue(sceneId,timestamp);

                        CategoryMap newCategoryMap = null ;
                        boolean isContainWindowKey =  mapState.contains(newWindow);
                        if(isContainWindowKey){
                            newCategoryMap = mapState.get(newWindow);
                        }else{

                            Object lock = locks.get(sceneId);
                            if(lock == null){
                                lock = new Object();
                                locks.put(sceneId,lock);
                            }
                            synchronized (lock){
                                if(mapState.contains(newWindow)){
                                    newCategoryMap = mapState.get(newWindow);
                                    isContainWindowKey = true;
                                }else{
                                    String newRowkey = reverseTimestamp(rowKey);
                                    List<String[]> categoryMapList = hbaseUtils.getCategoryMap(tablename, newRowkey);
                                    newCategoryMap = new CategoryMap(categoryMapList);
                                    mapState.put(newWindow,newCategoryMap);
                                    if(!triggerWindowLink.contains(sceneId)){
                                        triggerWindowLink.put(sceneId,new SceneIdAndTimeWindowKey());
                                    }
                                    TriggerWindowLinkUtils.insert(triggerWindowLink.get(sceneId),newWindow);
                                    logger.info("从HBase中取回CategoryMap数据."+rowKey);
                                }
                            }

                        }
                        newCategoryMap.addCategoryMap(categoryMap);

                        int[] minuterAndHour = getMinuterAndHour(triggerWindow);

                        if((type == 1 && (minuterAndHour[0] == 10 || minuterAndHour[0] == 25 || minuterAndHour[0] == 40 || minuterAndHour[0] == 55))||(type == 2 && minuterAndHour[0] == 55)||( type == 3 && minuterAndHour[0] == 55)){
                            long uv = 0;
                            try {
                                uv = hbaseUtils.getUv(uvTablename, rowKey);
                            } catch (Throwable throwable) {
                                throwable.printStackTrace();
                            }
                            newCategoryMap.setUv(uv);
                            putCategoryMapToHBase(tablename,rowKey,newCategoryMap);
                            hbaseUtils.updateRecall(tablename,rowKey);
                        }

                        if(!isContainWindowKey){
                            mapState.clear();
                            mapState.put(newWindow,newCategoryMap);
                            logger.info(tablename+",mapState:"+mapState.keys());
                        }
                    }

                    public void restartStrategy(ActionLog input , long currenttime) throws Exception{

                        //定义进来的日志所有的时间窗口
                        SceneIdAndTimeWindowKey sceneIdAndTimeWindowKey = new SceneIdAndTimeWindowKey();
                        sceneIdAndTimeWindowKey.setValue(input.sceneId,input.actionTime);

                        //判断这个窗口是否已经存在
                        boolean isContainWindowKey = false;
                        isContainWindowKey = minuteMapState.contains(sceneIdAndTimeWindowKey);
                        CategoryMap hbaseCategoryMap = null;
                        if(isContainWindowKey){
                            //如果窗口已经存在，直接取出
                            hbaseCategoryMap = minuteMapState.get(sceneIdAndTimeWindowKey);
                        }else{
                            //如果窗口不存在，则从hbase当中取出数据
                            Object lock = locks.get(input.sceneId);
                            if(lock == null){
                                lock = new Object();
                                locks.put(input.sceneId,lock);
                            }
                            synchronized (lock){
                                if(minuteMapState.contains(sceneIdAndTimeWindowKey)){
                                   hbaseCategoryMap = minuteMapState.get(sceneIdAndTimeWindowKey);
                                   isContainWindowKey = true;
                                }else{
                                    String newRowkey = reverseTimestamp(sceneIdAndTimeWindowKey.getString()[0]);
                                    List<String[]> categoryMapList = hbaseUtils.getCategoryMap(PropertiesUtil.RECOM_DASHBOARD_MINUTE, newRowkey);
                                    hbaseCategoryMap = new CategoryMap(categoryMapList);
                                    minuteMapState.put(sceneIdAndTimeWindowKey,hbaseCategoryMap);
                                    if(!triggerWindowLink.contains(input.sceneId)){
                                        triggerWindowLink.put(input.sceneId,new SceneIdAndTimeWindowKey());
                                    }
                                    TriggerWindowLinkUtils.insert(triggerWindowLink.get(input.sceneId),sceneIdAndTimeWindowKey);
                                    logger.info("从HBase中取回CategoryMap数据."+sceneIdAndTimeWindowKey.getString()[0]);
                                }
                            }
                        }

                        hbaseCategoryMap.addNewLog(input);

                        //当前时间对应的时间窗口
                        SceneIdAndTimeWindowKey currentSceneIdAndTimeWindowKey = new SceneIdAndTimeWindowKey();
                        currentSceneIdAndTimeWindowKey.setValue(input.sceneId,currenttime);
                        SceneIdAndTimeWindowKey holdSceneIdAndTimeWindowKey = holdWindow.get(input.sceneId);

                        if(holdSceneIdAndTimeWindowKey == null){
                            logger.info(input.sceneId+",holdwindow没有该场景的时间窗口");
                            holdSceneIdAndTimeWindowKey = currentSceneIdAndTimeWindowKey;
                            holdWindow.put(input.sceneId,currentSceneIdAndTimeWindowKey);
                        }

                        if(currentSceneIdAndTimeWindowKey.timeWindow.getStart() > holdSceneIdAndTimeWindowKey.timeWindow.getStart()){
                            holdWindow.put(input.sceneId,currentSceneIdAndTimeWindowKey);
                            //要触发的时间窗口
                            SceneIdAndTimeWindowKey preSceneIdAndTimeWindowKey = new SceneIdAndTimeWindowKey();
                            preSceneIdAndTimeWindowKey.setValue(input.sceneId, input.actionTime - TIMEWINDOW_DELAY_MINUTE);
                            //对应场景的窗口链表
                            SceneIdAndTimeWindowKey triggerWindow = triggerWindowLink.get(input.sceneId).getNext();
                            logger.info("5分钟触发一次操作");
                            while(triggerWindow != null){

                                if(triggerWindow.timeWindow.getStart() > preSceneIdAndTimeWindowKey.timeWindow.getStart()){
                                    break;
                                }

                                CategoryMap categoryMap = minuteMapState.get(triggerWindow);
                                if(categoryMap == null){
                                    logger.info("categoryMap is null.");
                                    minuteMaxWindowEndtime.put(triggerWindow.sceneId, triggerWindow.timeWindow.getEnd());
                                    TriggerWindowLinkUtils.remove(triggerWindowLink.get(input.sceneId),triggerWindow);
                                    minuteMapState.remove(triggerWindow);
                                    logger.info("holdwindow 5分钟mapState:"+minuteMapState.keys());
                                    triggerWindow = triggerWindow.getNext();
                                    continue;
                                }

                                String[] rowKeyTimes = triggerWindow.getString();
                                String rowKeyMinute = rowKeyTimes[0];
                                String rowKeyMinute15 = rowKeyTimes[1];
                                String rowKeyHour = rowKeyTimes[2];
                                String rowKeyDay = rowKeyTimes[3];

                                hbaseUtils.putUserId(PropertiesUtil.RECOM_DASHBOARD_ACTION_UV_MINUTE,rowKeyMinute,"userId",categoryMap.getActionUsers());
                                hbaseUtils.putUserId(PropertiesUtil.RECOM_DASHBOARD_ACTION_UV_MINUTE15,rowKeyMinute15,"userId",categoryMap.getActionUsers());
                                hbaseUtils.putUserId(PropertiesUtil.RECOM_DASHBOARD_ACTION_UV_HOUR,rowKeyHour,"userId",categoryMap.getActionUsers());
                                hbaseUtils.putUserId(PropertiesUtil.RECOM_DASHBOARD_ACTION_UV_DAY,rowKeyDay,"userId",categoryMap.getActionUsers());


                                long uv = 0;
                                try {
                                    uv = hbaseUtils.getUv(PropertiesUtil.RECOM_DASHBOARD_ACTION_UV_MINUTE, rowKeyMinute);
                                } catch (Throwable throwable) {
                                    throwable.printStackTrace();
                                }
                                categoryMap.setUv(uv);

                                putCategoryMapToHBase(PropertiesUtil.RECOM_DASHBOARD_MINUTE,rowKeyMinute,categoryMap);
                                hbaseUtils.updateRecall(PropertiesUtil.RECOM_DASHBOARD_MINUTE,rowKeyMinute);

                                logger.info("holdwindow recalllogjob触发15分钟的操作");
                                try {

                                    restartMerge(1,rowKeyMinute15,triggerWindow,categoryMap,minute15MapState);
                                } catch (Throwable throwable) {
                                    throwable.printStackTrace();
                                }

                                logger.info("holdwindow recalllogjob触发一小时的操作");
                                try {

                                    merge(2,rowKeyHour,triggerWindow,categoryMap,hourMapState);
                                } catch (Throwable throwable) {
                                    throwable.printStackTrace();
                                }

                                logger.info("holdwindow recalllogjob触发一天的操作");

                                try {
                                    merge(3,rowKeyDay,triggerWindow,categoryMap,dayMapState);
                                } catch (Throwable throwable) {
                                    throwable.printStackTrace();
                                }

                                minuteMaxWindowEndtime.put(triggerWindow.sceneId, triggerWindow.timeWindow.getEnd());
                                TriggerWindowLinkUtils.remove(triggerWindowLink.get(input.sceneId),triggerWindow);
                                minuteMapState.remove(triggerWindow);
                                logger.info("holdwindow 5分钟mapState:"+minuteMapState.keys());
                                triggerWindow = triggerWindow.getNext();

                            }
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
            logger.info("categoryMap to HBase , categoryMap is null");
            return ;
        }
        long start = System.currentTimeMillis();
        List<String[]> calcResult = categoryMap.getCalcResult();
        hbaseUtils.addRecords(tablename,rowkey,calcResult);
        long end = System.currentTimeMillis();
        logger.info("write actionlogjob categorymap to hbase spend time: "+(end - start) + ",categorymap count:"+categoryMap.count());
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


