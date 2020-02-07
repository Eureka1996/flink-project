package com.wufuqiang.flink.myprocess;

import com.wufuqiang.flink.entries.CountWithTimestamp;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * 维护了计数和超时间隔的ProcessFunction实现
 */
public class CountWithTimeoutFunction extends KeyedProcessFunction<Tuple, Tuple2<String, String>, Tuple2<String, Long>> {
    /** 这个状态是通过 ProcessFunction 维护*/
    private ValueState<CountWithTimestamp> state;

    @Override
    public void open(Configuration parameters) throws Exception {
        state = getRuntimeContext().getState(new ValueStateDescriptor<>("myState", CountWithTimestamp.class));
    }

    @Override
    public void processElement(
            Tuple2<String, String> value,
            Context ctx,
            Collector<Tuple2<String, Long>> out) throws Exception {

        // 查看当前计数
        CountWithTimestamp current = state.value();
        if (current == null) {
            current = new CountWithTimestamp();
            current.key = value.f0;
        }

        // 更新状态中的计数
        current.count++;

        // 设置状态的时间戳为记录的事件时间时间戳
        current.lastModified = System.currentTimeMillis();

        // 状态回写
        state.update(current);

//        System.out.println(ctx.getCurrentKey());
        // 从当前事件时间开始注册一个60s的定时器
        //同一个key，同一个时间戳只有一个定时器
        ctx.timerService().registerProcessingTimeTimer(System.currentTimeMillis()/(30*1000)*(30*1000)+30000);


//        ctx.timerService().registerEventTimeTimer(current.lastModified + 20000);
    }

    public void onTimer(
            long timestamp,
            OnTimerContext ctx,
            Collector<Tuple2<String, Long>> out) throws Exception {

        // 得到设置这个定时器的键对应的状态
        CountWithTimestamp result = state.value();
//        System.out.println("onTimer timestamp:"+(timestamp-result.lastModified));

        // 检查定时器是过时定时器还是最新定时器
            // emit the state on timeout

            out.collect(new Tuple2<String, Long>(result.key, result.count));
    }
}