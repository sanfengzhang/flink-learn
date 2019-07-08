package com.han.flink.weibo.common;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.EventTimeTrigger;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

import java.util.Calendar;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;

/**
 * @author: Hanl
 * @date :2019/7/5
 * @desc:
 */
public class WeiboWindowAssigner extends WindowAssigner<Object, TimeWindow> {

    private static final long serialVersionUID = 1L;

    private final long size;

    private final long offset;

    public WeiboWindowAssigner(long size, long offset) {
        if (Math.abs(offset) >= size) {
            throw new IllegalArgumentException("WeiboWindowAssigner parameters must satisfy abs(offset) < size");
        }

        this.size = size;
        this.offset = offset;
    }

    @Override
    public Collection<TimeWindow> assignWindows(Object element, long timestamp, WindowAssignerContext context) {
        if (timestamp > Long.MIN_VALUE) {
            // Long.MIN_VALUE is currently assigned when no timestamp is present

            Date date=new Date(timestamp);
            Calendar calendar = Calendar.getInstance();//日历对象
            calendar.setTime(date);//设置当前日期
            calendar.set(Calendar.HOUR_OF_DAY,0);
            calendar.set(Calendar.MINUTE,0);
            calendar.set(Calendar.SECOND,0);
            calendar.set(Calendar.MILLISECOND,0);
            Date dayStart = calendar.getTime();//获取时间所在的日，2019-07-05 00:00:00.000,作为窗口的其实时间

            long start = dayStart.getTime();
            return Collections.singletonList(new TimeWindow(start, start + size));
        } else {
            throw new RuntimeException("Record has Long.MIN_VALUE timestamp (= no timestamp marker). " +
                    "Is the time characteristic set to 'ProcessingTime', or did you forget to call " +
                    "'DataStream.assignTimestampsAndWatermarks(...)'?");
        }
    }

    @Override
    public Trigger<Object, TimeWindow> getDefaultTrigger(StreamExecutionEnvironment env) {
        return EventTimeTrigger.create();
    }

    @Override
    public String toString() {
        return "WeiboWindowAssigner(" + size + ")";
    }


    public static WeiboWindowAssigner of(Time size) {
        return new WeiboWindowAssigner(size.toMilliseconds(), 0);
    }


    @Override
    public TypeSerializer<TimeWindow> getWindowSerializer(ExecutionConfig executionConfig) {
        return new TimeWindow.Serializer();
    }

    @Override
    public boolean isEventTime() {
        return true;
    }
}
