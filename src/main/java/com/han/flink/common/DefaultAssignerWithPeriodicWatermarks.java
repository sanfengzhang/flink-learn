package com.han.flink.common;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;

/**
 * @author: Hanl
 * @date :2019/7/5
 * @desc:
 */
public class DefaultAssignerWithPeriodicWatermarks implements AssignerWithPeriodicWatermarks<Tuple3<String, Integer, Long>> {

    private static final long serialVersionUID = 1L;

    private long currentTimestamp = Long.MIN_VALUE;

    @Override
    public long extractTimestamp(Tuple3<String, Integer, Long> element, long previousElementTimestamp) {
        final long newTimestamp = element.f2;
        if (newTimestamp >= this.currentTimestamp) {
            this.currentTimestamp = newTimestamp;
            return newTimestamp;
        }
        return newTimestamp;
    }

    @Override
    public Watermark getCurrentWatermark() {

        return new Watermark(currentTimestamp == Long.MIN_VALUE ? Long.MIN_VALUE : currentTimestamp-1000);
    }
}