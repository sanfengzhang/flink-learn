package com.han.flink.common;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;

import javax.annotation.Nullable;

/**
 * @author: Hanl
 * @date :2019/7/23
 * @desc:
 */
public class AutoIncreaseAssignerWithPeriodicWatermarks<T> implements AssignerWithPeriodicWatermarks<T> {

    private static final long serialVersionUID = 1L;

    private long currentWatermark = Long.MIN_VALUE;

    private long lastWatermarkChangtime = Long.MIN_VALUE;

    @Nullable
    @Override
    public Watermark getCurrentWatermark() {

        long currentTime = System.currentTimeMillis();



        Watermark watermark = new Watermark(currentWatermark);
        return watermark;
    }

    @Override
    public long extractTimestamp(T element, long previousElementTimestamp) {
        lastWatermarkChangtime = System.currentTimeMillis();
        return 0;
    }
}
