package com.han.flink;

import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.junit.Test;

import java.util.Date;

/**
 * @author: Hanl
 * @date :2019/7/25
 * @desc:
 */
public class TimeWindowTest {
    @Test
    public void testWindow() {
        long time = new Date().getTime();
        long start = TimeWindow.getWindowStartWithOffset(time, 0, 10);
        System.out.println(time);
        System.out.println(start);
        System.out.println(TimeWindow.getWindowStartWithOffset(time+12, 0, 10));

    }

}
