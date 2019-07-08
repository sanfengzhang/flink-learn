package com.han.flink.weibo.function;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Comparator;
import java.util.Map;
import java.util.TreeMap;

/**
 * @author: Hanl
 * @date :2019/7/5
 * @desc:
 */
public class GlobalTopNFunction extends ProcessAllWindowFunction<Tuple3<String, Integer, Long>, Tuple3<String, Integer, Long>, TimeWindow> {

    private static final Logger logger = LoggerFactory.getLogger(GlobalTopNFunction.class);

    private int topNSize;

    public GlobalTopNFunction(int topNSize) {

        this.topNSize = topNSize;
    }

    @Override
    public void process(Context context, Iterable<Tuple3<String, Integer, Long>> elements, Collector<Tuple3<String, Integer, Long>> out) throws Exception {

        //TODO 窗口计算都会面临一个问题，就是当数据量大的时候该如何去优化和解决
        long start = System.currentTimeMillis();
        TreeMap<Integer, Tuple3<String, Integer, Long>> treemap = new TreeMap<Integer, Tuple3<String, Integer, Long>>(
                new Comparator<Integer>() {

                    @Override
                    public int compare(Integer y, Integer x) {

                        return (x < y) ? -1 : 1;
                    }

                }); //treemap按照key降序排列，相同count值不覆盖

        for (Tuple3<String, Integer, Long> element : elements) {
            treemap.put(element.f1, element);
            if (treemap.size() > topNSize) { //只保留前面TopN个元素
                treemap.pollLastEntry();
            }
        }

        for (Map.Entry<Integer, Tuple3<String, Integer, Long>> entry : treemap
                .entrySet()) {
            out.collect(entry.getValue());
        }
        long end = System.currentTimeMillis();
        logger.info("end proccess window costtime time={}ms,threadId={}", end - start,Thread.currentThread().getId());

    }
}
