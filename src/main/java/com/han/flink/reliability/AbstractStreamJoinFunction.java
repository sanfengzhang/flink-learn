package com.han.flink.reliability;

import org.apache.flink.api.common.functions.JoinFunction;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * @author: Hanl
 * @date :2019/6/27
 * @desc: 双流Join是根据数据集的某些key进行Equal比较，然后再Flink中有一个
 * 窗口会缓存两个流的数据集合A、B，然后再进行将A,B两个集合中的数据进行自定义的
 * JoinFunction计算。它是根据判断两个流的keyA,keyB条件判断的时候将它们放在同一个窗口中
 * ，然后到了窗口触发时间就开始计算了。但是源代码中当B没有数据的时候不会触发JoinFunction计算,
 * 所以本需求改动是将那些没有找到关联的数据做一个业务处理。
 * 1.为了实时了解Join的状态，我们可以通过mertic查看有多少数据没关联上、
 **/
public abstract class AbstractStreamJoinFunction<IN1, IN2, OUT> implements JoinFunction<IN1, IN2, OUT> {

    private List<IN1> cacheData = new CopyOnWriteArrayList<IN1>();

    private int cacheSize = 0;

    //单位M
    private int CACHE_SIZE_LIMIT = 1024;

    //无论有没有关联上对于IN1的流的数据总会向下游发送的
    private boolean alwaysSend;

    //会将当前cacheData的数据和IN2流当前窗口数据中参与计算
    private boolean layzJoin = true;

    @Override
    public OUT join(IN1 first, IN2 second) throws Exception {
        //这个表示当前IN1流中的数据，在当前计算窗口中没有找到符合条件的IN2数据
        if (null == second) {
            //添加到本地的缓存中,或者做一些补偿性的操作记录下来那些数据没有关联起来
            add(first);
        }
        return handle(first, second);
    }

    public abstract OUT handle(IN1 first, IN2 second) throws Exception;

    public void add(IN1 in1) {
        byte[] bytes = in1.toString().getBytes();
        cacheSize += bytes.length;
        if (cacheSize < CACHE_SIZE_LIMIT) {
            cacheData.add(in1);
        }
        //FIXME 超过大小采取什么样的策略去处理，在业务上一般出现这种情况:主要还是
        //数据延迟,上游业务到数据源的过程中数据丢失这样的情况不考虑，主要还是在流计算服务
        //过程中的数据延迟考虑。但是如果真的没有数据过来一直..那么将早期的数据做其他方面的
        //业务容错处理
    }

    public boolean isAlwaysSend() {
        return alwaysSend;
    }
}
