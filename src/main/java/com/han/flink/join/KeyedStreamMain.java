package com.han.flink.join;

import com.han.flink.weibo.WeiBo;
import com.han.flink.weibo.WeiboEntityStream;
import com.han.flink.weibo.source.TextWeiboEntityStream;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @author: Hanl
 * @date :2019/7/10
 * @desc:
 */
public class KeyedStreamMain {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // job失败重启的策略
        env.getConfig().setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 1000L));
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // 设置合理的CP的时间也是需要考量的
        env.getCheckpointConfig().setCheckpointInterval(10000l);
        env.getCheckpointConfig().setCheckpointTimeout(5000L);
        env.setParallelism(4);

        WeiboEntityStream weiboEntityStream = new TextWeiboEntityStream("D:\\eclipse-workspace\\flink-dev\\flink-app-platform\\src\\main\\resources\\weibo.txt");
        DataStream<WeiBo> weiboStream = weiboEntityStream.createDatastream(env);

        exampleUsageKeyedStream(weiboStream);

        env.execute();
    }

    public static void exampleUsageKeyedStream(DataStream<WeiBo> weiboStream) {
        /**
         * FIXME keyby就是可以实现sql中的分组计算！！！
         *  将DataStream按找uid进行keyby计算返回keyedStream，这里需要注意KeyedStream也是继承DataStream的，那么keyedStream也可以做
         *  哪些数据的M-R操作。这里所谓key是对流中的数据按某个字段进行有状态（valueState）的处理，这个在StreamGroupedReduce中可以看
         *  到它的实现。那么数据经过keyby的时候是如何选择channel发送的的？
         * Flink中是将key的Hashcode和并行度、最大并行度进行混合计算实现selectChannel的返回值，即当前数据写入到哪个channel。
         *
         * */
        weiboStream.map(new MapFunction<WeiBo, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(WeiBo value) throws Exception {
                return new Tuple2<String, Integer>(value.getUid(), value.getLike_count());
            }
        }).keyBy(0).sum(1).print();
    }

    public static void exampleUsageProcessFunction(DataStream<WeiBo> weiboStream) {

        weiboStream.map(new MapFunction<WeiBo, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(WeiBo value) throws Exception {
                return new Tuple2<String, Integer>(value.getUid(), value.getLike_count());
            }
        }).keyBy(0);
    }
}
