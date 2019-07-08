package com.han.flink.weibo;

import com.han.flink.common.DefaultAssignerWithPeriodicWatermarks;
import com.han.flink.common.sink.JDBCOutputFormat;
import com.han.flink.weibo.common.HashKeySelector;
import com.han.flink.weibo.common.WeboWindowTrigger;
import com.han.flink.weibo.common.WeiboWindowAssigner;
import com.han.flink.weibo.function.GlobalTopNFunction;
import com.han.flink.weibo.function.KeyedGlobalTopNFunction;
import com.han.flink.weibo.source.TextWeiboEntityStream;
import org.apache.commons.lang3.time.DateUtils;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.AllWindowedStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.OutputFormatSinkFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.types.Row;

import java.text.SimpleDateFormat;
import java.util.Date;

public class WeiboStreamJob {

    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // job失败重启的策略
        env.getConfig().setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 1000L));
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // 设置合理的CP的时间也是需要考量的
        env.getCheckpointConfig().setCheckpointInterval(10000l);
        env.getCheckpointConfig().setCheckpointTimeout(5000L);
        env.setParallelism(4);

        WeiboEntityStream weiboEntityStream = new TextWeiboEntityStream();
        DataStream<WeiBo> weiboStream = weiboEntityStream.createDatastream(env);

        //execSumByUid(weiboStream);

//        execLikeCountTopNGlobal(weiboStream);


        //addMysqlSink(weiboStream);

//         execLikeCountTopNGlobalByWindowAssigner(weiboStream);

        execLikeCountTopNGlobalByKeyByAndWindowAssigner(weiboStream);


        env.execute("WeiBo Job");
    }

    public static void addMysqlSink(DataStream<WeiBo> weiboStream) {
        DataStream<Row> mysqlRowDataStream = weiboStream.map(new MapFunction<WeiBo, Row>() {
            @Override
            public Row map(WeiBo value) throws Exception {

                Row row = new Row(6);
                row.setField(0, value.getUid());
                row.setField(1, value.getMid());
                row.setField(2, value.getTime());
                row.setField(3, value.getForward_count());
                row.setField(4, value.getComment_count());
                row.setField(5, value.getLike_count());
                //row.setField(6, value.getContent());

                return row;
            }
        });
        JDBCOutputFormat jdbcOutputFormat = JDBCOutputFormat.buildJDBCOutputFormat().setDrivername("com.mysql.jdbc.Driver")
                .setDBUrl("jdbc:mysql://localhost:3306/han?useUnicode=true")
                .setUsername("root")
                .setPassword("1234").setQuery("insert into t_weibo (uid,mid,time,forward_count,comment_count,like_count) values(?,?,?,?,?,?)")
                .setSqlTypes(new int[]{12, 12, 93, 4, 4, 4}).setBatchInterval(1000).finish();

        mysqlRowDataStream.addSink(new OutputFormatSinkFunction<Row>(jdbcOutputFormat));

    }

    public static void execSumByUid(DataStream<WeiBo> weiboStream) {
        //--------计算按UID统计每个用户发了多少条微博
        weiboStream.map(new MapFunction<WeiBo, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(WeiBo value) throws Exception {
                return new Tuple2<>(value.getUid(), 1);
            }
        }).name("WeiBo --->Tuple").keyBy(0).sum(1).print();
    }

    public static void execLikeCountTopNGlobal(DataStream<WeiBo> weiboStream) {
        //----------计算按天统计喜欢数量的TOPN的微博,计算窗口为时间翻滚窗口

        DataStream<Tuple3<String, Integer, Long>> dataStream = weiboStream.map(new MapFunction<WeiBo, Tuple3<String, Integer, Long>>() {
            @Override
            public Tuple3<String, Integer, Long> map(WeiBo value) throws Exception {
                Date date = DateUtils.parseDate(value.getTime(), "yyyy-MM-dd HH:mm:ss");
                return new Tuple3<>(value.getMid(), value.getLike_count(), date.getTime());
            }
        }).name("WeiBo --->Tuple").assignTimestampsAndWatermarks(new DefaultAssignerWithPeriodicWatermarks());

        //这里即使实现自己的trigger还是不能获得正确的结果,因为Flink的时间翻滚窗口是按数据中eventTime+winSize大小来创建窗口
        //所以如果想严格按照[2015-02-23,2015-02-24...],使用Flink自带的窗口、API是不能满足的，所以需要自己实现WindowAssigner

        AllWindowedStream<Tuple3<String, Integer, Long>, TimeWindow> allWindowedStream = dataStream.timeWindowAll(Time.days(1L)).trigger(new WeboWindowTrigger());

        allWindowedStream.process(new GlobalTopNFunction(3)).map(new MapFunction<Tuple3<String, Integer, Long>, Tuple3<String, Integer, String>>() {
            @Override
            public Tuple3<String, Integer, String> map(Tuple3<String, Integer, Long> value) throws Exception {
                Date date = new Date(value.f2);
                SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
                return new Tuple3<>(value.f0, value.f1, simpleDateFormat.format(date));
            }
        }).print();
    }

    public static void execLikeCountTopNGlobalByWindowAssigner(DataStream<WeiBo> weiboStream) {
        //----------计算按天统计喜欢数量的TOPN的微博,计算窗口为时间翻滚窗口

        DataStream<Tuple3<String, Integer, Long>> dataStream = weiboStream.map(new MapFunction<WeiBo, Tuple3<String, Integer, Long>>() {
            @Override
            public Tuple3<String, Integer, Long> map(WeiBo value) throws Exception {
                Date date = DateUtils.parseDate(value.getTime(), "yyyy-MM-dd HH:mm:ss");
                return new Tuple3<>(value.getMid(), value.getLike_count(), date.getTime());
            }
        }).name("WeiBo --->Tuple").assignTimestampsAndWatermarks(new DefaultAssignerWithPeriodicWatermarks());

        //这里即使实现自己的trigger还是不能获得正确的结果,因为Flink的时间翻滚窗口是按数据中eventTime+winSize大小来创建窗口
        //所以如果想严格按照[2015-02-23,2015-02-24...],使用Flink自带的窗口、API是不能满足的，所以需要自己实现WindowAssigner
        //这个时候只需要改写数据窗口分配规则就可以了，Trigger就使用原声的EventTimeTrigger就可以了

        AllWindowedStream<Tuple3<String, Integer, Long>, TimeWindow> allWindowedStream = dataStream.windowAll(WeiboWindowAssigner.of(Time.seconds(86400)));

        allWindowedStream.process(new GlobalTopNFunction(3)).map(new MapFunction<Tuple3<String, Integer, Long>, Tuple3<String, Integer, String>>() {
            @Override
            public Tuple3<String, Integer, String> map(Tuple3<String, Integer, Long> value) throws Exception {
                Date date = new Date(value.f2);
                SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
                return new Tuple3<>(value.f0, value.f1, simpleDateFormat.format(date));
            }
        }).print();
    }

    //FIXME 上面的两个统计案例，是计算当天所有用户的微博中点赞数量排名前TOPN的计数统计、这样所有的数据会在一个窗口任务中计算带来，资源压力
    //过于集中、那么需要缓解这种压力，我们可以先按文章名称进行keyBy计算然后用窗口计算、这样先将数据打散分布再不同计算节点上，再将每个节点计算的结果
    //汇集在一起作最后的计算。

    public static void execLikeCountTopNGlobalByKeyByAndWindowAssigner(DataStream<WeiBo> weiboStream) {
        DataStream<Tuple3<String, Integer, Long>> dataStream = weiboStream.map(new MapFunction<WeiBo, Tuple3<String, Integer, Long>>() {
            @Override
            public Tuple3<String, Integer, Long> map(WeiBo value) throws Exception {
                Date date = DateUtils.parseDate(value.getTime(), "yyyy-MM-dd HH:mm:ss");
                return new Tuple3<>(value.getMid(), value.getLike_count(), date.getTime());
            }
        }).name("WeiBo --->Tuple").assignTimestampsAndWatermarks(new DefaultAssignerWithPeriodicWatermarks());

        KeyedStream<Tuple3<String, Integer, Long>, Integer> keyedStream = dataStream.keyBy(new HashKeySelector(3));

        //按Key进行window计算
        WindowedStream<Tuple3<String, Integer, Long>, Integer, TimeWindow> windowedStream = keyedStream.window(WeiboWindowAssigner.of(Time.seconds(86400)));

        //每个分组经过窗口计算后将结果进行合并,然后再对dataStream1进行窗口计算
        DataStream<Tuple3<String, Integer, Long>> dataStream1 = windowedStream.process(new KeyedGlobalTopNFunction(3));

        dataStream1.print();

        //--------------和上面的处理逻辑是一致的-----------------
        AllWindowedStream<Tuple3<String, Integer, Long>, TimeWindow> allWindowedStream = dataStream1.windowAll(WeiboWindowAssigner.of(Time.seconds(86400)));
        allWindowedStream.process(new GlobalTopNFunction(3)).map(new MapFunction<Tuple3<String, Integer, Long>, Tuple3<String, Integer, String>>() {
            @Override
            public Tuple3<String, Integer, String> map(Tuple3<String, Integer, Long> value) throws Exception {
                Date date = new Date(value.f2);
                SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
                return new Tuple3<>(value.f0, value.f1, simpleDateFormat.format(date));
            }
        }).print();
    }

    //FIXME 还做几个需求分析：1、一个是Join的操作分析.2、一个是外部维度表的解决方案可能需要异步流+LRU的缓存策略
    //3、以流计算的方式实现一个热点微博的发现、也就是点击率、转发率最高的几条微博，把它和业务访问系统关联起来做一个缓存
    //击穿的防范措施
}
