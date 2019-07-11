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
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.sink.OutputFormatSinkFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import java.text.SimpleDateFormat;
import java.util.Date;

public class WeiboStreamJob {

    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // job失败重启的策略
        env.getConfig().setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 1000L));
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.disableOperatorChaining();

        // 设置合理的CP的时间也是需要考量的
        env.getCheckpointConfig().setCheckpointInterval(10000l);
        env.getCheckpointConfig().setCheckpointTimeout(5000L);
        env.setParallelism(1);

        WeiboEntityStream weiboEntityStream = new TextWeiboEntityStream("D:/dataset/Weibo_Data/weibo.txt");
        DataStream<WeiBo> weiboStream = weiboEntityStream.createDatastream(env);

        //execSumByUid(weiboStream);

        // execLikeCountTopNGlobal(weiboStream);

        //addMysqlSink(weiboStream);

        //execLikeCountTopNGlobalByWindowAssigner(weiboStream);

//        execLikeCountTopNGlobalByKeyByAndWindowAssigner(weiboStream);

        exampleUsageProcessFunction(weiboStream);

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
    //汇集在一起作最后的计算。预聚合计算怎样的字段才能起到比较好的效果？我们知道keyby是用了hash计算，所以如何让数据比较均匀的分布在task的并行度上
    //是比较合理做法。
    // 预聚合并没有让需要的数据量减少了，回到当前案例：查询所有微博TOPN，我们可以先按用户名或者微博id进行keyby（在keyby的时候如何分散压力）
    //比如可以增加keyby时候的节点计算并行度，并且在每一个节点计算一部分的微博的的TOPN，然后再将各分组的TOPN放在一个窗口中再次计算TOPN达到分散减压的目的

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

    public static void exampleUsageProcessFunction(DataStream<WeiBo> weiboStream) {
        weiboStream.map(new MapFunction<WeiBo, Tuple3<String, Integer, Long>>() {
            @Override
            public Tuple3<String, Integer, Long> map(WeiBo value) throws Exception {
                Date date = DateUtils.parseDate(value.getTime(), "yyyy-MM-dd HH:mm:ss");
                Thread.sleep(3000);
                return new Tuple3<>(value.getMid(), value.getLike_count(), date.getTime());
            }
        }).name("WeiBo --->Tuple").assignTimestampsAndWatermarks(new DefaultAssignerWithPeriodicWatermarks()).
                process(new ProcessFunction<Tuple3<String, Integer, Long>, Tuple3<String, Integer, Long>>() {
                    private long lastProcessTime = Long.MIN_VALUE;

                    @Override
                    public void processElement(Tuple3<String, Integer, Long> value, Context ctx, Collector<Tuple3<String, Integer, Long>> out) throws Exception {

                        out.collect(value);
                        long currentTime = System.currentTimeMillis();
                        if (lastProcessTime == Long.MIN_VALUE) {
                            lastProcessTime = currentTime;
                        }
                        //TODO 注意当前是不支持注册的{@ProcessOperator#ContextImpl}这个内部类没有实现注册timer的方法,
                        //Setting timers is only supported on a keyed streams
                        //ctx.timerService().registerEventTimeTimer(currentTime + 20000)

                        //FIXME 在获取waterMark的时候，用案例的时候因为数据少量，且不长时间持续，因为水印是200ms才发送一次，
                        // 测试数据可能在200ms内就已经执行完了，程序结束(这个数据源是文件的).所以加大数据发送时间间隔才能看的出来
                        // 否则还以为是出了啥问题!!
                        /**
                         * 输出结果：
                         * =====================================58 || -9223372036854775808
                         * =====================================58 || -9223372036854775808
                         * =====================================58 || -9223372036854775808
                         * =====================================58 || -9223372036854775808
                         * =====================================58 || -9223372036854775808
                         * =====================================58 || -9223372036854775808
                         * =====================================58 || 1424707198000
                         * =====================================58 || 1424707198000
                         * =====================================58 || 1424707198000
                         * =====================================58 || 1424707198000
                         * 对上述结果分析一下为什么是连续输出相同的结果，我们需要理解该Operator生成WaterMark的机制，TimestampsAndPeriodicWatermarksOperator是
                         * 根据定时任务来发送WaterMark的,默认是200ms触发一次，注意，只有userFunction.getCurrentWaterMark>currentWatermark时候才会
                         * emit水印消息！！！那么userFunction.getCurrentWaterMark实质上是只有新的数据才会去更新实例对象中的waterMark，所以再一段时间内，WaterMark是
                         * 没有发生任何变化的。上面的观察还是存在问题，那就是明明程序已经更新了呀，为什么结果输出的这个鬼样子，按照当前程序不是每处理一条数据waterMark应该会更新的
                         * 嘛？关键就在于 env.disableOperatorChaining();这个要禁用掉输出结果就是如下：
                         *=====================================1562831310559||61 || -9223372036854775808
                         * =====================================1562831313681||61 || 1424684488000
                         * =====================================1562831316697||61 || 1424684997000
                         * =====================================1562831319618||61 || 1424685485000
                         * =====================================1562831322631||61 || 1424695196000
                         * =====================================1562831325648||61 || 1424696557000
                         * =====================================1562831328664||61 || 1424707198000
                         * =====================================1562831331702||61 || 1424711783000
                         * =====================================1562831334709||61 || 1424744696000
                         * =====================================1562831337506||61 || 1424747015000
                         * 上面这个结果就是期望的结果O(∩_∩)O，上面的问题思考了好久，才发现这个问题。很可能是这些oprator被chain在一起了带来的问题。
                         * 将它指定为不chain在一起就好了。
                         *
                         */
                        System.out.println("=====================================" +System.currentTimeMillis()+"||" +Thread.currentThread().getId() + " || " + ctx.timerService().currentWatermark());
                    }
                });
    }

}
