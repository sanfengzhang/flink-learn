package com.han.flink;

import com.han.flink.common.sink.JDBCOutputFormat;
import com.han.flink.weibo.WeiBo;
import com.han.flink.weibo.WeiboEntityStream;
import com.han.flink.weibo.source.KafkaWeiboEntityStream;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.OutputFormatSinkFunction;
import org.apache.flink.types.Row;

/**
 * @author: Hanl
 * @date :2019/7/23
 * @desc:
 */
public class FlinkBackPressure {

    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // job失败重启的策略
        env.getConfig().setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 1000L));
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.disableOperatorChaining();

        // 设置合理的CP的时间也是需要考量的
        env.getCheckpointConfig().setCheckpointInterval(10000l);
        env.getCheckpointConfig().setCheckpointTimeout(5000L);
        env.setParallelism(2);

        WeiboEntityStream weiboEntityStream = new KafkaWeiboEntityStream();
        DataStream<WeiBo> weiboStream = weiboEntityStream.createDatastream(env);

        addMysqlSink(weiboStream);

        env.execute();

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
                Thread.sleep(1000L);

                return row;
            }
        });
        JDBCOutputFormat jdbcOutputFormat = JDBCOutputFormat.buildJDBCOutputFormat().setDrivername("com.mysql.jdbc.Driver")
                .setDBUrl("jdbc:mysql://10.91.18.27:3306/han?useUnicode=true")
                .setUsername("root")
                .setPassword("1234").setQuery("insert into t_weibo (uid,mid,time,forward_count,comment_count,like_count) values(?,?,?,?,?,?)")
                .setSqlTypes(new int[]{12, 12, 93, 4, 4, 4}).setBatchInterval(10).finish();

        mysqlRowDataStream.addSink(new OutputFormatSinkFunction<Row>(jdbcOutputFormat));

    }
}
