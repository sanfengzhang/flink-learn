package com.han.flink;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.io.IOUtils;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import com.alibaba.fastjson.JSON;
import com.han.flink.common.CommonMessage;
import com.han.flink.common.DefaultJobPramters;
import com.han.flink.common.function.KafkaMessageAnanlyseFunction;

public class MorphlineMain {

    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // job失败重启的策略
        env.getConfig().setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 30000L));

        // 设置合理的CP的时间也是需要考量的
        env.getCheckpointConfig().setCheckpointInterval(10000L);
        env.getCheckpointConfig().setCheckpointTimeout(5000L);
        env.setParallelism(1);

        FileInputStream fis = null;
        try {
            fis = new FileInputStream(new File("src/main/resources/trade.conf"));
            String value = IOUtils.toString(fis);
            Map<String, String> morphlineMap = new HashMap<String, String>();
            morphlineMap.put("trans_trade", value);

            DefaultJobPramters globalJobParameters = new DefaultJobPramters();

            globalJobParameters.put(KafkaMessageAnanlyseFunction.MORPHLINE_CONF_STRING,
                    JSON.toJSONString(morphlineMap));
            env.getConfig().setGlobalJobParameters(globalJobParameters);
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            IOUtils.closeQuietly(fis);
        }

        DataStream<String> dataStream = env.socketTextStream("127.0.0.1", 8085).name("read messgae from socket");

        DataStream<CommonMessage> messageStream = dataStream.map(new MapFunction<String, CommonMessage>() {
            private static final long serialVersionUID = 1L;

            @Override
            public CommonMessage map(String value) throws Exception {
                CommonMessage message = new CommonMessage("trans_trade", value);

                return message;
            }
        }).name("transnat to CommonMessage");

        DataStream<Map<String, Object>> mapStream = messageStream.map(new KafkaMessageAnanlyseFunction())
                .name("morphline analyse");


        mapStream.print();

        env.execute();

    }

}
