package com.han.flink.weibo.source;

import com.han.flink.common.CommonMessage;
import com.han.flink.common.DefaultKafkaDeserializationSchema;
import com.han.flink.weibo.WeiBo;
import com.han.flink.weibo.WeiboEntityStream;
import com.han.flink.weibo.function.MessageToWeboFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;

import java.util.Properties;

/**
 * @author: Hanl
 * @date :2019/7/5
 * @desc:
 */
public class KafkaWeiboEntityStream implements WeiboEntityStream {

    @Override
    public DataStream<WeiBo> createDatastream(StreamExecutionEnvironment env) {
        // -----TODO--需要指定和zk之间的sessionTimeOut、心跳时长这些参数、还可以指定是否开启kafka-metric
        Properties props = new Properties();
        props.put("bootstrap.servers", "192.168.12.100:9092");
        props.put("group.id", "weibo-consumer-1");
        props.put("auto.offset.reset", "earliest");

        DataStreamSource<CommonMessage> source = env.addSource(
                new FlinkKafkaConsumer010("test-leader", new DefaultKafkaDeserializationSchema(), props),
                "weibo-kafka-source");

        DataStream<WeiBo> commonMessageDataStream = source.map(new MessageToWeboFunction()).name("CommonMessage -->Weibo");
        return commonMessageDataStream;
    }
}
