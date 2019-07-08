package com.han.flink.weibo.source;

import com.han.flink.common.CommonMessage;
import com.han.flink.weibo.WeiBo;
import com.han.flink.weibo.WeiboEntityStream;
import com.han.flink.weibo.function.MessageToWeboFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author: Hanl
 * @date :2019/7/5
 * @desc:
 */
public class SocketWeiboEntityStream implements WeiboEntityStream {

    @Override
    public DataStream<WeiBo> createDatastream(StreamExecutionEnvironment env) {
        DataStream<String> source = env.socketTextStream("127.0.0.1", 8085).name("read messgae from socket");

        DataStream<WeiBo> commonMessageDataStream = source.map(new MapFunction<String, CommonMessage>() {
            private static final long serialVersionUID = 1L;

            @Override
            public CommonMessage map(String value) throws Exception {
                CommonMessage message = new CommonMessage("wei-bo", value);
                return message;
            }
        }).name("String -->CommonMessage").map(new MessageToWeboFunction()).name("CommonMessage -->Weibo");
        return commonMessageDataStream;
    }
}
