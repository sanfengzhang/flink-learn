package com.han.flink.weibo;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author: Hanl
 * @date :2019/7/5
 * @desc:
 */
public interface WeiboEntityStream {

    public DataStream<WeiBo> createDatastream(StreamExecutionEnvironment env);
}
