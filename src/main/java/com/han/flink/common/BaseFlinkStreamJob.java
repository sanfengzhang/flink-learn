package com.han.flink.common;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public abstract class BaseFlinkStreamJob {

    public abstract StreamExecutionEnvironment createEnv();


}
