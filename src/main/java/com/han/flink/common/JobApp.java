package com.han.flink.common;

import com.han.flink.FlinkStreamJob;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.springframework.core.io.Resource;
import org.springframework.core.io.support.PathMatchingResourcePatternResolver;
import org.springframework.core.type.classreading.CachingMetadataReaderFactory;
import org.springframework.core.type.classreading.MetadataReader;
import org.springframework.core.type.classreading.MetadataReaderFactory;

import java.util.concurrent.CountDownLatch;

/**
 * @author: Hanl
 * @date :2019/5/24
 * @desc:
 */
public class JobApp {

    public static void main(String args[]) throws Exception {
        BaseFlinkStreamJob baseFlinkStreamJob = new FlinkStreamJob();
        StreamExecutionEnvironment env = baseFlinkStreamJob.createEnv();
        env.execute();

    }
}
