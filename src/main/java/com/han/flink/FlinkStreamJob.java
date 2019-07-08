package com.han.flink;

import com.han.flink.common.BaseFlinkStreamJob;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @author: Hanl
 * @date :2019/5/24
 * @desc:
 */
public class FlinkStreamJob extends BaseFlinkStreamJob {

    @Override
    public StreamExecutionEnvironment createEnv() {
        // set up the execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        System.out.println("Executing WordCount example with default input data set.");
        System.out.println("Use --input to specify file input.");
        // get default test text data
        DataStream<String> text = env.fromElements(null);


        DataStream<Tuple2<String, Integer>> counts =
                // split up the lines in pairs (2-tuples) containing: (word,1)
                text.flatMap(new Tokenizer())
                        // group by the tuple field "0" and sum up tuple field "1"
                        .keyBy(0).sum(1);


        System.out.println("Printing result to stdout. Use --output to specify output path.");
        counts.print();


        return env;

    }

    public static final class Tokenizer implements FlatMapFunction<String, Tuple2<String, Integer>> {

        @Override
        public void flatMap(String value, Collector<Tuple2<String, Integer>> out) {
            // normalize and split the line
            String[] tokens = value.toLowerCase().split("\\W+");

            // emit the pairs
            for (String token : tokens) {
                if (token.length() > 0) {
                    out.collect(new Tuple2<>(token, 1));
                }
            }
        }
    }
}
