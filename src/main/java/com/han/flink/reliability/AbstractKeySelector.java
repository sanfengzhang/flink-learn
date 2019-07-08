package com.han.flink.reliability;

import org.apache.flink.api.java.functions.KeySelector;

/**
 * @author: Hanl
 * @date :2019/6/27
 * @desc:
 */
public class AbstractKeySelector<IN, KEY> implements KeySelector<IN, KEY> {

    @Override
    public KEY getKey(IN value) throws Exception {
        return null;
    }
}
