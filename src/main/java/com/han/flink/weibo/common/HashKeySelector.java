package com.han.flink.weibo.common;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple3;

/**
 * @author: Hanl
 * @date :2019/7/8
 * @desc:
 */
public class HashKeySelector implements KeySelector<Tuple3<String, Integer, Long>, Integer> {

    private int mod;

    public HashKeySelector() {

    }

    public HashKeySelector(int mod) {
        this.mod = mod;
    }

    @Override
    public Integer getKey(Tuple3<String, Integer, Long> value) throws Exception {
        String key = value.f0;
        int hasCode = key.hashCode();
        Integer result = hasCode % mod;
        return result;
    }
}
