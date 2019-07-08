package com.han.json;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.io.IOUtils;
import org.junit.Test;

import com.alibaba.fastjson.JSON;
import com.han.flink.common.DefaultJobPramters;
import com.han.flink.common.function.KafkaMessageAnanlyseFunction;

public class JsonTest {

    @Test
    public void testRegexWrong() {
        String badRegex = "^([hH][tT]{2}[pP]://|[hH][tT]{2}[pP][sS]://)(([A-Za-z0-9-~]+).)+([A-Za-z0-9-~\\/])+$";
        String bugUrl = "http://www.fapiao.com/dddp-web/pdf/download?request=6e7JGxxxxx4ILd-kExxxxxxxqJ4-CHLmqVnenXC692m74H38sdfdsazxcUmfcOH2fAfY1Vw__%5EDadIfJgiEf";
        if (bugUrl.matches(badRegex)) {
            System.out.println("match!!");
        } else {
            System.out.println("no match!!");
        }
    }

    @Test
    public void testRegexRight() {
        String badRegex = "^([hH][tT]{2}[pP]://|[hH][tT]{2}[pP][sS]://)(([A-Za-z0-9-~]+).)++([A-Za-z0-9-~\\/])+$";
        String bugUrl = "http://www.fapiao.com/dddp-web/pdf/download?request=6e7JGxxxxx4ILd-kExxxxxxxqJ4-CHLmqVnenXC692m74H38sdfdsazxcUmfcOH2fAfY1Vw__%5EDadIfJgiEf";
        if (bugUrl.matches(badRegex)) {
            System.out.println("match!!");
        } else {
            System.out.println("no match!!");
        }

    }

    @Test
    public void testJson() {

        FileInputStream fis = null;
        try {
            fis = new FileInputStream(new File("src/main/resources/trade.conf"));
            String value = IOUtils.toString(fis);
            System.out.println(value);
            Map<String, String> morphlineMap = new HashMap<String, String>();
            morphlineMap.put("trans_trade", value);

            DefaultJobPramters globalJobParameters = new DefaultJobPramters();

            globalJobParameters.put(KafkaMessageAnanlyseFunction.MORPHLINE_CONF_STRING,
                    JSON.toJSONString(morphlineMap));

            String json = globalJobParameters.toMap().get(KafkaMessageAnanlyseFunction.MORPHLINE_CONF_STRING);
            System.out.println(JSON.parseObject(json, Map.class).get("trans_trade"));

        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            IOUtils.closeQuietly(fis);
        }

    }

}
