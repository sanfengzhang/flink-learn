package com.han.flink.weibo.function;

import com.han.flink.common.CommonMessage;
import com.han.flink.common.DefaultKafkaMessage;
import com.han.flink.weibo.WeiBo;
import org.apache.flink.api.common.functions.MapFunction;

/**
 * @author: Hanl
 * @date :2019/7/5
 * @desc:
 */
public class MessageToWeboFunction implements MapFunction<CommonMessage, WeiBo> {
    private static final long serialVersionUID = 1L;

    public WeiBo map(CommonMessage value) throws Exception {
        DefaultKafkaMessage newValue = null;
        String data = null;
        if (value instanceof DefaultKafkaMessage) {

            newValue = (DefaultKafkaMessage) value;

            data = newValue.getValue();
        } else {
            data = value.getValue();
        }


        String[] datas = data.split("\\s{2,}|\t");

        WeiBo weiBo = new WeiBo();
        weiBo.setUid(datas[0]);
        weiBo.setMid(datas[1]);
        weiBo.setTime(datas[2]);
        weiBo.setForward_count(Integer.parseInt(datas[3].trim()));
        weiBo.setComment_count(Integer.parseInt(datas[4].trim()));
        weiBo.setLike_count(Integer.parseInt(datas[5].trim()));
        weiBo.setContent(datas[6]);

        return weiBo;
    }
}