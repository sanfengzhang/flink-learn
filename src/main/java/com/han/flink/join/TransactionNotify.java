package com.han.flink.join;

import java.io.Serializable;

/**
 * @author: Hanl
 * @date :2019/7/10
 * @desc:
 */
public class TransactionNotify implements Serializable {

    private int id;//通知表的唯一

    private String uid;

    private long notifyTime;

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getUid() {
        return uid;
    }

    public void setUid(String uid) {
        this.uid = uid;
    }

    public long getNotifyTime() {
        return notifyTime;
    }

    public void setNotifyTime(long notifyTime) {
        this.notifyTime = notifyTime;
    }
}
