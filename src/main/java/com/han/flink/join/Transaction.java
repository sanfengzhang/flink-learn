package com.han.flink.join;

import java.io.Serializable;

/**
 * @author: Hanl
 * @date :2019/7/10
 * @desc:
 */
public class Transaction implements Serializable {

    private int id;//交易唯一ID

    private String uid;//用户ID

    private long transTime;//该交易发生的时间

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

    public long getTransTime() {
        return transTime;
    }

    public void setTransTime(long transTime) {
        this.transTime = transTime;
    }
}
