package com.han.flink.common;

import java.io.Serializable;

public class CommonMessage implements Serializable {

	private static final long serialVersionUID = 1L;

	private String type;

	private String value;

	public CommonMessage() {

	}

	public CommonMessage(String type, String value) {
		this.type = type;
		this.value = value;
	}

	public String getType() {
		return type;
	}

	public void setType(String type) {
		this.type = type;
	}

	public String getValue() {
		return value;
	}

	public void setValue(String value) {
		this.value = value;
	}

}
