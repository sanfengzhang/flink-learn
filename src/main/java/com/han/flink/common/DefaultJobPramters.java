package com.han.flink.common;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.flink.api.common.ExecutionConfig.GlobalJobParameters;

public class DefaultJobPramters extends GlobalJobParameters {

	private static final long serialVersionUID = 1L;

	private Map<String, String> parameters = null;

	public DefaultJobPramters() {
		this.parameters = new ConcurrentHashMap<String, String>();
	}

	public void put(String key, String value) {
		parameters.put(key, value);
	}

	@Override
	public Map<String, String> toMap() {
		return parameters;
	}

}
