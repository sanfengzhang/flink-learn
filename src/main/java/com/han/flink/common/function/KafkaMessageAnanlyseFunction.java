package com.han.flink.common.function;

import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.flink.api.common.ExecutionConfig.GlobalJobParameters;

import com.alibaba.fastjson.JSONObject;

public class KafkaMessageAnanlyseFunction extends AnalyseMorphlineFunction<Map<String, Object>> {

	private static final long serialVersionUID = 1L;
	

	@Override
	public Map<String, Object> transfer(Map<String, Collection<Object>> value) {

		Iterator<Entry<String, Collection<Object>>> it = value.entrySet().iterator();
		Map<String, Object> resultMap = new HashMap<String, Object>();

		while (it.hasNext()) {
			Entry<String, Collection<Object>> en = it.next();

			if (en.getKey().equals("message")) {
				continue;
			}

			resultMap.put(en.getKey(), en.getValue().iterator().next());
		}

		return resultMap;
	}

	@Override
	protected Map<String, String> getMorphlineCommandsMap() {
		
		GlobalJobParameters jobParameters = this.getRuntimeContext().getExecutionConfig().getGlobalJobParameters();

		String morphlineCfg = jobParameters.toMap().get(MORPHLINE_CONF_STRING);
		@SuppressWarnings("unchecked")
		Map<String, String> resultMap = JSONObject.parseObject(morphlineCfg, Map.class);
		if (resultMap.isEmpty()) {
			log.debug("init Morphline config but result is null");
		} 

		return resultMap;
	}

}
