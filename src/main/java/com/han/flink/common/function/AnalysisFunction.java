package com.han.flink.common.function;

import java.util.Collection;
import java.util.Map;

import com.han.flink.common.CommonMessage;

/**
 * 定义一个数据的解析通用接口
 * 
 * @author hanlin01
 *
 */
public interface AnalysisFunction<IN extends CommonMessage, OUT> {

	/**
	 * 负责解析record记录
	 * @param record
	 * @return
	 */
	public Map<String, Collection<Object>> proccess(CommonMessage record);

	/**
	 * 可以自己根据结果类型进行转换
	 * @param value
	 * @return
	 */
	public OUT transfer(Map<String, Collection<Object>> value);

	

}
