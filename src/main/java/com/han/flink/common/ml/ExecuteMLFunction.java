package com.han.flink.common.ml;

public interface ExecuteMLFunction<IN, OUT>
{

	public OUT result(IN params);

}
