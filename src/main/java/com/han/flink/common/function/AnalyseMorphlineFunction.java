package com.han.flink.common.function;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.kitesdk.morphline.api.Command;
import org.kitesdk.morphline.api.MorphlineContext;
import org.kitesdk.morphline.api.Record;
import org.kitesdk.morphline.base.Compiler;
import org.kitesdk.morphline.base.FaultTolerance;
import org.kitesdk.morphline.base.Fields;
import org.kitesdk.morphline.base.Notifications;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.SharedMetricRegistries;
import com.google.common.base.Preconditions;
import com.han.flink.common.CommonMessage;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

/**
 * 定义morphline的解析抽象接口
 * 
 * @author hanlin01
 *
 */
public abstract class AnalyseMorphlineFunction<OUT> extends RichMapFunction<CommonMessage, OUT>
		implements AnalysisFunction<CommonMessage, OUT> {

	private static final long serialVersionUID = 1L;

	private Map<String, Command> cmdMap = null;

	private MorphlineContext morphlineContext = null;

	// 这个是线程安全的
	private Collector finalChild = null;

	protected String dataType = null;

	public static final String MORHLINE_SUCCEEDED_METRICS_COUNTER = "morplineSuccess";

	public static final String MORHLINE_FAILE_METRICS_COUNTER = "morplineFaile";

	public static final String MORPHLINE_CONF_STRING = "$morphline_cfg";

	private transient Counter failedProcessRecordsNum;

	private transient Counter successProcessRecordsNum;

	protected static final Logger log = LoggerFactory.getLogger(AnalyseMorphlineFunction.class);

	@Override
	public void open(Configuration parameters) throws Exception {

		this.failedProcessRecordsNum = this.getRuntimeContext().getMetricGroup()
				.counter(MORHLINE_FAILE_METRICS_COUNTER);
		this.successProcessRecordsNum = this.getRuntimeContext().getMetricGroup()
				.counter(MORHLINE_SUCCEEDED_METRICS_COUNTER);

		Map<String, String> logtypeToCmd = getMorphlineCommandsMap();

		morphlineContext = new MorphlineContext.Builder().setExceptionHandler(new FaultTolerance(false, false))
				.setMetricRegistry(SharedMetricRegistries.getOrCreate("flink.morphlineContext")).build();

		cmdMap = new ConcurrentHashMap<String, Command>();
		Iterator<Entry<String, String>> it = logtypeToCmd.entrySet().iterator();

		finalChild = new Collector();
		while (it.hasNext()) {
			Entry<String, String> en = it.next();
			String logType = en.getKey();
			String cmdValue = en.getValue();

			Config config = ConfigFactory.parseString(cmdValue);

			Command cmd = new Compiler().compile(config, morphlineContext, finalChild);
			cmdMap.put(logType, cmd);
		}

	}

	protected abstract Map<String, String> getMorphlineCommandsMap();

	@Override
	public OUT map(CommonMessage message) throws Exception {

		Map<String, Collection<Object>> processResult = proccess(message);
		return transfer(processResult);
	}

	@Override
	public Map<String, Collection<Object>> proccess(CommonMessage message) {
		try {

			Command cmd = cmdMap.get(message.getType());
			if (null != cmd) {
				Record record = new Record();
				record.put(Fields.ATTACHMENT_BODY, message.getValue().getBytes("UTF-8"));

				Notifications.notifyStartSession(cmd);
				if (!cmd.process(record)) {

					throw new RuntimeException("faile to analyse record");
				}
				record = finalChild.getRecords().get(0);

				Map<String, Collection<Object>> result = record.getFields().asMap();

				successProcessRecordsNum.inc();
				return result;
			} else {
				throw new NullPointerException("Morphline command must not be null,message type=" + message.getType());
			}

		} catch (Exception e) {
			failedProcessRecordsNum.inc();
			log.warn("Morphline {} failed to process record: {},exception cause={}", message.toString(), e.getCause());
		} finally {
			finalChild.reset();
		}

		return null;

	}

	@Override
	public void close() throws Exception {

		if (null != cmdMap && !cmdMap.isEmpty()) {
			for (Command cmd : cmdMap.values()) {
				Notifications.notifyShutdown(cmd);
			}

			cmdMap = null;

		}

	}

	public static final class Collector implements Command {

		private final List<Record> results = new ArrayList<Record>();

		public List<Record> getRecords() {
			return results;
		}

		public void reset() {
			results.clear();
		}

		public Command getParent() {
			return null;
		}

		public void notify(Record notification) {
		}

		public boolean process(Record record) {
			Preconditions.checkNotNull(record);
			results.add(record);
			return true;
		}

	}
}
