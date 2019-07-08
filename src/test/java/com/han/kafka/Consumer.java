package com.han.kafka;

import java.time.Duration;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.Test;

public class Consumer {

	@Test
	public void testSpilt() {
		String str = "d38e9bed5d98110dc2489d0d1cac3c2a	7d45833d9865727a88b960b0603c19f6	2015-02-23 17:41:29	0	0	0	丽江旅游(sz002033)#股票##炒股##财经##理财##投资#推荐包赢股，盈利对半分成，不算本金，群：46251412";

		// String data[] = str.split("\\s{2,}|\t");

		String data[] = StringUtils.split(str, "\\s{2,}|\t");
		for (String s : data) {
			System.out.println(s);
		}
	}

	@SuppressWarnings("static-access")
	@Test
	public void testConsumeWeiBoData() throws Exception {

		@SuppressWarnings("resource")
		KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<String, String>(getProperties());
		Set<String> set = new HashSet<String>();
		set.add("test-leader1");
		kafkaConsumer.subscribe(set);

		while (true) {
			ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofSeconds(1));
			if (!records.isEmpty()) {
				System.out.println("当前批次拉取的记录数为:" + records.count());
				for (ConsumerRecord<String, String> record : records) {
					System.out.println(record.value());
				}

				Thread.currentThread().sleep(1000L);
			}
		}

	}

	private Properties getProperties() {
		Properties p = new Properties();
		p.put("bootstrap.servers", "192.168.12.100:9092,192.168.12.101:9092,192.168.12.102:9092");

		p.put("value.deserializer", StringDeserializer.class);
		p.put("key.deserializer", StringDeserializer.class);
		p.put("group.id", "test-leader");
		p.put("enable.auto.commit", "true");
		p.put("auto.commit.interval.ms", "3000");
		p.put("session.timeout.ms", "30000");
		p.put("auto.offset.reset", "earliest");
		p.put("max.poll.records", "1");

		return p;
	}

}
