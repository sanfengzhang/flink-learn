package com.han.kafka;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.List;
import java.util.Properties;

import org.apache.commons.io.IOUtils;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.Test;

public class Produce {

	private List<String> getWeiboData() throws IOException {

		FileInputStream fis = new FileInputStream(new File("D:\\dataset\\Weibo_Data\\weibo_train_data.txt"));
		List<String> _list = IOUtils.readLines(fis, "UTF-8");

		fis.close();
		return _list;

	}

	@Test
	public void produceWeiBoData() throws FileNotFoundException, IOException {

		Properties props = getProperties();

		List<String> _list = getWeiboData();

		KafkaProducer<String, String> producer = new KafkaProducer<String, String>(props);
		int count = 0;
		for (String weibo : _list) {
			if (count == 10) {
				break;
			}
			ProducerRecord<String, String> record = new ProducerRecord<String, String>("weibo_topic", weibo);
			producer.send(record, new Callback() {

				public void onCompletion(RecordMetadata recordMetadata, Exception arg1) {
					if (arg1 != null) {
						System.out.println(arg1);
					}

				}
			});

			count++;
		}

		producer.close();

	}

	private List<String> getWeiboData10() throws IOException {

		FileInputStream fis = new FileInputStream(new File("C:\\Users\\hanlin01\\Desktop\\leader.txt"));
		List<String> _list = IOUtils.readLines(fis, "UTF-8");

		fis.close();
		return _list;

	}

	@Test
	public void produceWeiBoData10() throws FileNotFoundException, IOException {

		Properties props = getProperties();

		List<String> _list = getWeiboData10().subList(10, 50);

		KafkaProducer<String, String> producer = new KafkaProducer<String, String>(props);
		for (String weibo : _list) {

			ProducerRecord<String, String> record = new ProducerRecord<String, String>("test-leader", weibo);
			producer.send(record, new Callback() {

				public void onCompletion(RecordMetadata recordMetadata, Exception arg1) {
					if (arg1 != null) {
						System.out.println(arg1);
					}

				}
			});

		}

		producer.close();

	}

	private Properties getProperties() {
		Properties props = new Properties();
		props.put("bootstrap.servers", "192.168.12.100:9092,192.168.12.101:9092,192.168.12.102:9092");
		props.put("acks", "all");
		props.put("retries", 0);
		props.put("batch.size", 10000);
		props.put("linger.ms", 10);
		props.put("buffer.memory", 33554432);
		props.put("key.serializer", StringSerializer.class);
		props.put("value.serializer", StringSerializer.class);

		return props;
	}

}
