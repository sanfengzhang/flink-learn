package com.han.kafka;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.AdminClient;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class KafkaApiTest {

	private Properties clientProps = null;

	AdminClient client = null;

	@Before
	public void setup() {
		clientProps = new Properties();
		clientProps.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG,
				"baspv76.360es.cn:9092,baspv77.360es.cn:9092,baspv78.360es.cn:9092");
		clientProps.put("client.id", "client0");

		client = AdminClient.create(clientProps);

	}

	@After
	public void after() {
		client.close();
	}

	//listConsumerGroupOffsets这个方法会请求这个消费组所有的topic消费情况
	@Test
	public void testAllTopicPartitionOffsets() throws InterruptedException, ExecutionException {
       long start=System.currentTimeMillis();
		client.listConsumerGroups().all().get().forEach(action -> {
			try {
				client.listConsumerGroupOffsets(action.groupId()).partitionsToOffsetAndMetadata().get()
						.forEach((k, v) -> {
						//	System.out.println(action.groupId() + " " + k + " " + v);

						});
			} catch (InterruptedException e) {

				e.printStackTrace();
			} catch (ExecutionException e) {

				e.printStackTrace();
			}

		});
		
		long end=System.currentTimeMillis();
		
		System.out.println(end-start);

	}

}
