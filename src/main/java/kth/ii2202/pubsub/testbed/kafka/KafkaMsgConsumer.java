package kth.ii2202.pubsub.testbed.kafka;

import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import kth.ii2202.pubsub.testbed.Consumer;
import kth.ii2202.pubsub.testbed.Endpoint;

public class KafkaMsgConsumer extends Consumer implements Endpoint {
	KafkaConsumer<String, String> consumer;

	public KafkaMsgConsumer(String brokerUrl, String queueName) {
		super(brokerUrl, queueName);
	}

	@Override
	protected void createConnection() throws Exception {
		Properties props = new Properties();
		props.put("bootstrap.servers", brokerUrl);
		props.put("group.id", "test");
		props.put("enable.auto.commit", "true");
		props.put("auto.commit.interval.ms", "1000");
		props.put("session.timeout.ms", "30000");
		props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		consumer = new KafkaConsumer<>(props);
		consumer.subscribe(Arrays.asList(queueName));
		
	}
	
	@Override
	public void listenForMessages() throws Exception {
		while (true) {
			ConsumerRecords<String, String> records = consumer.poll(10);

			for (ConsumerRecord<String, String> record : records) {
				switch (record.topic()) {
				case QUEUE_NAME:
					logMessage(record.value());
				}
			}
		}
	}
	
}
