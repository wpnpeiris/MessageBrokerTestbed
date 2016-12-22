package kth.ii2202.pubsub.testbed.kafka;

import java.io.InputStream;
import java.util.Arrays;
import java.util.Properties;
import java.util.Random;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import com.google.common.io.Resources;

import kth.ii2202.pubsub.testbed.Consumer;

public class KafkaMsgConsumer extends Consumer {
	KafkaConsumer<String, String> consumer;

	public KafkaMsgConsumer(String brokerUrl, int expectedMessages) {
		super(brokerUrl, expectedMessages);
	}

	@Override
	public void init() throws Exception {
		try (InputStream props = Resources.getResource("kafka.consumer.props").openStream()) {
			Properties properties = new Properties();
			properties.load(props);
			if (properties.getProperty("group.id") == null) {
				properties.setProperty("group.id", "group-" + new Random().nextInt(100000));
			}
			consumer = new KafkaConsumer<>(properties);
		}
		consumer.subscribe(Arrays.asList(QUEUE_NAME));
		process();
	}

	private void process() throws Exception {
		while (true) {
			ConsumerRecords<String, String> records = consumer.poll(10);

			for (ConsumerRecord<String, String> record : records) {
				switch (record.topic()) {
				case QUEUE_NAME:
//					System.out.println(">>>> " + record.value());
					logMessage(record.value());
				}
			}
		}
	}
}
