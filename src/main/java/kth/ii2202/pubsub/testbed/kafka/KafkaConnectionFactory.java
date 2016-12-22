package kth.ii2202.pubsub.testbed.kafka;

import java.util.Properties;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;

/**
 * @author pradeeppeiris
 */
public class KafkaConnectionFactory {

	private static KafkaProducer<String, String> producer;
	private static KafkaConsumer<String, String> consumer;

	public static KafkaProducer<String, String> getProducer(String url) {
		if (producer == null) {
			producer = setupProducer(url);
		}

		return producer;
	}
	
	public static KafkaConsumer<String, String> getConsumer(String urls) {
		if (consumer == null) {
			consumer = setupConsumer(urls);
			
		}

		return consumer;
	}

	private static KafkaConsumer<String, String> setupConsumer(String urls) {
		Properties props = new Properties();
		props.put("bootstrap.servers", urls);
		props.put("group.id", "test");
		props.put("enable.auto.commit", "true");
		props.put("auto.commit.interval.ms", "1000");
		props.put("session.timeout.ms", "30000");
		props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		return new KafkaConsumer<>(props);
	}

	private static KafkaProducer<String, String> setupProducer(String urls) {
		Properties props = new Properties();
		props.put("bootstrap.servers", urls);
		props.put("acks", "all");
		props.put("retries", 0);
		props.put("batch.size", 16384);
		props.put("linger.ms", 1);
		props.put("buffer.memory", 33554432);
		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

		return new KafkaProducer<>(props);
	}
}
