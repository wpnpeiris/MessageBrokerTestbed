package kth.ii2202.pubsub.testbed.kafka;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import com.google.common.io.Resources;

import kth.ii2202.pubsub.testbed.Producer;

public class KafkaMsgProducer extends Producer {

	public KafkaMsgProducer(String brokerUrl, double messageSizeInKB) throws Exception {
		super(brokerUrl, messageSizeInKB);
	}

	@Override
	public void send(String message) {
		try {
			message = appendTimestamp(message);
			KafkaConnectionFactory.getProducer(brokerUrl).send(new ProducerRecord<String, String>(QUEUE_NAME, message));
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

	private KafkaProducer<String, String> setupProducer() throws IOException {
		//
		// try (InputStream props =
		// Resources.getResource("kafka.producer.props").openStream()) {
		// Properties properties = new Properties();
		// properties.load(props);
		// producer = new KafkaProducer<>(properties);
		// }

		Properties props = new Properties();
		props.put("bootstrap.servers", brokerUrl);
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
