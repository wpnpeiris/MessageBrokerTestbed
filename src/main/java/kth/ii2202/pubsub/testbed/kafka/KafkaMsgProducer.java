package kth.ii2202.pubsub.testbed.kafka;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import kth.ii2202.pubsub.testbed.Producer;

public class KafkaMsgProducer extends Producer {

	private static KafkaProducer<String, String> producer;
	
	public KafkaMsgProducer(String brokerUrl, String queueName) throws Exception {
		super(brokerUrl, queueName);
	}

	@Override
	protected void createConnection() throws Exception {
		Properties props = new Properties();
		props.put("bootstrap.servers", brokerUrl);
		props.put("acks", "all");
		props.put("retries", 0);
		props.put("batch.size", 16384);
		props.put("linger.ms", 1);
		props.put("buffer.memory", 33554432);
		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("block.on.buffer.full", "false");

		producer = new KafkaProducer<>(props);
		
	}

	@Override
	protected void sendMessage(String message) throws Exception {
		producer.send(new ProducerRecord<String, String>(queueName, message));
	}

	@Override
	protected void closeConnection() throws Exception {
		producer.close();
	}
}
