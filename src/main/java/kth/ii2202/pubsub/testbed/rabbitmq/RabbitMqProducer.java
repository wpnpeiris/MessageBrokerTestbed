package kth.ii2202.pubsub.testbed.rabbitmq;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import kth.ii2202.pubsub.testbed.Producer;

public class RabbitMqProducer extends Producer {

	public RabbitMqProducer(String brokerUrl, double messageSizeInKB) {
		super(brokerUrl, messageSizeInKB);
	}
	
	@Override
	public void send(String message) {
		try {
			Channel channel = RabbitMqConnectionFactory.getChannel(brokerUrl);
			
			message = appendTimestamp(message);
			channel.queueDeclare(QUEUE_NAME, false, false, false, null);
			channel.basicPublish("", QUEUE_NAME, null, message.getBytes());
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

}
