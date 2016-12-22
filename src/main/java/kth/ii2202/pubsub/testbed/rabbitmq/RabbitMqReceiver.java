package kth.ii2202.pubsub.testbed.rabbitmq;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Consumer;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;

import kth.ii2202.pubsub.testbed.activemq.ActiveMqConsumer;

public class RabbitMqReceiver extends kth.ii2202.pubsub.testbed.Consumer {
	private static final Logger logger = LogManager.getLogger(ActiveMqConsumer.class);
	
	private ConnectionFactory factory;
	private Connection connection;
	private Channel channel;
	
	public RabbitMqReceiver(String brokerUrl, int expectedMessages) {
		super(brokerUrl, expectedMessages);
	}

	@Override
	public void init() throws Exception {
		logger.debug("Initialize RabbitMqReceiver");
		
		factory = new ConnectionFactory();
		factory.setHost(brokerUrl);
		connection = factory.newConnection();
		channel = connection.createChannel();
		
		channel.queueDeclare(QUEUE_NAME, false, false, false, null);
		channel.basicConsume(QUEUE_NAME, true, new Consumer(channel, this));
	}
	
	private class Consumer extends DefaultConsumer {
		RabbitMqReceiver receiver;
		public Consumer(Channel channel, RabbitMqReceiver receiver) {
			super(channel);
			this.receiver = receiver;
		}
		
		@Override
		public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties,
				byte[] body) throws IOException {
			String message = new String(body, "UTF-8");
			receiver.logMessage(message);
		}
		
	}

}
