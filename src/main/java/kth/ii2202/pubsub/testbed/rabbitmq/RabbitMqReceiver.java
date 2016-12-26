package kth.ii2202.pubsub.testbed.rabbitmq;

import java.io.IOException;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;

public class RabbitMqReceiver extends kth.ii2202.pubsub.testbed.Consumer {
	private ConnectionFactory factory;
	private Connection connection;
	private Channel channel;
	
	public RabbitMqReceiver(String brokerUrl, String queueName) {
		super(brokerUrl, queueName);
	}

	@Override
	protected void createConnection() throws Exception {
		factory = new ConnectionFactory();
		factory.setHost(brokerUrl);
		connection = factory.newConnection();
		channel = connection.createChannel();
	}
	
	@Override
	public void listenForMessages() throws Exception {
		channel.queueDeclare(queueName, false, false, false, null);
		channel.basicConsume(queueName, true, new Consumer(channel, this));
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
