package kth.ii2202.pubsub.testbed.rabbitmq;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import kth.ii2202.pubsub.testbed.Producer;

public class RabbitMqProducer extends Producer {
	
	private Connection connection;
	private Channel channel;
	
	public RabbitMqProducer(String brokerUrl, String queueName) {
		super(brokerUrl, queueName);
	}
	
	@Override
	protected void createConnection() throws Exception {
		ConnectionFactory factory = new ConnectionFactory();
		factory.setHost(brokerUrl);
	    connection = factory.newConnection();
		channel = connection.createChannel();
		channel.queueDeclare(queueName, false, false, false, null);
	}
	
	@Override
	protected void sendMessage(String message) throws Exception {
		channel.basicPublish("", queueName, null, message.getBytes());
	}

	@Override
	protected void closeConnection() throws Exception {
		channel.close();
		connection.close();
	}

}
