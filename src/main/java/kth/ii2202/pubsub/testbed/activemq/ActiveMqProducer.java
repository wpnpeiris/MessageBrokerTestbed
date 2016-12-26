/**
 * 
 */
package kth.ii2202.pubsub.testbed.activemq;

import javax.jms.Connection;
import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.apache.activemq.ActiveMQConnectionFactory;

import kth.ii2202.pubsub.testbed.Producer;

/**
 * @author pradeeppeiris
 *
 */
public class ActiveMqProducer extends Producer {

	private Connection connection;
	private Session session;
	private javax.jms.MessageProducer producer;

	public ActiveMqProducer(String brokerUrl, String queueName) {
		super(brokerUrl, queueName);
	}
	
	@Override
	protected void createConnection() throws Exception {
		ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory(brokerUrl);
		connection = connectionFactory.createConnection();
		connection.start();
		
		session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
		Destination destination = session.createQueue(queueName);
		producer = session.createProducer(destination);
		producer.setDeliveryMode(DeliveryMode.PERSISTENT);
	}

	@Override
	protected void sendMessage(String message) throws Exception {
		TextMessage textMessage = session.createTextMessage(message);
		producer.send(textMessage);
	}

	@Override
	protected void closeConnection() throws Exception {
		session.close();
		connection.close();
	}

}
