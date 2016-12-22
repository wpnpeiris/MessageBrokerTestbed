/**
 * 
 */
package kth.ii2202.pubsub.testbed.activemq;

import javax.jms.Connection;
import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.apache.activemq.ActiveMQConnectionFactory;

import kth.ii2202.pubsub.testbed.Endpoint;
import kth.ii2202.pubsub.testbed.Producer;

/**
 * @author pradeeppeiris
 *
 */
public class ActiveMqProducer extends Producer {
	
//	private Session session;
//	private MessageProducer producer;
	
	public ActiveMqProducer(String brokerUrl, double messageSizeInKB) throws Exception {
		super(brokerUrl, messageSizeInKB);
		
//		Connection connection = createConnection();
//		this.session = createSession(connection);
//		this.producer = createProducer();
	}
	
	@Override
	public void send(String message) {
		try {
			ActiveMqConnectionFactory.ActiveMqConnection activeMqConnection = ActiveMqConnectionFactory.getProducer(brokerUrl, QUEUE_NAME);
			message = appendTimestamp(message);
			TextMessage textMessage = activeMqConnection.getSession().createTextMessage(message);
			
			logMessage(message);
			activeMqConnection.getProducer().send(textMessage);
			
//			session.close();
//			connection.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
		
	}

//	private MessageProducer createProducer() throws Exception {
//		Destination destination = session.createQueue(QUEUE_NAME);
//		MessageProducer producer = session.createProducer(destination);
//		producer.setDeliveryMode(DeliveryMode.PERSISTENT);
//		
//		return producer;
//	}

//	private Session createSession(Connection connection) throws JMSException {
//		Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
//		return session;
//	}
//	
//	private Connection createConnection() throws Exception {
//		ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory(brokerUrl);
//		Connection connection = connectionFactory.createConnection();
//		connection.start();
//		
//		return connection;
//	}

}
