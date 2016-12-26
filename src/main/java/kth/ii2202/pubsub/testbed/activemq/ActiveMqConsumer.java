/**
 * 
 */
package kth.ii2202.pubsub.testbed.activemq;

import javax.jms.Connection;
import javax.jms.Destination;
import javax.jms.ExceptionListener;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import kth.ii2202.pubsub.testbed.Consumer;

/**
 * @author pradeeppeiris
 *
 */
public class ActiveMqConsumer extends Consumer implements ExceptionListener {
	private static final Logger logger = LogManager.getLogger(ActiveMqConsumer.class);
	
	private Connection connection;
	private Session session;
	private javax.jms.MessageConsumer consumer;

	public ActiveMqConsumer(String brokerUrl, String queueName) {
		super(brokerUrl, queueName);
	}
	
	@Override
	protected void createConnection() throws Exception {
		ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory(brokerUrl);
		connection = connectionFactory.createConnection();
		connection.start();
		connection.setExceptionListener(this);
		
		session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
		Destination destination = session.createQueue(queueName);
		consumer = session.createConsumer(destination);
	}
	
	@Override
	public void listenForMessages() throws Exception {
		while(true) {
			Message messageObj = consumer.receive();
			if (messageObj != null && messageObj instanceof TextMessage) {
				TextMessage textMessage = (TextMessage) messageObj;
				String message = textMessage.getText();
				logMessage(message);
			}
		}
	}

	@Override
	public void onException(JMSException ex) {
		logger.error(ex);
	}
}
