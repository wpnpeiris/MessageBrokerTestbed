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
import kth.ii2202.pubsub.testbed.Main;

/**
 * @author pradeeppeiris
 *
 */
public class ActiveMqConsumer extends Consumer implements ExceptionListener {
	private static final Logger logger = LogManager.getLogger(ActiveMqConsumer.class);
	
	private Connection connection;
	private Session session;
	private MessageConsumer consumer;

	public ActiveMqConsumer(String brokerUrl, int expectedMessages) {
		super(brokerUrl, expectedMessages);
	}
	
	@Override
	public void init() throws Exception {
		logger.debug("Initialize ActiveMqConsumer");
		connection = createConnection();
		session = createSession(connection);
		consumer = createConsumer(session);
		
		process();
	}

	@Override
	public void onException(JMSException ex) {

	}

	private void process() throws Exception {
		while(true) {
			Message messageObj = consumer.receive();
			if (messageObj != null && messageObj instanceof TextMessage) {
				TextMessage textMessage = (TextMessage) messageObj;
				String message = textMessage.getText();
				logMessage(message);
			}
		}
		
//		consumer.close();
//		session.close();
//		connection.close();
	}

	private MessageConsumer createConsumer(Session session) throws JMSException {
		Destination destination = session.createQueue(QUEUE_NAME);
		MessageConsumer consumer = session.createConsumer(destination);
		return consumer;
	}

	private Session createSession(Connection connection) throws JMSException {
		Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
		return session;
	}

	private Connection createConnection() throws JMSException {
		ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory(brokerUrl);
		Connection connection = connectionFactory.createConnection();
		connection.start();
		connection.setExceptionListener(this);

		return connection;
	}

}
