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

import org.apache.activemq.ActiveMQConnectionFactory;

/**
 * @author pradeeppeiris
 *
 */
public class ActiveMqConnectionFactory {
	private static ActiveMqConnection producerConnection;
	
	public static ActiveMqConnection getProducer(String url, String queue) throws Exception {
		if(producerConnection == null) {
			Connection connection = createConnection(url);
			Session session = createSession(connection);
			MessageProducer producer = createProducer(queue, session);
			
			producerConnection = new ActiveMqConnection();
			producerConnection.setSession(session);
			producerConnection.setProducer(producer);
		}
		return producerConnection;
	}
	
	private static Session createSession(Connection connection) throws JMSException {
		Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
		return session;
	}
	
	private static MessageProducer createProducer(String queue, Session session) throws Exception {
		Destination destination = session.createQueue(queue);
		MessageProducer producer = session.createProducer(destination);
		producer.setDeliveryMode(DeliveryMode.PERSISTENT);
		
		return producer;
	}

	private static Connection createConnection(String url) throws Exception {
		ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory(url);
		Connection connection = connectionFactory.createConnection();
		connection.start();
		
		return connection;
	}
	
	static class ActiveMqConnection {
		Session session;
		MessageProducer producer;
		public Session getSession() {
			return session;
		}
		public void setSession(Session session) {
			this.session = session;
		}
		public MessageProducer getProducer() {
			return producer;
		}
		public void setProducer(MessageProducer producer) {
			this.producer = producer;
		}
	}
}
