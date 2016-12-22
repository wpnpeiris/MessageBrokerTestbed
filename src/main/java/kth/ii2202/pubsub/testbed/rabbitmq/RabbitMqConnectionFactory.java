/**
 * 
 */
package kth.ii2202.pubsub.testbed.rabbitmq;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

/**
 * @author pradeeppeiris
 *
 */
public class RabbitMqConnectionFactory {

	private static Channel channel;
	
	public static Channel getChannel(String url) throws Exception {
		if(channel == null) {
			ConnectionFactory factory = new ConnectionFactory();
			factory.setHost(url);
			Connection connection = factory.newConnection();
			channel = connection.createChannel();
		}
		
		return channel;
	}
}
