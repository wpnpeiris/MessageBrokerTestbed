/**
 * 
 */
package kth.ii2202.pubsub.testbed;

import java.util.Arrays;
import java.util.UUID;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * @author pradeeppeiris
 * 
 * java -cp "target/testbed-1.0.jar:config" kth.ii2202.pubsub.testbed.Main producer
 */
public abstract class Producer implements Runnable, Endpoint {
	
	private static final Logger logger = LogManager.getLogger(Producer.class);
	
	protected String brokerUrl;
	protected Double messageSizeInKB;
	
	public Producer(String brokerUrl, Double messageSizeInKB) {
		this.brokerUrl = brokerUrl;
		this.messageSizeInKB = messageSizeInKB;
	}
	
	@Override
	public void run() {
		String message = createMessage();
		send(message);
	}

	public abstract void send(String message);
	
	private String createMessage() {
		StringBuilder message = new StringBuilder();
		message.append(UUID.randomUUID().toString());
		
		return message.toString();
	}
	
	protected String appendTimestamp(String message) {
		StringBuilder sb = new StringBuilder();
		sb.append(message).append(",").append(System.currentTimeMillis()).append(",");
		sb.append(resizeMessage());
		
		return sb.toString();
	}
	
	protected void logMessage(String message) {
//		logger.info("Message {} sent", message);
	}
	
	private String resizeMessage() {
		Double dMsgSize = (messageSizeInKB / 2) * 1024;
		int msgSize = dMsgSize.intValue();
		
		char[] chars = new char[msgSize];
		Arrays.fill(chars, 'a');
		
		return new String(chars);
	}
}
