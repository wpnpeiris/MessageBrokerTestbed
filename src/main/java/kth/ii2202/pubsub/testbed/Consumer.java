/**
 * 
 */
package kth.ii2202.pubsub.testbed;

import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * @author pradeeppeiris
 * java -cp "target/testbed-1.0.jar:config" kth.ii2202.pubsub.testbed.Main consumer
 */
public abstract class Consumer {
	private static final Logger logger = LogManager.getLogger(Consumer.class);

	protected String brokerUrl;
	protected final String queueName;

	public Consumer(String brokerUrl, String queueName) {
		this.brokerUrl = brokerUrl;
		this.queueName = queueName;
		logger.info("TIME MESSAGEID LATENCY");
	}

	public void receiveMessages() throws Exception {
		createConnection();
		listenForMessages();
	}
	
	public abstract void listenForMessages() throws Exception;
	protected abstract void createConnection() throws Exception;
	
	protected void logMessage(String message) {
		new Thread(new ConsumerLog(message, System.currentTimeMillis())).start();
	}
	
	private class ConsumerLog implements Runnable {
		String message;
		long receivedTime;
		
		ConsumerLog(String message, long receivedTime) {
			this.message = message;
			this.receivedTime = receivedTime;
		}
		
		@Override
		public void run() {
			processMessage();
		}
		
		private void processMessage() {
			String[] data = message.split(",");
			
			String messageId = data[0];
			long sentTime = Long.valueOf(data[1]);
			long elappsedTime = (receivedTime - sentTime);
			logger.info("{} {} {}", new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS").format(new Date(receivedTime)), messageId, elappsedTime);	
		}
	}
}
