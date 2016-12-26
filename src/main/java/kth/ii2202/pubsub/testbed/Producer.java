/**
 * 
 */
package kth.ii2202.pubsub.testbed;

import java.util.Arrays;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * MessageProducer follows the Template Method design pattern
 * to provide the skeleton of message generating logic 
 * for any Message Producer client in Message broker systems.
 * 
 * @author pradeeppeiris
 *
 */
public abstract class Producer {
	protected final String brokerUrl;
	protected final String queueName;
	
	private void sendMessages(int batchSize, double messageSizeInKB) {
		ExecutorService executor = Executors.newCachedThreadPool();
		for (int i = 0; i < batchSize; i++){
			executor.execute(new MessageSender(messageSizeInKB));
		}
		waitForAllExecutersToComplete(executor);
	}

	private void waitForAllExecutersToComplete(ExecutorService executor) {
		executor.shutdown();
		while (!executor.isTerminated()) {	 
		}
	}
	
	public Producer(String brokerUrl, String queueName) {
		this.brokerUrl = brokerUrl;
		this.queueName = queueName;
	}
	
	public void generateMessages(int batchSize, double messageSizeInKB) throws Exception {
		createConnection();
		sendMessages(batchSize, messageSizeInKB);
		closeConnection();
	}
	
	protected abstract void createConnection() throws Exception;
	protected abstract void sendMessage(String message) throws Exception;
	protected abstract void closeConnection() throws Exception;
	
	private class MessageSender implements Runnable {	
		double messageSizeInKB;
		
		MessageSender(double messageSizeInKB) {
			this.messageSizeInKB = messageSizeInKB;
		}
		
		@Override
		public void run() {
			String message = createMessage(messageSizeInKB);
			try {
				sendMessage(message);
			} catch (Exception e) {
				e.printStackTrace();
			}
			
		}
		
		private String createMessage(double messageSizeInKB) {
			StringBuilder message = new StringBuilder();
			message.append(UUID.randomUUID().toString());
			message.append(",").append(System.currentTimeMillis()).append(",");
			message.append(resizeMessage(messageSizeInKB));
			
			return message.toString();
		}
		
		private String resizeMessage(double messageSizeInKB) {
			Double dMsgSize = (messageSizeInKB / 2) * 1024;
			int msgSize = dMsgSize.intValue();
			
			char[] chars = new char[msgSize];
			Arrays.fill(chars, 'a');
			
			return new String(chars);
		}
		
	}
}
