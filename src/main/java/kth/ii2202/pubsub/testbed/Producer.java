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
	
	private void sendMessages(int batchSize, double messageSizeInByte) {
		ExecutorService executor = Executors.newCachedThreadPool();
		for (int i = 0; i < batchSize; i++){
			executor.execute(new MessageSender(messageSizeInByte));
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
	
	public void generateMessages(int batchSize, double messageSizeInByte) throws Exception {
		createConnection();
		sendMessages(batchSize, messageSizeInByte);
		closeConnection();
	}
	
	protected abstract void createConnection() throws Exception;
	protected abstract void sendMessage(String message) throws Exception;
	protected abstract void closeConnection() throws Exception;
	
	private class MessageSender implements Runnable {	
		double messageSizeInByte;
		
		MessageSender(double messageSizeInByte) {
			this.messageSizeInByte = messageSizeInByte;
		}
		
		@Override
		public void run() {
			String message = createMessage();
			try {
				sendMessage(message);
			} catch (Exception e) {
				e.printStackTrace();
			}
			
		}
		
		private String createMessage() {
			StringBuilder message = new StringBuilder();
			message.append(UUID.randomUUID().toString());
			message.append(",").append(System.currentTimeMillis()).append(",");
			message.append(resizeMessage());
			
			return message.toString();
		}
		
		private String resizeMessage() {
			Double dMsgSize = messageSizeInByte / 2;
			int msgSize = dMsgSize.intValue();
			
			char[] chars = new char[msgSize];
			Arrays.fill(chars, 'a');
			
			return new String(chars);
		}
		
	}
}
