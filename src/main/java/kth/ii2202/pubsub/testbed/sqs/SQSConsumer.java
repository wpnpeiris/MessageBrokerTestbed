package kth.ii2202.pubsub.testbed.sqs;

import java.util.List;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.DeleteMessageRequest;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;

import kth.ii2202.pubsub.testbed.Consumer;

public class SQSConsumer extends Consumer {
	private static final String ENDPOINT = "https://sqs.eu-west-1.amazonaws.com";
	
	private AmazonSQSClient sqs;
			
	public SQSConsumer(String brokerUrl, String queueName) {
		super(brokerUrl, queueName);
	}
	
	@Override
	protected void createConnection() throws Exception {
		AWSCredentials credentials = null;
        try {
            credentials = new ProfileCredentialsProvider().getCredentials();
        } catch (Exception e) {
           e.printStackTrace();
        }
 
        sqs = new AmazonSQSClient(credentials);
        sqs.setEndpoint(ENDPOINT);
		
	}

	@Override
	public void listenForMessages() throws Exception {
		while(true) {
			ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest(brokerUrl + queueName);
			List<Message> messages = sqs.receiveMessage(receiveMessageRequest).getMessages();
			if(messages != null && messages.size() > 0) {
				for(Message message : messages) {
					logMessage(message.getBody());
				}
				
				for(Message message : messages) {
					String messageReceiptHandle = message.getReceiptHandle();
		            sqs.deleteMessage(new DeleteMessageRequest(brokerUrl + queueName, messageReceiptHandle));
				}
			}
		}
	}
}
