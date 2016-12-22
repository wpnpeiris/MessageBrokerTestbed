package kth.ii2202.pubsub.testbed.sqs;

import java.util.List;
import java.util.Map.Entry;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.amazonaws.AmazonClientException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.DeleteMessageRequest;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;

import kth.ii2202.pubsub.testbed.Consumer;
import kth.ii2202.pubsub.testbed.activemq.ActiveMqConsumer;

public class SQSConsumer extends Consumer {
	private static final String ENDPOINT = "https://sqs.eu-west-1.amazonaws.com";
	
	private AmazonSQSClient sqs;
			
	public SQSConsumer(String brokerUrl, int expectedMessages) {
		super(brokerUrl, expectedMessages);
	}
	

	@Override
	public void init() throws Exception {
		AWSCredentials credentials = null;
        try {
            credentials = new ProfileCredentialsProvider().getCredentials();
        } catch (Exception e) {
           e.printStackTrace();
        }
 
        sqs = new AmazonSQSClient(credentials);
        sqs.setEndpoint(ENDPOINT);
        
        process();
	}
	
	private void process() throws Exception {
		while(true) {
			ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest(brokerUrl + QUEUE_NAME);
			List<Message> messages = sqs.receiveMessage(receiveMessageRequest).getMessages();
			if(messages != null && messages.size() > 0) {
				for(Message message : messages) {
					logMessage(message.getBody());
				}
			}
		}
	}
}
