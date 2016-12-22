package kth.ii2202.pubsub.testbed.sqs;

import com.amazonaws.AmazonClientException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.SendMessageRequest;
import com.amazonaws.services.sqs.model.SendMessageResult;

import kth.ii2202.pubsub.testbed.Producer;

/**
 * @author pradeeppeiris
 *	
 */
public class SQSProducer extends Producer {

	private static final String ENDPOINT = "https://sqs.eu-west-1.amazonaws.com";
	
	public SQSProducer(String brokerUrl, double messageSizeInKB) {
		super(brokerUrl, messageSizeInKB);
	}

	@Override
	public void send(String message) {
		AmazonSQSClient client = SQSConnectionFactory.getClient(ENDPOINT);
		
		message = appendTimestamp(message);
		SendMessageRequest sendMessageRequest = new SendMessageRequest(brokerUrl + QUEUE_NAME, message);
		sendMessageRequest.setMessageGroupId("messageGroup1");
		client.sendMessage(sendMessageRequest);
        
	}

}
