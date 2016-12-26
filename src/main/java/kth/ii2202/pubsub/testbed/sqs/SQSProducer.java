package kth.ii2202.pubsub.testbed.sqs;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.SendMessageRequest;

import kth.ii2202.pubsub.testbed.Producer;

/**
 * @author pradeeppeiris
 *	
 */
public class SQSProducer extends Producer {

	private static final String ENDPOINT = "https://sqs.eu-west-1.amazonaws.com";
	
	AmazonSQSClient sqs;
	
	public SQSProducer(String brokerUrl, String queueName) {
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
	protected void sendMessage(String message) throws Exception {
		SendMessageRequest sendMessageRequest = new SendMessageRequest(brokerUrl + queueName, message);
		sendMessageRequest.setMessageGroupId("messageGroup1");
		sqs.sendMessage(sendMessageRequest);
		
	}

	@Override
	protected void closeConnection() throws Exception {
		
	}
}
