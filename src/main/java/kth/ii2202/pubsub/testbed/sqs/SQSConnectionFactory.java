/**
 * 
 */
package kth.ii2202.pubsub.testbed.sqs;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.sqs.AmazonSQSClient;

/**
 * @author pradeeppeiris
 *
 */
public class SQSConnectionFactory {
	private static AmazonSQSClient client;
	
	public static AmazonSQSClient getClient(String endpoint) {
		if(client == null) {
			client = setupClient(endpoint);
		}
		
		return client;
	}
	
	private static AmazonSQSClient setupClient(String endpoint) {
		AWSCredentials credentials = null;
        try {
            credentials = new ProfileCredentialsProvider().getCredentials();
        } catch (Exception e) {
           e.printStackTrace();
        }
 
        AmazonSQSClient sqs = new AmazonSQSClient(credentials);
        sqs.setEndpoint(endpoint);
        
        return sqs;
	}
	
}
