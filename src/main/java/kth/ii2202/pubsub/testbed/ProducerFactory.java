/**
 * 
 */
package kth.ii2202.pubsub.testbed;

import kth.ii2202.pubsub.testbed.activemq.ActiveMqProducer;
import kth.ii2202.pubsub.testbed.kafka.KafkaMsgProducer;
import kth.ii2202.pubsub.testbed.rabbitmq.RabbitMqProducer;
import kth.ii2202.pubsub.testbed.sqs.SQSProducer;

/**
 * @author pradeeppeiris
 *
 */
public class ProducerFactory {
	private static final String PROP_BROKER = "main.type";
	private static final String BROKER_ACTIVEMQ = "activemq";
	private static final String BROKER_RABBITMQ = "rabbitmq";
	private static final String BROKER_KAFKA = "kafka";
	private static final String BROKER_SQS = "sqs";
	
	private static final String PROP_QUEUE_NAME = "main.queue.name";
	
	private static final String PROP_ACTIVEMQ_URL = "broker.activemq.url";
	private static final String PROP_RABBITMQ_URL = "broker.rabbitmq.url";
	private static final String PROP_KAFKA_URL = "broker.kafka.url";
	private static final String PROP_SQS_URL = "broker.sqs.url";
	
	public static Producer getMessageProducer() throws Exception {
		Context context = Context.getInstance();
		String brokerType = context.getProperty(PROP_BROKER);
		String queueName = context.getProperty(PROP_QUEUE_NAME);
		
		Producer producer;
		if(BROKER_ACTIVEMQ.equals(brokerType)) {
			producer = new ActiveMqProducer(context.getProperty(PROP_ACTIVEMQ_URL), queueName);
		} else if(BROKER_RABBITMQ.equals(brokerType)) {
			producer = new RabbitMqProducer(context.getProperty(PROP_RABBITMQ_URL), queueName);
		} else if(BROKER_KAFKA.equals(brokerType)) {
			producer = new KafkaMsgProducer(context.getProperty(PROP_KAFKA_URL), queueName);
		} else if(BROKER_SQS.equals(brokerType)) {
			producer = new SQSProducer(context.getProperty(PROP_SQS_URL), queueName);
		} else {
			throw new Exception("Invalid broker type specified");
		}
		
		return producer;
	}
}
