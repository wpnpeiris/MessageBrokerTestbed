package kth.ii2202.pubsub.testbed;

import java.util.Properties;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import kth.ii2202.pubsub.testbed.activemq.ActiveMqConsumer;
import kth.ii2202.pubsub.testbed.activemq.ActiveMqProducer;
import kth.ii2202.pubsub.testbed.kafka.KafkaMsgConsumer;
import kth.ii2202.pubsub.testbed.kafka.KafkaMsgProducer;
import kth.ii2202.pubsub.testbed.rabbitmq.RabbitMqProducer;
import kth.ii2202.pubsub.testbed.rabbitmq.RabbitMqReceiver;
import kth.ii2202.pubsub.testbed.sqs.SQSConsumer;
import kth.ii2202.pubsub.testbed.sqs.SQSProducer;

/**
 * @author pradeeppeiris
 * 
 *         ActiveMQ ./activemq start ./activemq stop
 *
 *
 *         RabittMQ /usr/local/Cellar/rabbitmq/3.6.4/sbin/rabbitmq-server
 *         ./rabbitmqctl stop
 *         /etc/init.d/rabbitmq-server restart
 * 
 *         kafka bin/zookeeper-server-start.sh config/zookeeper.properties
 *         bin/kafka-server-start.sh config/server.properties
 *         
 */
public class Main {
	private static final Logger logger = LogManager.getLogger(Main.class);
	
	private static Properties properties = new Properties();

	private static final String CONSUMER = "consumer";
	private static final String PRODUCER = "producer";
	
	private static final String PROP_PRODUCERS = "main.producers";
	private static final String PROP_MESSAGE_SIZE = "main.message.size";
	private static final String PROP_BATCH_SIZE = "main.batch.size";
	private static final String PROP_BROKER = "main.type";
	private static final String PROP_ACTIVEMQ_URL = "broker.activemq.url";
	private static final String PROP_RABBITMQ_URL = "broker.rabbitmq.url";
	private static final String PROP_KAFKA_URL = "broker.kafka.url";
	private static final String PROP_SQS_URL = "broker.sqs.url";

	private static final String BROKER_ACTIVEMQ = "activemq";
	private static final String BROKER_RABBITMQ = "rabbitmq";
	private static final String BROKER_KAFKA = "kafka";
	private static final String BROKER_SQS = "sqs";
	
	public static void main(String[] args) throws Exception {
		properties.load(Main.class.getClassLoader().getResourceAsStream("testbed.properties"));
		if(args.length == 0) {
			throw new Exception("Missing application parameters");
		}
		
		String type = args[0];
		int numProducers = Integer.valueOf(properties.getProperty(PROP_PRODUCERS));
		double messageSizeInKB = Double.valueOf(properties.getProperty(PROP_MESSAGE_SIZE));
		int batchSize = Integer.valueOf(properties.getProperty(PROP_BATCH_SIZE));
		if (PRODUCER.equals(type)) {
			startProducers(numProducers, messageSizeInKB, batchSize);
		} else if (CONSUMER.equals(type)) {
			startConsumer(numProducers);
		} else {
			throw new Exception("Invalid app type specified");
		}

	}

	public static void startProducers(int numProducers, double messageSizeInKB, int batchSize) throws Exception {
		String brokerType = properties.getProperty(PROP_BROKER);
		logger.info("Start {} {} producers", brokerType, numProducers);
		for (int i = 1; i <= numProducers; i++) {
			if((i % batchSize) == 0) {
				Thread.sleep(100);
			}
			
			Producer producer = initProducer(brokerType, messageSizeInKB);
			startThread(producer, false);
		}
		
		logger.info("Completed {} producers", numProducers);
	}

	public static void startConsumer(int expectedMessages) throws Exception {
		String brokerType = properties.getProperty(PROP_BROKER);
		logger.debug("Start {} consumer", brokerType);
		Consumer consumer = initConsumer(brokerType, expectedMessages);
		consumer.init();
	}
	
	private static Producer initProducer(String brokerType, double messageSizeInKB) throws Exception {
		Producer producer;
		if(BROKER_ACTIVEMQ.equals(brokerType)) {
			producer = new ActiveMqProducer(properties.getProperty(PROP_ACTIVEMQ_URL), messageSizeInKB);
		} else if(BROKER_RABBITMQ.equals(brokerType)) {
			producer = new RabbitMqProducer(properties.getProperty(PROP_RABBITMQ_URL), messageSizeInKB);
		} else if(BROKER_KAFKA.equals(brokerType)) {
			producer = new KafkaMsgProducer(properties.getProperty(PROP_KAFKA_URL), messageSizeInKB);
		} else if(BROKER_SQS.equals(brokerType)) {
			producer = new SQSProducer(properties.getProperty(PROP_SQS_URL), messageSizeInKB);
		} else {
			throw new Exception("Invalid broker type specified");
		}
		return producer;
	}
	
	private static Consumer initConsumer(String brokerType, int expectedMessages) throws Exception {
		Consumer consumer;
		if(BROKER_ACTIVEMQ.equals(brokerType)) {
			consumer = new ActiveMqConsumer(properties.getProperty(PROP_ACTIVEMQ_URL), expectedMessages);
		} else if(BROKER_RABBITMQ.equals(brokerType)) {
			consumer = new RabbitMqReceiver(properties.getProperty(PROP_RABBITMQ_URL), expectedMessages);
		} else if(BROKER_KAFKA.equals(brokerType)) {
			consumer = new KafkaMsgConsumer(properties.getProperty(PROP_KAFKA_URL), expectedMessages);
		} else if(BROKER_SQS.equals(brokerType)) {
			consumer = new SQSConsumer(properties.getProperty(PROP_SQS_URL), expectedMessages);
		} else {
			throw new Exception("Invalid broker type specified");
		}
		return consumer;
	}
	
	public static void startThread(Runnable runnable, boolean daemon) {
		Thread brokerThread = new Thread(runnable);
		brokerThread.setDaemon(daemon);
		brokerThread.start();
	}
}
