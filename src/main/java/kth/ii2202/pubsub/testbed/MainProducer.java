package kth.ii2202.pubsub.testbed;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

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
public class MainProducer {
	private static final Logger logger = LogManager.getLogger(MainProducer.class);

	private static final String PROP_NUM_BATCHES = "main.batch.count";
	private static final String PROP_MESSAGE_SIZE = "main.message.size";
	private static final String PROP_BATCH_SIZE = "main.batch.size";
	
	public static void main(String[] args) throws Exception {
		Context context = Context.getInstance();
		int numBatches = Integer.valueOf(context.getProperty(PROP_NUM_BATCHES));
		double messageSizeInKB = Double.valueOf(context.getProperty(PROP_MESSAGE_SIZE));
		int batchSize = Integer.valueOf(context.getProperty(PROP_BATCH_SIZE));
		startProducers(numBatches, messageSizeInKB, batchSize);
	}

	public static void startProducers(int numBatches, double messageSizeInKB, int batchSize) throws Exception {
		logger.info("Start producers; number of batches{}, batch size {}", numBatches, batchSize);
		for(int i = 0; i < numBatches; i++) {
			Producer messageProducer = ProducerFactory.getMessageProducer();
			messageProducer.generateMessages(batchSize, messageSizeInKB);
			Thread.sleep(1);
		}
		
		logger.info("Completed {} producers", numBatches);
	}
}
