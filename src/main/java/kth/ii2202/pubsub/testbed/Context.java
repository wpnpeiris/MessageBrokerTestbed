/**
 * 
 */
package kth.ii2202.pubsub.testbed;

import java.io.IOException;
import java.util.Properties;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * @author pradeeppeiris
 *
 */
public class Context {
	private static final Logger logger = LogManager.getLogger(Context.class);
	private Properties properties = new Properties();
	
	private static Context instance;
	
	private Context() {
		try {
			properties.load(Context.class.getClassLoader().getResourceAsStream("testbed.properties"));
		} catch (IOException e) {
			logger.error(e);
		}
	}
	
	public static Context getInstance() {
		if(instance == null) {
			instance = new Context();
		}
		
		return instance;
	}
	
	public String getProperty(String key) {
		return properties.getProperty(key);
	}
}
