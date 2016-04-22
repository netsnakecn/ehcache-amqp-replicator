package com.maicard.misc.ehcache.amqp;

import net.sf.ehcache.Ehcache;

import static com.maicard.misc.ehcache.amqp.AMQPUtil.EXCHANGE_NAME;
import static com.maicard.misc.ehcache.amqp.AMQPUtil.HOST;
import static com.maicard.misc.ehcache.amqp.AMQPUtil.PASSWORD;
import static com.maicard.misc.ehcache.amqp.AMQPUtil.PORT;
import static com.maicard.misc.ehcache.amqp.AMQPUtil.QUEUE_NAME;
import static com.maicard.misc.ehcache.amqp.AMQPUtil.USERNAME;
import static com.maicard.misc.ehcache.amqp.AMQPUtil.TIMEOUT_MILLIS;
import net.sf.ehcache.loader.CacheLoader;
import net.sf.ehcache.loader.CacheLoaderFactory;
import net.sf.ehcache.util.PropertyUtil;

import java.util.Properties;
import java.util.logging.Logger;

import com.rabbitmq.client.Channel;

public class AMQPCacheLoaderFactory extends CacheLoaderFactory {

	    /**
	     * The default timeoutMillis - time in milliseconds to wait for a reply from a JMS Cache Loader. 
	     */
	    protected static final int DEFAULT_TIMEOUT_INTERVAL_MILLIS = 30000;

	    private static final Logger LOG = Logger.getLogger(AMQPCacheLoaderFactory.class.getName());

	    /**
	     * Creates a CacheLoader using the Ehcache configuration mechanism at the time the associated cache
	     * is created.
	     *
	     * @param properties implementation specific properties. These are configured as comma
	     *                   separated name value pairs in ehcache.xml
	     * @return a constructed CacheLoader
	     */
	    public CacheLoader createCacheLoader(Ehcache cache, Properties properties) {
	    	
	    	String host = PropertyUtil.extractAndLogProperty(HOST, properties);
			String port = PropertyUtil.extractAndLogProperty(PORT, properties);
			String username = PropertyUtil.extractAndLogProperty(USERNAME, properties);
			String password = PropertyUtil.extractAndLogProperty(PASSWORD, properties);
			String timeoutMillis = PropertyUtil.extractAndLogProperty(TIMEOUT_MILLIS, properties);
			//LOG.fine("Creating TopicSession in " + effectiveAcknowledgementMode.name() + " mode.");

			String exchangeName = PropertyUtil.extractAndLogProperty(EXCHANGE_NAME, properties);
			String queueName = PropertyUtil.extractAndLogProperty(QUEUE_NAME, properties);

			Channel channel= AMQPUtil.createChannel(host, Integer.parseInt(port), username, password, exchangeName, queueName);



	       

	        return new AMQPCacheLoader(cache, channel, Long.parseLong(timeoutMillis));
	    }





	    /**
	     * Extracts the value of timeoutMillis. Sets it to 30000ms if
	     * either not set or there is a problem parsing the number
	     *
	     * @param properties
	     */
	    protected int extractTimeoutMillis(Properties properties) {
	        int timeoutMillis = 0;
	        String timeoutMillisString =
	                PropertyUtil.extractAndLogProperty(TIMEOUT_MILLIS, properties);
	        if (timeoutMillisString != null) {
	            try {
	                timeoutMillis = Integer.parseInt(timeoutMillisString);
	            } catch (NumberFormatException e) {
	                LOG.warning("Number format exception trying to set timeoutMillis. " +
	                        "Using the default instead. String value was: '" + timeoutMillisString + "'");
	                timeoutMillis = DEFAULT_TIMEOUT_INTERVAL_MILLIS;
	            }
	        } else {
	            timeoutMillis = DEFAULT_TIMEOUT_INTERVAL_MILLIS;
	        }
	        return timeoutMillis;
	    }

	}

