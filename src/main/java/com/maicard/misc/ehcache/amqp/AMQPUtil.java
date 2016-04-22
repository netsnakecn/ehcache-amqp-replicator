package com.maicard.misc.ehcache.amqp;

import net.sf.ehcache.CacheException;
import net.sf.ehcache.Ehcache;
import net.sf.ehcache.CacheManager;

import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import java.util.Properties;
import java.util.logging.Logger;


public final class AMQPUtil {


    public static final String QUEUE_NAME = "amqpQueueName";

	    /***/
	    public static final String EXCHANGE_NAME = "amqpExchangeName";

	    /***/
	    public static final String HOST = "amqpHost";

	    /***/
	    public static final String PORT = "amqpPort";

	    /***/
	    public static final String USERNAME = "amqpUsername";

	    /***/
	    public static final String PASSWORD = "amqpPassword";


	    /***/
	    public static final String TIMEOUT_MILLIS = "timeoutMillis";

	    /***/
	    public static final String DEFAULT_LOADER_ARGUMENT = "defaultLoaderArgument";

	    /***/
	    public static final int MAX_PRIORITY = 9;
	    
	    public static final String ACTION_PROPERTY = "action";

	    public static final String CACHE_NAME_PROPERTY = "cacheName";

	    public static final String KEY_PROPERTY = "key";

	    /***/
	    public static final String CACHE_MANAGER_UID = "cacheManagerUniqueId";

	    public static final int CONSUMBER_QUERY_INTERVAL = 5;
	    

	    private static final Logger LOG = Logger.getLogger(AMQPUtil.class.getName());

	   /* private AMQPUtil() {
	        //Utility class
	    }
*/
	   
	    public static Context createInitialContext(String amqpHost, String amqpPort, String amqpUsername, String amqpPassword, String exchangeName) {
	        Context context;

	        Properties env = new Properties();

	        if (amqpHost != null) {
	            env.put(HOST, amqpHost);
	        }
	        if (amqpPort != null) {
	            env.put(PORT, amqpPort);
	        }
	        if (amqpUsername != null) {
	            env.put(USERNAME, amqpUsername);
	        }
	        if (amqpPassword != null) {
	            env.put(PASSWORD, amqpPassword);
	        }

	        env.put(exchangeName, EXCHANGE_NAME);

	      
	        try {
	            context = new InitialContext(env);
	        } catch (NamingException ne) {

	            throw new CacheException("NamingException " + ne.getMessage(), ne);
	        }
	        return context;
	    }

	   
	   
	    public static int localCacheManagerUid(Ehcache cache) {
	        return localCacheManagerUid(cache.getCacheManager());
	    }


	    /**
	     * Returns a unique ID for a CacheManager. This method always returns the same value
	     * for the life of a CacheManager instance.
	     *
	     * @param cacheManager the CacheManager of interest
	     * @return an identifier for the local CacheManager
	     */
	    public static int localCacheManagerUid(CacheManager cacheManager) {
	        return System.identityHashCode(cacheManager);
	    }
	    
	    public static Channel createChannel(String host, int port, String username, String password, String exchangeName, String queueName){
	    	Channel channel = null;
			Connection  connection = null;

			ConnectionFactory connectionFactory;




			connectionFactory = new ConnectionFactory();
			connectionFactory.setHost(host);
			connectionFactory.setPort(port);
			connectionFactory.setUsername(username);
			connectionFactory.setPassword(password);
			LOG.severe("Connect to server[host=" + host + ",port=" + port + ",username=" + username + ",password=" + password + ",exchange=" + exchangeName + "]");
			try{
				connection = connectionFactory.newConnection();  
				channel = connection.createChannel();  
				//Create fanout exchange
				channel.exchangeDeclare(exchangeName,"fanout");
				channel.queueDeclare(queueName, false, false, false, null);  
			}catch(Exception e){
				e.printStackTrace();
			}
			return channel;
	    }
	    
	    
	    
	}

