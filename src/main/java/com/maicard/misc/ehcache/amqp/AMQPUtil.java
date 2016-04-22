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

	    /***/
	    public static final String CACHE_MANAGER_UID = "cacheManagerUniqueId";


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
	    
	    
	    public static void connectionTest(){
	    	String host = "124.133.240.120";
	    	String port = "5672";
	    	String username = "mq";
	    	String password = "vectAO";
	    	String exchangeName = "chaoka";
	    	
	         String message = "Hello World!";  
	         
	         
			try {

				ConnectionFactory		connectionFactory = new ConnectionFactory();
				connectionFactory.setHost(host);
				connectionFactory.setPort(Integer.parseInt(port));
				connectionFactory.setUsername(username);
				connectionFactory.setPassword(password);
				
				
				Connection connection = connectionFactory.newConnection();  
		        Channel channel = connection.createChannel();  
				
				
		         channel.queueDeclare(exchangeName, false, false, false, null);  
		         
		         channel.basicPublish("", exchangeName, null, message.getBytes());  
		         System.out.println(" [x] Sent '" + message + "'");  
		   
		         channel.close();  
		         connection.close();  
		         
		         
		         
		         
			} catch (Exception ne) {
				ne.printStackTrace();
			}

	    }
	    
	    public static void main(String[] argv){
	    	AMQPUtil.connectionTest();
	    }
	}

