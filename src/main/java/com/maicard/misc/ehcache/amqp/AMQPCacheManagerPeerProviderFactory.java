package com.maicard.misc.ehcache.amqp;


import net.sf.ehcache.CacheManager;
import net.sf.ehcache.distribution.CacheManagerPeerProvider;
import net.sf.ehcache.distribution.CacheManagerPeerProviderFactory;
import net.sf.ehcache.util.PropertyUtil;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import static com.maicard.misc.ehcache.amqp.AMQPUtil.EXCHANGE_NAME;
import static com.maicard.misc.ehcache.amqp.AMQPUtil.HOST;
import static com.maicard.misc.ehcache.amqp.AMQPUtil.PASSWORD;
import static com.maicard.misc.ehcache.amqp.AMQPUtil.PORT;
import static com.maicard.misc.ehcache.amqp.AMQPUtil.USERNAME;

import java.util.Properties;
import java.util.logging.Logger;
import java.util.logging.Level;

/**
 *
 *
 * @author NetSnake<netsnakecn@163.com>
 * 
 */
public class AMQPCacheManagerPeerProviderFactory  extends CacheManagerPeerProviderFactory{

	private static final Logger LOG = Logger.getLogger(AMQPCacheManagerPeerProviderFactory.class.getName());


	@Override
	public CacheManagerPeerProvider createCachePeerProvider(CacheManager cacheManager, Properties properties) {

		if (LOG.isLoggable(Level.FINEST)) {
			LOG.finest("createCachePeerProvider ( cacheManager = " + cacheManager
					+ ", properties = " + properties + " ) called ");
		}





		String host = PropertyUtil.extractAndLogProperty(HOST, properties);
		String port = PropertyUtil.extractAndLogProperty(PORT, properties);
		String userName = PropertyUtil.extractAndLogProperty(USERNAME, properties);
		String password = PropertyUtil.extractAndLogProperty(PASSWORD, properties);

		//LOG.fine("Creating TopicSession in " + effectiveAcknowledgementMode.name() + " mode.");

		String exchangeName = PropertyUtil.extractAndLogProperty(EXCHANGE_NAME, properties);

		Channel channel = null;
		Connection  connection = null;

		ConnectionFactory connectionFactory;




		connectionFactory = new ConnectionFactory();
		connectionFactory.setHost(host);
		connectionFactory.setPort(Integer.parseInt(port));
		connectionFactory.setUsername(userName);
		connectionFactory.setPassword(password);
		LOG.severe("Connect to server[host=" + host + ",port=" + port + ",username=" + userName + ",password=" + password + ",exchange=" + exchangeName + "]");
		try{
			connection = connectionFactory.newConnection();  
			channel = connection.createChannel();  
			channel.queueDeclare(exchangeName, false, false, false, null);  
		}catch(Exception e){
			e.printStackTrace();
		}
		return new AMQPCacheManagerPeerProvider(cacheManager, channel);
	}




}


