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
import static com.maicard.misc.ehcache.amqp.AMQPUtil.QUEUE_NAME;

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

		LOG.fine("createCachePeerProvider ( cacheManager = " + cacheManager
				+ ", properties = " + properties + " ) called ");






		String host = PropertyUtil.extractAndLogProperty(HOST, properties);
		String port = PropertyUtil.extractAndLogProperty(PORT, properties);
		String username = PropertyUtil.extractAndLogProperty(USERNAME, properties);
		String password = PropertyUtil.extractAndLogProperty(PASSWORD, properties);
		String exchangeName = PropertyUtil.extractAndLogProperty(EXCHANGE_NAME, properties);
		String queueName = PropertyUtil.extractAndLogProperty(QUEUE_NAME, properties);

		Channel channel= AMQPUtil.createChannel(host, Integer.parseInt(port), username, password, exchangeName, queueName);
		return new AMQPCacheManagerPeerProvider(cacheManager, channel, queueName, exchangeName);
	}




}


