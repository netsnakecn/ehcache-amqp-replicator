package com.maicard.misc.ehcache.amqp;


import net.sf.ehcache.CacheManager;
import net.sf.ehcache.Ehcache;
import net.sf.ehcache.Element;
import net.sf.ehcache.MimeTypeByteArray;
import net.sf.ehcache.distribution.CachePeer;
import net.sf.ehcache.util.CacheTransactionHelper;

import static com.maicard.misc.ehcache.amqp.AMQPUtil.ACTION_PROPERTY;
import static com.maicard.misc.ehcache.amqp.AMQPUtil.CACHE_MANAGER_UID;
import static com.maicard.misc.ehcache.amqp.AMQPUtil.CACHE_NAME_PROPERTY;
import static com.maicard.misc.ehcache.amqp.AMQPUtil.KEY_PROPERTY;
import static com.maicard.misc.ehcache.amqp.AMQPUtil.CONSUMBER_QUERY_INTERVAL;
import javax.jms.BytesMessage;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.ObjectMessage;
import javax.jms.Queue;
import javax.jms.QueueSender;
import javax.jms.QueueSession;
import javax.jms.TextMessage;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.QueueingConsumer;

import java.io.IOException;
import java.io.Serializable;
import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

public class AMQPCachePeer implements CachePeer  {



	private static final Logger LOG = Logger.getLogger(AMQPCachePeer.class.getName());

	/**
	 * Used only in testing
	 */
	private static final int TEST_DELAY = 11000;

	/***/
	protected Channel channel;

	private CacheManager cacheManager;
	private boolean shutdown;
	private QueueSession getQueueSession;




	/**
	 * Constructor
	 */
	public AMQPCachePeer(CacheManager cacheManager,
			Channel channel, String queueName, String exchangeName) {


		LOG.fine("JMSCachePeer constructor ( cacheManager = "
				+ cacheManager
				+ ", channel = " + channel + " ) called");

		AMQPConsumer amqpConsumer  = new AMQPConsumer(channel, queueName, exchangeName);
		new Thread(amqpConsumer).start();
	}


	/**
	 * Cleanup on shutdown
	 * @throws IOException 
	 */
	public void dispose() throws JMSException, IOException {

		channel.close();
		cacheManager = null;
		shutdown = true;

	}

	/**
	 * Process a cache replication message.
	 * <p/>
	 * Unwraps the JMSEventMessage and performs the cache action
	 * <p/>
	 *
	 * @param message the message, which contains a payload and action
	 * @param cache   the cache to perform the action upon
	 */
	private void handleNotification(AMQPEventMessage message, Ehcache cache) {

		if (LOG.isLoggable(Level.FINEST)) {
			LOG.finest("handleNotification ( message = " + message + " ) called ");
		}

		int event = message.getEvent();

		switch (event) {
		case AMQPEventMessage.PUT:
			Element element = message.getElement();
			put(cache, element);
			break;
		case AMQPEventMessage.REMOVE:
			remove(cache, message.getSerializableKey());
			break;
		case AMQPEventMessage.REMOVE_ALL:
			removeAll(cache);
			break;
		default:
			if (LOG.isLoggable(Level.FINE)) {
				LOG.severe(" Undefined action " + event);
			}
		}
	}


	/**
	 * Process a non-cache message
	 * <p/>
	 * Performs the cache action
	 *
	 * @param element the element which was sent over JMS in an ObjectMessage
	 * @param cache   the cache to perform the action upon
	 * @param action  the action to perform
	 */
	private void handleNotification(Element element, Serializable key, Ehcache cache, Action action) {

		if (LOG.isLoggable(Level.FINEST)) {
			LOG.finest("handleNotification ( element = " + element + " ) called ");
		}

		if (action.equals(Action.PUT)) {
			put(cache, element);
		} else if (action.equals(Action.REMOVE)) {
			remove(cache, key);
		} else if (action.equals(Action.REMOVE_ALL)) {
			removeAll(cache);
		}
	}

	/**
	 * Process a non-cache message
	 * <p/>
	 * Performs the cache action
	 *
	 * @param cache  the cache to perform the action upon
	 * @param action the action to perform
	 */
	private void handleNotification(Object object, Serializable key, Ehcache cache, Action action) {

		Element element = new Element(key, object);
		if (action.equals(Action.PUT)) {
			put(cache, element);
		} else if (action.equals(Action.REMOVE)) {
			remove(cache, key);
		} else if (action.equals(Action.REMOVE_ALL)) {
			removeAll(cache);
		}
	}

	private void removeAll(Ehcache cache) {
		if (LOG.isLoggable(Level.FINEST)) {
			LOG.finest("removeAll ");
		}
		cache.removeAll(true);
	}

	private void remove(Ehcache cache, Serializable key) {
		if (LOG.isLoggable(Level.FINEST)) {
			LOG.finest("remove ( key = " + key + " ) ");
		}
		cache.remove(key, true);
	}

	private void put(Ehcache cache, Element element) {
		if (LOG.isLoggable(Level.FINEST)) {
			LOG.finest("put ( element = " + element + " ) ");
		}
		cache.put(element, true);
	}


	public void send(List eventMessages) throws RemoteException {

		if (LOG.isLoggable(Level.FINEST)) {
			LOG.finest("send ( eventMessages = " + eventMessages + " ) called ");
		}

	}


	public void onMessage(Message message) {

		assert !shutdown : "Peer is shutdown. " + message;

	final Ehcache cache;
	try {
		cache = extractAndValidateCache(message);
	} catch (JMSException e) {
		LOG.log(Level.WARNING, "Unable to handle JMS Notification: " + e.getMessage(), e);
		return;
	}
	boolean started = CacheTransactionHelper.isTransactionStarted(cache);
	if (!started) {
		CacheTransactionHelper.beginTransactionIfNeeded(cache);
	}

	try {
		if (message instanceof ObjectMessage) {
			handleObjectMessage(message);
		} else if (message instanceof TextMessage) {
			handleTextMessage(message);
		} else if (message instanceof BytesMessage) {
			handleBytesMessage(message);
		} else {
			throw new InvalidAMQPMessageException("Cannot handle message of type (class=" + message.getClass().getName()
					+ "). Notification ignored.");
		}
	} catch (Exception e) {
		LOG.log(Level.WARNING, "Unable to handle JMS Notification: " + e.getMessage(), e);
	} finally {
		if (!started) {
			CacheTransactionHelper.commitTransactionIfNeeded(cache);
		}
	}
	}

	private void handleObjectMessage(Message message) throws JMSException, RemoteException {
		ObjectMessage objectMessage = (ObjectMessage) message;
		Object object = objectMessage.getObject();


		//If a non-cache publisher sends an Element
		if (object instanceof Element) {
			if (LOG.isLoggable(Level.FINE)) {
				LOG.fine(getName() + ": Element message received - " + object);
			}


			Element element = (Element) object;

			Ehcache cache = extractAndValidateCache(objectMessage);
			Action action = extractAndValidateAction(objectMessage);
			//not required for Element
			Serializable key = extractAndValidateKey(objectMessage, action);
			handleNotification(element, key, cache, action);

		} else if (object instanceof AMQPEventMessage) {
			if (LOG.isLoggable(Level.FINE)) {
				LOG.fine(getName() + ": JMSEventMessage message received - " + object);
			}


			//no need for cacheName, mimeType, key or action properties as all are in message.
			AMQPEventMessage jmsEventMessage = (AMQPEventMessage) object;
			if (LOG.isLoggable(Level.FINE)) {
				LOG.fine(jmsEventMessage.toString());
			}

			Ehcache cache = extractAndValidateCache(objectMessage);
			if (jmsEventMessage.getEvent() == Action.GET.toInt()) {
				handleGetRequest(objectMessage, jmsEventMessage, cache);
			} else {
				handleNotification(jmsEventMessage, cache);
			}

		} else {
			LOG.fine(getName() + ": Other ObjectMessage received - " + object);


			//no need for mimeType. An object has a type
			Ehcache cache = extractAndValidateCache(objectMessage);
			Action action = extractAndValidateAction(objectMessage);
			Serializable key = extractAndValidateKey(objectMessage, action);
			handleNotification(object, key, cache, action);
		}
	}

	private void handleTextMessage(Message message) throws RemoteException, JMSException {
		TextMessage textMessage = (TextMessage) message;
		if (LOG.isLoggable(Level.FINE)) {
			LOG.fine(getName() + ": Other ObjectMessage received - " + textMessage);
		}

		Ehcache cache = extractAndValidateCache(message);
		Action action = extractAndValidateAction(message);
		Serializable key = extractAndValidateKey(message, action);
		String mimeType = extractAndValidateMimeType(message, action);
		byte[] payload = new byte[0];
		if (textMessage.getText() != null) {
			payload = textMessage.getText().getBytes();
		}
		MimeTypeByteArray value = new MimeTypeByteArray(mimeType, payload);
		handleNotification(value, key, cache, action);
	}

	private void handleBytesMessage(Message message) throws RemoteException, JMSException {
		BytesMessage bytesMessage = (BytesMessage) message;
		if (LOG.isLoggable(Level.FINE)) {
			LOG.fine(getName() + ": Other ObjectMessage received - " + bytesMessage);
		}

		Ehcache cache = extractAndValidateCache(message);
		Action action = extractAndValidateAction(message);
		Serializable key = extractAndValidateKey(message, action);
		String mimeType = extractAndValidateMimeType(message, action);
		byte[] payload = new byte[(int) bytesMessage.getBodyLength()];
		bytesMessage.readBytes(payload);
		MimeTypeByteArray value = new MimeTypeByteArray(mimeType, payload);
		handleNotification(value, key, cache, action);
	}

	private void handleGetRequest(ObjectMessage objectMessage, AMQPEventMessage jmsEventMessage, Ehcache cache)
			throws JMSException {
		if (LOG.isLoggable(Level.FINE)) {
			LOG.fine(cacheManager.getName() + ": JMSEventMessage message received - " + objectMessage.getJMSMessageID());
		}
		Serializable keyOrKeys = jmsEventMessage.getSerializableKey();
		boolean collectionLoad = false;
		if (keyOrKeys instanceof ArrayList) {
			collectionLoad = true;
		}

		QueueSender replyQueueSender = null;

		try {
			Serializable value = loadKeyOrKeys(cache, keyOrKeys, collectionLoad);

			int localCacheManagerUid = AMQPUtil.localCacheManagerUid(cache);
			LOG.log(Level.FINE, "Receiver CacheManager UID: {}", localCacheManagerUid);

			assert (objectMessage.getIntProperty(CACHE_MANAGER_UID) != localCacheManagerUid) :
				"The JMSCachePeer received a getQueue request sent by a JMSCacheLoader belonging to the same" +
				"CacheManager, which is invalid";
			ObjectMessage reply = getQueueSession.createObjectMessage(value);
			String name = null;
			try {
				name = getName();
			} catch (RemoteException e) {
				//impossible - local call
			}
			reply.setStringProperty("responder", name);
			reply.setJMSCorrelationID(objectMessage.getJMSMessageID());

			Queue replyQueue = (Queue) objectMessage.getJMSReplyTo();
			replyQueueSender = getQueueSession.createSender(replyQueue);
			replyQueueSender.send(reply);
		} finally {
			if (replyQueueSender != null) {
				replyQueueSender.close();
			}
		}

	}

	private Serializable loadKeyOrKeys(Ehcache cache, Serializable keyOrKeys, boolean collectionLoad) {
		if (collectionLoad) {
			ArrayList keys = (ArrayList) keyOrKeys;
			return loadKeys(cache, keys);
		} else {
			return loadKey(cache, keyOrKeys);
		}
	}

	private Serializable loadKey(Ehcache cache, Serializable key) {
		Element element = cache.get(key);
		delayForTest(key);
		Serializable value = null;
		if (element != null) {
			value = element.getValue();
		}
		return value;
	}

	private HashMap loadKeys(Ehcache cache, ArrayList keys) {
		HashMap<Serializable, Serializable> responseMap = new HashMap<Serializable, Serializable>(keys.size());

		for (Object listKey : keys) {
			Serializable key = (Serializable) listKey;
			Element element = cache.get(listKey);
			Serializable value;
			if (element != null) {
				value = element.getValue();
				responseMap.put(key, value);
			}
		}
		return responseMap;
	}

	/**
	 * @param key
	 */
	private void delayForTest(Serializable key) {
		if (key.equals("net.sf.ehcache.distribution.jms.Delay")) {
			try {
				Thread.sleep(TEST_DELAY);
			} catch (InterruptedException e) {
				//
			}
		}
	}

	private Serializable extractAndValidateKey(Message message, Action action) throws JMSException {
		String key = message.getStringProperty(KEY_PROPERTY);
		if (key == null && action.equals(Action.REMOVE)) {
			throw new InvalidAMQPMessageException("No key property specified. The key is required when the action is REMOVE.");
		}
		return key;
	}

	private String extractAndValidateMimeType(Message message, Action action) throws JMSException {
		String mimeType = message.getStringProperty(AMQPEventMessage.MIME_TYPE_PROPERTY);
		if (mimeType == null && action.equals(Action.PUT)) {
			if (message instanceof TextMessage) {
				mimeType = "text/plain";
			} else if (message instanceof BytesMessage) {
				mimeType = "application/octet-stream";
			}
			if (LOG.isLoggable(Level.FINE)) {
				LOG.fine("mimeType property not set. Auto setting MIME Type for message " + message.getJMSMessageID() + " to " + mimeType);
			}
		}
		return mimeType;
	}

	private Action extractAndValidateAction(Message message) throws JMSException {
		String actionString = message.getStringProperty(ACTION_PROPERTY);
		Action action = Action.valueOf(actionString);
		if (actionString == null || action == null) {
			throw new InvalidAMQPMessageException("No action specified. Must be one of PUT, REMOVE or REMOVE_ALL");
		}
		return action;
	}

	private Ehcache extractAndValidateCache(Message message) throws JMSException {
		String cacheName;
		if (message instanceof ObjectMessage && ((ObjectMessage) message).getObject() instanceof AMQPEventMessage) {
			cacheName = ((AMQPEventMessage) ((ObjectMessage) message).getObject()).getCacheName();
		} else {
			cacheName = message.getStringProperty(CACHE_NAME_PROPERTY);
		}
		if (cacheName == null) {
			throw new InvalidAMQPMessageException("No cache name specified.");
		}
		Ehcache cache = cacheManager.getEhcache(cacheName);
		if (cache == null) {
			throw new InvalidAMQPMessageException("No cache named " + cacheName + "exists in the target CacheManager.");
		}
		return cache;
	}

	/**
	 * Not implemented for JMS
	 *
	 * @param keys a list of serializable values which represent keys
	 * @return a list of Elements. If an element was not found or null, it will not be in the list.
	 */
	public List getElements(List keys) throws RemoteException {
		throw new RemoteException("Not implemented for JMS");
	}

	/**
	 * Not implemented for JMS
	 *
	 * @return a String representation of the GUID
	 * @throws RemoteException
	 */
	public String getGuid() throws RemoteException {
		throw new RemoteException("Not implemented for JMS");
	}

	/**
	 * Not implemented for JMS
	 *
	 * @return a list of {@link Object} keys
	 */
	public List getKeys() throws RemoteException {
		throw new RemoteException("Not implemented for JMS");
	}

	/**
	 * Not implemented for JMS
	 */
	public String getName() throws RemoteException {
		return cacheManager.getName() + " JMSCachePeer";
	}

	/**
	 * Not implemented for JMS
	 *
	 * @param key a serializable value
	 * @return the element, or null, if it does not exist.
	 */
	public Element getQuiet(Serializable key) throws RemoteException {
		throw new RemoteException("Not implemented for JMS");
	}

	/**
	 * Not implemented for JMS
	 *
	 * @return the URL as a string
	 */
	public String getUrl() throws RemoteException {
		throw new RemoteException("Not implemented for JMS");
	}

	/**
	 * The URL base for the remote replicator to connect. The value will have meaning
	 * only to a specific implementation of replicator and remote peer.
	 */
	public String getUrlBase() throws RemoteException {
		throw new RemoteException("Not implemented for JMS");
	}

	/**
	 * Not implemented for JMS
	 *
	 * @param element the element to put
	 * @throws IllegalStateException    if the cache is not {@link net.sf.ehcache.Status#STATUS_ALIVE}
	 * @throws IllegalArgumentException if the element is null
	 */
	public void put(Element element) throws IllegalArgumentException, IllegalStateException, RemoteException {
		throw new RemoteException("Not implemented for JMS");
	}

	/**
	 * Not implemented for JMS
	 *
	 * @param key the element key
	 * @return true if the element was removed, false if it was not found in the cache
	 * @throws IllegalStateException if the cache is not {@link net.sf.ehcache.Status#STATUS_ALIVE}
	 */
	public boolean remove(Serializable key) throws IllegalStateException, RemoteException {
		throw new RemoteException("Not implemented for JMS");
	}

	/**
	 * Not implemented for JMS
	 *
	 * @throws IllegalStateException if the cache is not {@link net.sf.ehcache.Status#STATUS_ALIVE}
	 */
	public void removeAll() throws RemoteException, IllegalStateException {
		throw new RemoteException("Not implemented for JMS");
	}


}

class AMQPConsumer implements Runnable{
	
	Channel channel = null;
	String queueName = null;
	String exchangeName = null;
	
	
	private static final Logger LOG = Logger.getLogger(AMQPCachePeer.class.getName());
	
	
	public AMQPConsumer(Channel channel, String queueName, String exchangeName){
		this.channel = channel;
		this.queueName = queueName;
		this.exchangeName = exchangeName;
	}

	@Override
	public void run() {
		LOG.fine("Begin AMQPCachePeer Consumer thread...");
		try{
			channel.queueBind(queueName, exchangeName, ""); // 绑定
			QueueingConsumer consumer = new QueueingConsumer(channel);
			channel.basicConsume(queueName, true, consumer);
			while (true) {
				QueueingConsumer.Delivery delivery = consumer.nextDelivery();
				String m = new String(delivery.getBody());
				LOG.fine("> [x] Received '" + m + "'");
				Thread.sleep(CONSUMBER_QUERY_INTERVAL * 1000);
			}
		}catch(Exception e){
			e.printStackTrace();
		}		
	}
	
}


