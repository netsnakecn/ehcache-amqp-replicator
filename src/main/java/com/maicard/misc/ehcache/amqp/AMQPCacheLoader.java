package com.maicard.misc.ehcache.amqp;

import net.sf.ehcache.Ehcache;
import net.sf.ehcache.Status;
import net.sf.ehcache.loader.CacheLoader;
import net.sf.ehcache.CacheException;

import javax.jms.MessageConsumer;
import javax.jms.Queue;
import javax.jms.QueueConnection;
import javax.jms.QueueSession;
import javax.jms.TemporaryQueue;

import com.rabbitmq.client.Channel;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.logging.Logger;

public class AMQPCacheLoader  implements CacheLoader {

	    /**
	     * The highest JMS priority
	     */
	    protected static final int HIGHEST_JMS_PRORITY = 9;

	    private static final Logger LOG = Logger.getLogger(AMQPCacheLoader.class.getName());

	    /***/
	    protected Channel channel;

	    /***/
	    protected QueueSession getQueueSession;

	    /***/
	    protected long timeoutMillis;

	    /***/
	    protected Ehcache cache;


	//    private AcknowledgementMode acknowledgementMode;
	    private Status status;
	    private QueueConnection getQueueConnection;
	    private String defaultLoaderArgument;
	    private Queue getQueue;


	    /**
	     * Constructor.
	     *
	     * @param cache
	     * @param defaultLoaderArgument
	     * @param getQueueConnection
	     * @param getQueue
	     * @param acknowledgementMode
	     * @param timeoutMillis
	     */
	    public AMQPCacheLoader(Ehcache cache,
	                          Channel channel,
	                          long timeoutMillis) {

	        this.cache = cache;
	        this.channel = channel;
	        this.timeoutMillis = timeoutMillis;
	        status = Status.STATUS_UNINITIALISED;
	    }


	    /**
	     * loads an object. Application writers should implement this
	     * method to customize the loading of cache object. This method is called
	     * by the caching service when the requested object is not in the cache.
	     * <p/>
	     *
	     * @param key the key identifying the object being loaded
	     * @return The object that is to be stored in the cache.
	     * @throws net.sf.jsr107cache.CacheException
	     *
	     */
	    @Override
	    public Object load(Object key) throws CacheException {
	        return load(key, null);
	    }


	    /**
	     * Load using both a key and an argument.
	     * <p/>
	     * JCache will call through to the load(key) method, rather than this method, where the argument is null.
	     *
	     * @param key      the key to load the object for.
	     * @param argument can be anything that makes sense to the loader.
	     *                 The argument is converted to a String with toString()
	     *                 to use for the JMS StringProperty loaderArgument
	     * @return the Object loaded
	     * @throws net.sf.jsr107cache.CacheException
	     *
	     */
	    @Override
	    public Object load(Object key, Object argument) throws CacheException {
	        Serializable keyAsSerializable = (Serializable) key;
	        Serializable effectiveLoaderArgument = effectiveLoaderArgument(argument);

	        AMQPEventMessage jmsEventMessage = new AMQPEventMessage(Action.GET,
	                keyAsSerializable, null, cache.getName(), effectiveLoaderArgument);

	        return loadFromJMS(jmsEventMessage);
	    }

	    /**
	     * A common loader which handles the JMS interactions.
	     *
	     * @param jmsEventMessage
	     * @return the object loaded from JMS.
	     * @throws CacheException
	     */
	    protected Object loadFromJMS(AMQPEventMessage jmsEventMessage) throws CacheException {
	        Object value;
	        MessageConsumer replyReceiver = null;
	        TemporaryQueue temporaryReplyQueue = null;
	        LOG.fine("Load from AMQP message");
	        return null;
	    }


	    /**
	     * loads multiple object. Application writers should implement this
	     * method to customize the loading of cache object. This method is called
	     * by the caching service when the requested object is not in the cache.
	     * <p/>
	     *
	     * @param keys a Collection of keys identifying the objects to be loaded
	     * @return A Map of objects that are to be stored in the cache.
	     * @throws net.sf.jsr107cache.CacheException
	     *
	     */
	    public Map loadAll(Collection keys) throws CacheException {
	        return loadAll(keys, null);
	    }

	    /**
	     * Load using both a key and an argument.
	     * <p/>
	     * JCache will use the loadAll(key) method where the argument is null.
	     *
	     * @param keys     a <code>Collection</code> of keys to load objects for. Each key must be <code>Serializable</code>.
	     * @param argument can be anything that makes sense to the loader. It must be <code>Serializable</code>.
	     * @return a map of Objects keyed by the collection of keys passed in.
	     * @throws net.sf.jsr107cache.CacheException
	     *
	     */
	    public Map loadAll(Collection keys, Object argument) throws CacheException {

	        Serializable effectiveLoaderArgument;
	        effectiveLoaderArgument = effectiveLoaderArgument(argument);

	        ArrayList<Serializable> requestList = new ArrayList<Serializable>();
	        for (Object key : keys) {
	            Serializable keyAsSerializable = (Serializable) key;
	            requestList.add(keyAsSerializable);
	        }

	        Map responseMap;
	        AMQPEventMessage jmsEventMessage = new AMQPEventMessage(Action.GET,
	                requestList, null, cache.getName(), effectiveLoaderArgument);
	        responseMap = (Map) loadFromJMS(jmsEventMessage);
	        return responseMap;
	    }

	    private Serializable effectiveLoaderArgument(Object argument) {
	        Serializable effectiveLoaderArgument;
	        if (argument == null) {
	            effectiveLoaderArgument = defaultLoaderArgument;
	        } else {
	            effectiveLoaderArgument = (Serializable) argument;
	        }
	        return effectiveLoaderArgument;
	    }

	    /**
	     * Gets the name of a CacheLoader
	     *
	     * @return the name of this CacheLoader
	     */
	    public String getName() {
	        return "AMQPCacheLoader with default loaderArgument: " + defaultLoaderArgument;
	    }

	    public CacheLoader clone(Ehcache cache) throws CloneNotSupportedException {
	        throw new CloneNotSupportedException("not supported");
	    }


	    public void init() {
	    	LOG.fine("AMQPCacheLoader initialized.");
	            
	            status = Status.STATUS_ALIVE;
	        
	    }


	    public void dispose() throws net.sf.ehcache.CacheException {
	    	LOG.fine("AMQPCacheLoader is shutdown.");

	        status = Status.STATUS_SHUTDOWN;
	    }

	    /**
	     * @return the status of the extension
	     */
	    public Status getStatus() {
	    	LOG.fine("AMQPCacheLoader currrent status:" + status);

	        return status;
	    }

	}
