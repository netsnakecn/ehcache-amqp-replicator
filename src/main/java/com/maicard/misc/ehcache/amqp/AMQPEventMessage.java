package com.maicard.misc.ehcache.amqp;


import net.sf.ehcache.Element;
import net.sf.ehcache.distribution.LegacyEventMessage;

import java.io.Serializable;

public class AMQPEventMessage extends LegacyEventMessage {
	

	    /**
	     * A JMS message property which contains the name of the cache to operate on.
	     */
	    public static final String CACHE_NAME_PROPERTY = "cacheName";

	    /**
	     * A JMS message property which contains the mimeType of the message.
	     * Applies to the <code>PUT</code> action. If not set the message is interpreted as follows:
	     * <ul>
	     * <li>ObjectMessage - if it is an net.sf.ehcache.Element, then it is treated as such and stored in the cache. All others
	     * are stored in the cache as value of  MimeTypeByteArray. The mimeType is stored as type of <code>application/x-java-serialized-object</code>.
	     * When the ObjectMessage is of type net.sf.ehcache.Element, the mimeType is ignored.
	     * <li>TextMessage - Stored in the cache as value of MimeTypeByteArray. The mimeType is stored as type of <code>text/plain</code>.
	     * <li>BytesMessage - Stored in the cache as value of MimeTypeByteArray. The mimeType is stored as type of <code>application/octet-stream</code>.
	     * </ul>
	     * Other message types are not supported.
	     * <p/>
	     * To send XML use a TextMessage and set the mimeType to <code>application/xml</code>.It will be stored in the cache
	     * as a value of MimeTypeByteArray.
	     */
	    public static final String MIME_TYPE_PROPERTY = "mimeType";

	    /**
	     * A JMS message property which contains the action to perform on the cache.
	     * Available actions are <code>PUT</code>, <code>REMOVE</code> and <code>REMOVE_ALL</code>.
	     * If not set no action is performed.
	     */
	    public static final String ACTION_PROPERTY = "action";

	    /**
	     * The key in the cache on which to operate on.
	     * The <code>REMOVE_ALL</code> action does not require a key.
	     * <p/>
	     * If an ObjectMessage of type net.sf.ehcache.Element is sent, the key is contained in the element. Any key
	     * set as a property is ignored.
	     */
	    public static final String KEY_PROPERTY = "key";

	    private static final long serialVersionUID = 927345728947584L;

	    private String cacheName;
	    private Serializable loaderArgument;

	    /**
	     * @param action    one of the types from Action
	     * @param key       the key of the Element. May be null for the removeAll message type. For loadAll requests,
	     *                  key may actually be an ArrayList of keys.
	     * @param element   may be null for removal and invalidation message types
	     * @param cacheName the name of the cache in the CacheManager.
	     */
	    public AMQPEventMessage(Action action,
	                           Serializable key,
	                           Element element,
	                           String cacheName,
	                           Serializable loaderArgument) {
	        super(action.toInt(), key, element);
	        setCacheName(cacheName);
	        this.loaderArgument = loaderArgument;
	    }

	    /**
	     * Returns the cache name
	     *
	     * @return the cache name in the CacheManager.
	     */
	    public String getCacheName() {
	        return cacheName;
	    }

	    /**
	     * Sets the cache name.
	     *
	     * @param cacheName the name of the cache in the CacheManager
	     */
	    public void setCacheName(String cacheName) {
	        this.cacheName = cacheName;
	    }

	    /**
	     * @return the loader argument. May be and is usually null.
	     */
	    public Serializable getLoaderArgument() {
	        return loaderArgument;
	    }

	    /**
	     * @param loaderArgument
	     */
	    public void setLoaderArgument(Serializable loaderArgument) {
	        this.loaderArgument = loaderArgument;
	    }

	    /**
	     * Returns the message as a String
	     *
	     * @return a String represenation of the message
	     */
	    @Override
	    public String toString() {
	        return new StringBuilder().append("JMSEventMessage ( event = ").append(getEvent()).append(", element = ")
	                .append(getElement()).append(", cacheName = ").append(cacheName)
	                .append(", key = ").append(getSerializableKey())
	                .append(", loaderArgument = ").append(loaderArgument).append(" )").toString();
	    }

	
}
