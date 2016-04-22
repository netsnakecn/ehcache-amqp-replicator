package com.maicard.misc.ehcache.amqp;

import net.sf.ehcache.CacheException;
import net.sf.ehcache.CacheManager;
import net.sf.ehcache.Ehcache;
import net.sf.ehcache.distribution.CacheManagerPeerProvider;
import net.sf.ehcache.distribution.CachePeer;
import javax.jms.JMSException;
import javax.jms.Queue;
import javax.jms.QueueConnection;
import javax.jms.QueueReceiver;
import javax.jms.QueueSession;
import javax.jms.TopicPublisher;
import javax.jms.TopicSession;
import javax.jms.TopicSubscriber;

import com.rabbitmq.client.Channel;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;


public class AMQPCacheManagerPeerProvider implements CacheManagerPeerProvider{
	

	    private static final Logger LOG = Logger.getLogger(AMQPCacheManagerPeerProvider.class.getName());

	    /***/
	    protected CacheManager cacheManager;

	    /***/
	    protected List<CachePeer> remoteCachePeers = new ArrayList<CachePeer>();


	    /***/
	    protected Channel channel;

	    /***/
	    protected QueueConnection getQueueConnection;

	    /***/
	    protected Queue getQueue;

	    /***/
	//    protected AcknowledgementMode acknowledgementMode;

	    /***/
	    protected QueueReceiver getQueueRequestReceiver;

	    /***/
	    protected TopicSession topicPublisherSession;

	    /***/
	    protected TopicPublisher topicPublisher;

	    /***/
	    protected TopicSubscriber topicSubscriber;

	    /***/
	    protected QueueSession getQueueSession;

	    /***/
	    protected AMQPCachePeer cachePeer;

	    /***/
	    protected boolean listenToTopic;


	    public AMQPCacheManagerPeerProvider(CacheManager cacheManager,
	                                       Channel channel) {


	        this.cacheManager = cacheManager;
	        this.channel = channel;
	    }


	    /**
	     * Time for a cluster to form. This varies considerably, depending on the implementation.
	     *
	     * @return the time in ms, for a cluster to form
	     */
	    public long getTimeForClusterToForm() {

	        if (LOG.isLoggable(Level.FINEST)) {
	            LOG.finest("getTimeForClusterToForm ( ) called ");
	        }

	        return 0;
	    }

	    /**
	     * The replication scheme. Each peer provider has a scheme name, which can be used by caches to specify
	     * for replication and bootstrap purposes.
	     *
	     * @return the well-known scheme name, which is determined by the replication provider author.
	     */
	    public String getScheme() {
	        return "JMS";
	    }

	    public void init() {


	        cachePeer = new AMQPCachePeer(cacheManager, channel);

	        remoteCachePeers.add(cachePeer);
	       /* try {
	            if (listenToTopic) {
	                topicSubscriber.setMessageListener(cachePeer);
	            }
	            getQueueRequestReceiver.setMessageListener(cachePeer);
	        } catch (JMSException e) {
	            LOG.log(Level.SEVERE, "Cannot register " + cachePeer + " as messageListener", e);
	        }
*/

	    }


	    /**
	     * Providers may be doing all sorts of exotic things and need to be able to clean up on dispose.
	     *
	     * @throws CacheException
	     */
	    public void dispose() throws CacheException {

	        LOG.fine("AMQPCacheManagerPeerProvider for CacheManager " + cacheManager.getName() + " being disposed.");

	        try {

	            cachePeer.dispose();

	            topicPublisher.close();
	           /* if (listenToTopic) {
	                topicSubscriber.close();
	                replicationTopicConnection.stop();
	            }
	            topicPublisherSession.close();
	            replicationTopicConnection.close();
*/
	            getQueueRequestReceiver.close();
	            getQueueSession.close();
	            getQueueConnection.close();

	        } catch (JMSException | IOException e) {
	            LOG.severe(e.getMessage());
	            throw new CacheException(e.getMessage(), e);
	        }


	    }


	    /**
	     * @return a list of {@link CachePeer} peers for the given cache, excluding the local peer.
	     */
	    public List<CachePeer> listRemoteCachePeers(Ehcache cache) throws CacheException {
	        return remoteCachePeers;
	    }


	    /**
	     * Register a new peer.
	     *
	     * @param rmiUrl
	     */
	    public void registerPeer(String rmiUrl) {
	        throw new CacheException("Not implemented for JMS");
	    }

	    /**
	     * Unregisters a peer.
	     *
	     * @param rmiUrl
	     */
	    public void unregisterPeer(String rmiUrl) {
	        throw new CacheException("Not implemented for JMS");
	    }



	
}
