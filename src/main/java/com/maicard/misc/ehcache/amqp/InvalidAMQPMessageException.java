package com.maicard.misc.ehcache.amqp;

import net.sf.ehcache.CacheException;

public class InvalidAMQPMessageException extends CacheException {

	    /**
	 * 
	 */
	private static final long serialVersionUID = -8214769891505243296L;


		/**
	     * Constructor for the InvalidJMSMessageException object.
	     */
	    public InvalidAMQPMessageException() {
	        super();
	    }

	    /**
	     * Constructor for the InvalidJMSMessageException object.
	     *
	     * @param message the exception detail message
	     */
	    public InvalidAMQPMessageException(String message) {
	        super(message);
	    }

	    /**
	     * Constructs a new InvalidJMSMessageException with the specified detail message and
	     * cause.  <p>Note that the detail message associated with
	     * <code>cause</code> is <i>not</i> automatically incorporated in
	     * this runtime exception's detail message.
	     *
	     * @param message the detail message (which is saved for later retrieval
	     *                by the {@link #getMessage()} method).
	     * @param cause   the cause (which is saved for later retrieval by the
	     *                {@link #getCause()} method).  (A <tt>null</tt> value is
	     *                permitted, and indicates that the cause is nonexistent or
	     *                unknown.)
	     */
	    public InvalidAMQPMessageException(String message, Throwable cause) {
	        super(message, cause);
	    }

	   
	    public InvalidAMQPMessageException(Throwable cause) {
	        super(cause);
	    }
	}

