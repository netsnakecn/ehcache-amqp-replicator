package com.maicard.misc.ehcache.amqp;

import net.sf.ehcache.distribution.LegacyEventMessage;

public enum Action {


	    /**
	     *
	     */
	    PUT(LegacyEventMessage.PUT),

	    /**
	     *
	     */
	    REMOVE(LegacyEventMessage.REMOVE),

	    /**
	     *
	     */
	    REMOVE_ALL(LegacyEventMessage.REMOVE_ALL),

	    /**
	     *
	     */
	    GET(10);

	    private int action;

	    /**
	     * @param mode
	     */
	    Action(int mode) {
	        this.action = mode;
	    }

	    /**
	     * @param value
	     * @return The action enum corresponsing to the string value
	     */
	    public static Action forString(String value) {
	        for (Action action : values()) {
	            if (action.name().equals(value)) {
	                return action;
	            }
	        }
	        return null;
	    }


	    /**
	     * @return an int value for the action. The same int values as EventMessage are used
	     */
	    public int toInt() {
	        return action;
	    }
	
}
