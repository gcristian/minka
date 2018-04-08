/**
 *  Copyright (c) 2011-2015 Zauber S.A.  -- All rights reserved
 */

package io.tilt.minka.domain;


import static java.util.Collections.unmodifiableList;

import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collectors;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

import io.tilt.minka.domain.EntityJournal.Log.TimeState;

/**
 * 
 * A list of {@linkplain Log} contained in a {@linkplain ShardEntity}
 *  
 * Deliveries are organized so events occurr in this order:
 * 
 * 	 event		state		shard	plan id
 * 	 =====		=========	=====	=======
 * 
 * log1:  
 *   DETTACH 	PREPARED	9000	 1
 *   DETTACH 	PENDING		9000	 1
 *   DETTACH 	CONFIRMED	9000	 1
 *   
 * log2:
 * 	 ATTACH 	PREPARED	9001	 1
 *   ATTACH 	PENDING		9001	 1
 *   ATACH 		CONFIRMED	9001	 1
 *  
 *  @author Cristian Gonzalez
 *  @since  Oct 15, 2018
*/
public class EntityJournal implements Serializable {
	
	protected static final long serialVersionUID = 1L;
	private static int MAX_JOURNAL_SIZE = 50;
	
	final static SimpleDateFormat sdf = new SimpleDateFormat("yyMMdd:hhmmss.SSS");

	private boolean sliding;
	

	@JsonIgnore
	private final LinkedList<Log> logs = new LinkedList<>();

	public final static String NOT_APPLIABLE = "N/A";
	
	/**
	 * Add a new state to an existing key: event + shard + plan.
	 * Create a new Log if the key not exists already.
	 * 
	 * @param event		create, attach, dettach, remove
	 * @param state		pending, confirmed
	 * @param shardid	shard1, shard2
	 * @param planid	version of plan
	 */
	public void addEvent(
	        final EntityEvent event, 
	        final EntityState state, 
	        final ShardIdentifier shardid,
	        final long planid) {		
        
        // look up the right log 
		final String shid = shardid!=null ? shardid.getId() : null;
		Log log = (planid > 0) ? find_(planid, shid, event) : null;
		if (log==null) {
            logs.add(log = new Log(new Date(), event, shid == null ? NOT_APPLIABLE : shid, planid));
            if (sliding || (sliding= logs.size()== MAX_JOURNAL_SIZE)) {
            	logs.removeFirst();
            }
        }
        log.addState(state);
    }
	
	
	@JsonProperty("log-size")
	public int eventSize() {
	    return this.logs == null ? 0 : this.logs.size();
	}
	
	public Log getLast() {
		return this.logs.getLast();
	}
	
	public Log getFirst() {
		return this.logs.getFirst();
	}
	
	@JsonIgnore
	/** @return an unmodifiable list */
	public List<Log> getLogs() {
		return unmodifiableList(logs);
	}

	public Iterator<Log> descendingIterator() {
		return logs.descendingIterator();
	}

	public List<Log> getLogs(final long planId) {
        return unmodifiableList(logs.stream()
        	.filter(et->et.getPlanId()==planId)
        	.collect(Collectors.toList()));
    }

	public boolean hasEverBeenDistributed() {
	    for (final Log log: logs) {
	        if (log.getEvent()==EntityEvent.ATTACH) {
	            for (final TimeState ds: log.getStates()) {
	                if (ds.getState()==EntityState.CONFIRMED) {
	                    return true;
	                }
	            }
	        }
	    }
	    return false;
	}

	/** 
	 * @return reading from latest to earliest a Log that matches given:
	 * plan version, target shard and specific events, or any if null
	 **/
	public Log find(final long planid, final ShardIdentifier shardid, final EntityEvent...events) {
		return find_(planid, shardid.getId(), events);
	}
	
	/** 
	 * @return reading from latest to earliest limited to the last plan
	 * a Log that matches a target shard 
	 **/
	public Log find(final ShardIdentifier shardid) {
		return find_(0, shardid.getId(), null);
	}
	
	/** 
	 * Descends in time (logs are stacked) until an older plan than current is reached. 
	 * @return a Log matching plan + event + shard, whereas those args. specified or any if omitted 
	 */
	private Log find_(final long planid, final String shardid, final EntityEvent...events) {
		Log ret = null;
		boolean onePlanFound = false;
		for (final Iterator<Log> it = logs.descendingIterator(); it.hasNext();) {
            final Log log = it.next();
            if ((planid == 0 || log.getPlanId() == planid) 
            		&& (shardid == null || log.getTargetId().equals(shardid))) {
            	onePlanFound = true;
            	if (events==null) {
            		return log;
            	} else {
	            	for (EntityEvent ee: events) {
	            		if (log.getEvent() == ee) {
	            			return log;
	            		}
	            	}
            	}
            	break;
            } else if (planid > log.getPlanId()) {
            	// avoid phantom events
            	break;
            } else if (planid ==0 && onePlanFound) {
            	break;
            }
		}
		return ret;
	}

	@JsonProperty("log")
	public List<String> getStringHistory() {
		final Map<TimeState, String> ordered = new TreeMap<>(new TimeState.DateComparer());
	    for (Log el: logs) {	    	
	        for (final TimeState ts: el.getStates()) {
	        	ordered.put(ts, String.format("%s %s: %s (%s) at %s", 
	        			el.getPlanId(), 
	        			el.getEvent(), 
	        			el.getTargetId(), 
	        			ts.getState(), 
	        			sdf.format(ts.getDate()))); 
	        }
	    }
	    return new ArrayList<>(ordered.values());
	}

	/**
	 * a list of States {Prepared, Pending...Confirmed} 
	 * for an EntityEvent {Create, Attach..Dettach}, a Shard and a Plan.
	 */
	public static class Log implements Serializable {

	    private static final long serialVersionUID = -8873965041941783628L;
	    
	    private final Date head;
	    private final EntityEvent event;
	    private final LinkedList<TimeState> states = new LinkedList<>();
	    private final String targetId;
	    private final long planId;
	    
	    private Log(final Date head, final EntityEvent event, final String targetId, final long planId) {
	        super();
	        this.head = head;
	        this.event = event;
	        this.targetId = targetId;
	        this.planId = planId;
	    }
	    public Date getHead() {
	        return this.head;
	    }
	    public EntityEvent getEvent() {
	        return this.event;
	    }
	    public String getTargetId() {
	        return this.targetId;
	    }
	    public long getPlanId() {
	        return this.planId;
	    }
	    
	    // ---------------------------------------------------------------------------------------------------
	    
	    public List<TimeState> getStates() {
	        return Collections.unmodifiableList(states);
	    }
	        
	    public EntityState getLastState() {
	    	return this.states.getLast().getState();
	    }
	    
	    public boolean matches(final Log log) {
	    	return matches(log.getEvent(), log.getTargetId(), log.getPlanId());
	    }
	    
	    /** @return whether passed log belongs to the same shard, version plan and type of event */	    
	    public boolean matches(final EntityEvent event, final String targetId, final long planId) {
	    	return event == this.event
	    			&& targetId.equals(this.targetId)
	    			&& planId == this.planId;
	    }
	    
	    public void addState(final EntityState state) {
	        this.states.add(new TimeState(new Date(), state));
	    }
	    
	    @Override
	    public String toString() {
	        final StringBuilder sb = new StringBuilder(15+3+13+15+25)
	        		.append("p:").append(planId).append(' ')
	        		.append("dt:").append(sdf.format(head)).append(' ')
	        		.append("ev:").append(event).append(' ')
	                .append("sh:").append(targetId).append(' ')
	                
	                ;	        
	        return sb.toString();
	    }

		@Override
		public int hashCode() {
			final int prime = 31;
			int res = 1;
			res *= prime + ((head == null ) ? 0 : head.hashCode());
			res *= prime + ((targetId == null ) ? 0 : targetId.hashCode());
			res *= prime + planId;
			res *= prime + ((event == null ) ? 0 : event.hashCode());			
			return res;
		}
		@Override
		public boolean equals(final Object obj) {
			if (obj != null && obj!=this && obj instanceof Log) {
				return false;
			} else if (obj == this) {
				return true;
			} else {
				final Log o = (Log)obj;
				return head.equals(o.getHead())
						&& targetId.equals(o.getTargetId())
						&& planId==o.getPlanId()
						&& event==o.getEvent();
			}
		}
	    
		/**
		 * A recording timestamp for a reached state 
		 */
	    public static class TimeState implements Serializable {
	        private static final long serialVersionUID = -3611519717574368897L;
	        
	    	public static class DateComparer implements Comparator<TimeState>, Serializable {
	    		private static final long serialVersionUID = 3709876521530551544L;
	    		@Override
	    		public int compare(final TimeState o1, final TimeState o2) {
	    			if (o1==null || o2 == null) {
	    				return o1==null && o2!=null ? -1 : o2==null ? 0 : 1;
	    			} else {
	    				return o1.getDate().compareTo(o2.getDate());
	    			}
	    		}
	    	}

	        private final Date date;
	        private final EntityState state;

	        public TimeState(final Date date, final EntityState state) {
	            super();
	            this.date = date;
	            this.state = state;
	        }
	        public Date getDate() {
	            return this.date;
	        }
	        public EntityState getState() {
	            return this.state;
	        }
	        
			@Override
			public int hashCode() {
				final int prime = 31;
				int res = 1;
				res *= prime + ((date == null ) ? 0 : date.hashCode());
				res *= prime + ((state== null ) ? 0 : state.hashCode());
				return res;
			}
	        @Override
	        public String toString() {
	        	return new StringBuilder(8+17+10)
	        			.append("dt:").append(date).append(' ')
	        			.append("st:").append(state)
	        			.toString();
	        }
	        @Override
	        public boolean equals(final Object obj) {
	        	if (obj==null || !(obj instanceof TimeState)) {
	        		return false;
	        	} else if (obj==this) {
	        		return true;
	        	} else {
	        		final TimeState ts = (TimeState)obj;
	        		return ts.getDate().equals(date) && ts.getState().equals(state);
	        	}
	        }
	    }

	}
	
	

}