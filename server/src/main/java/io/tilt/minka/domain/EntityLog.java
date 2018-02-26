/**
 *  Copyright (c) 2011-2015 Zauber S.A.  -- All rights reserved
 */

package io.tilt.minka.domain;

import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.Collections;
import java.util.Date;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

import io.tilt.minka.domain.EntityLog.Log.TimeState;

public class EntityLog implements Serializable {
	
	protected static final long serialVersionUID = 1L;

	final static SimpleDateFormat sdf = new SimpleDateFormat("yyMMdd:hhmmss.SSS");

	@JsonIgnore
	private final LinkedList<Log> logs = new LinkedList<>();

	
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
	public List<Log> getLogs() {
		return Collections.unmodifiableList(logs);
	}
	
	public List<Log> getLogs(final long planId) {
        return logs.stream()
        	.filter(et->et.getPlanId()==planId)
        	.collect(Collectors.toList());
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
    /** add a state to the last event */
    public void addState(final EntityState state) {
        addEvent(null, state, null, 0);
    }
    
    /*
    	deliveries are organized so events occurr in this order:
    	
    	event		state		shard	plan id
    	log1:    	
    	DETTACH 	PREPARED	9000	 1
    	DETTACH 	PENDING		9000	 1
    	DETTACH 	CONFIRMED	9000	 1
    	log2:
    	ATTACH 		PREPARED	9001	 1    	
    	ATTACH 		PENDING		9001	 1    	
    	ATACH 		CONFIRMED	9001	 1
    	
    */ 
	public void addEvent(
	        final EntityEvent event, 
	        final EntityState state, 
	        final ShardIdentifier shardid,
	        final long planid) {
		
        Log log = this.logs.isEmpty() ? null : this.logs.getLast();
        final String shid = shardid==null ? "n/a" : shardid.getStringIdentity();
		if (log == null || (log.getEvent() != event && event != null)
                || (shid != null && !log.getTargetId().equals(shid))) {
            this.logs.add(log = new Log(new Date(), event, shid, planid));
        }
        log.addState(state);
    }
	
	public Iterator<Log> getDescendingIterator() {
		return this.logs.descendingIterator();
	}
	
	@JsonProperty("log")
	public List<String> getStringHistory() {
		final List<String> tmp = new LinkedList<>();
	    for (Log el: logs) {
	        final String main = new StringBuilder(30)
                    .append(el.getTargetId()).append(" / ")
                    .append(el.getPlanId()).append(" / ")
                    .toString();
	        for (final TimeState ds: el.getStates()) {
	            final StringBuilder sb= new StringBuilder(main)
    	            .append(sdf.format(ds.getDate())).append(" / ")
                    .append(el.getEvent()).append(" / ")
                    .append(ds.getState());
	            tmp.add(sb.toString());
	        }
	    }
	    return tmp;
	}
	
	public static class Log implements Serializable {

	    private static final long serialVersionUID = -8873965041941783628L;
	    
	    private final Date head;
	    private final EntityEvent event;
	    private final LinkedList<TimeState> states;
	    private final String targetId;
	    private final long planId;
	    
	    private Log(final Date head, final EntityEvent event, final String targetId, final long planId) {
	        super();
	        this.head = head;
	        this.event = event;
	        this.targetId = targetId;
	        this.planId = planId;
	        this.states = new LinkedList<>();
	    }
	    public Date getHead() {
	        return this.head;
	    }
	    public EntityEvent getEvent() {
	        return this.event;
	    }
	    
	    public List<TimeState> getStates() {
	        return Collections.unmodifiableList(states);
	    }
	        
	    public EntityState getLastState() {
	    	return this.states.getLast().getState();
	    }
	    
	    public boolean matches(final Log log) {
	    	return log.getEvent()== this.event
	    			&& log.getTargetId().equals(this.targetId)
	    			&& log.getPlanId() == this.planId;
	    }
	    
	    public boolean matches(final EntityEvent event, final String targetId, final int planId) {
	    	return event == this.event
	    			&& targetId.equals(this.targetId)
	    			&& planId == this.planId;
	    }
	    
	    public void addState(final EntityState state) {
	        this.states.add(new TimeState(new Date(), state));
	    }
	    public String getTargetId() {
	        return this.targetId;
	    }
	    public long getPlanId() {
	        return this.planId;
	    }
	    @Override
	    public String toString() {
	        final StringBuilder sb = new StringBuilder(20)
	        		.append("ev:").append(event).append(" ")
	                .append("dt:").append(sdf.format(head)).append(" ")
	                
	                .append("sh:").append(targetId).append(" ")
	                .append("p:").append(planId);	        
	        return sb.toString();
	    }

		@Override
		public int hashCode() {
			return hasher(head, targetId, planId, event);
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

		public static int hasher(final Object...objs) {
			final int prime = 31;
			int res = 1;
			for (final Object o: objs) {
				res *= prime + ((o == null ) ? 0 : o.hashCode());
			}
			return res;
		}
	    
	    
	    public static class TimeState implements Serializable {
	        private static final long serialVersionUID = -3611519717574368897L;
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
				return hasher(date, state);
			}
	        
	        @Override
	        public boolean equals(final Object obj) {
	        	if (obj==null || !(obj instanceof TimeState)) {
	        		return false;
	        	} else if (obj==this) {
	        		return true;
	        	} else {
	        		final TimeState ts = (TimeState)obj;
	        		return ts.getDate().equals(date)
	        				&& ts.getState().equals(state);
	        	}
	        }
	    }

	}
	
	

}