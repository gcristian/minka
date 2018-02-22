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

public class EventTrack implements Serializable {
	
	protected static final long serialVersionUID = 1L;

	final static SimpleDateFormat sdf = new SimpleDateFormat("yyMMdd:hhmmss.SSS");

	@JsonIgnore
	private final LinkedList<Track> tracks = new LinkedList<>();

	
	@JsonProperty("event-track-size")
	public int eventSize() {
	    return this.tracks == null ? 0 : this.tracks.size();
	}
	
	public Track getLast() {
		return this.tracks.getLast();
	}
	
	public Track getFirst() {
		return this.tracks.getFirst();
	}
	
	@JsonIgnore
	public List<Track> getTracks() {
		return Collections.unmodifiableList(tracks);
	}
	
	public List<Track> getEventTrack(final long planId) {
        return tracks.stream()
        	.filter(et->et.getPlanId()==planId)
        	.collect(Collectors.toList());
    }

	public boolean hasEverBeenDistributed() {
	    for (final Track track: tracks) {
	        if (track.getEvent()==EntityEvent.ATTACH) {
	            for (final TimeState ds: track.getStates()) {
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

	public void addEvent(
	        final EntityEvent event, 
	        final EntityState state, 
	        final String targetId, 
	        final long id) {
        Track track = this.tracks.isEmpty() ? null : this.tracks.getLast();
        if (track == null || (track.getEvent() != event && event != null)
                || (targetId != null && !track.getTargetId().equals(targetId))) {
            this.tracks.add(track = new Track(new Date(), event, targetId, id));
        }
        track.addState(state);
    }
	
	public Iterator<Track> getDescendingIterator() {
		return this.tracks.descendingIterator();
	}
	
	@JsonProperty("event-track")
	public List<String> getStringHistory() {
		final List<String> tmp = new LinkedList<>();
	    for (Track el: tracks) {
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
	
	public static class Track implements Serializable {

	    private static final long serialVersionUID = -8873965041941783628L;
	    
	    private final Date head;
	    private final EntityEvent event;
	    private final LinkedList<TimeState> states;
	    private final String targetId;
	    private final long planId;
	    
	    private Track(final Date head, final EntityEvent event, final String targetId, final long planId) {
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
	                .append("dt:").append(sdf.format(head)).append(" ")
	                .append("ev:").append(event).append(" ")
	                .append("sh:").append(targetId).append(" ")
	                .append("p:").append(planId);	        
	        return sb.toString();
	    }
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
	}
	

}