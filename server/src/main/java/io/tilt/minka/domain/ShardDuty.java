/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.tilt.minka.domain;

import static io.tilt.minka.domain.ShardDuty.State.PREPARED;

import java.io.Serializable;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang.builder.HashCodeBuilder;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import io.tilt.minka.api.Duty;

/**
 * Representation of a {@linkplain Duty} selected for an action in a {@linkplain Shard}  
 * 
 * @author Cristian Gonzalez
 * @since Nov 5, 2015
 */
public class ShardDuty implements Comparable<ShardDuty>, Comparator<ShardDuty>, Serializable {

    private static final long serialVersionUID = 4519763920222729635L;

    private final Duty<?> duty;
    
	private DutyEvent dutyEvent;
	private State state;
	private StuckCause stuckCause;
	private Serializable userPayload;
	
	private Map<DutyEvent, DateTime> partitionDates;
	private Map<State, DateTime> stateDates;
	private boolean checkForChange;
	
	/**
     * states while the duty travels along the wire and the action is confirmed
     * because it takes time, and inconsistencies will happen
     */
    public enum State {
        /* when created */
        PREPARED,
        /* status at leader after being sent */
        SENT,
        /* status at followet when arrives */
        RECEIVED,
        /* status at leader after the effect is confirmed */
        CONFIRMED,
        /* status at leader when a follower falls, and at follower when lack of its registry presence */
        DANGLING,
        /* suddenly stop being reported from follower: no solution yet */
        MISSING,
        /* status at a leader or follower when there's no viable solution for a duty */
        STUCK
    }
    
    public enum StuckCause {
        /* at follower: when Delegate does not release or take the duty */
        UNRELEASED,
        UNTAKEN,
        /* at leader: at distribution when duty is too big to fit in any available shard */ 
        UNSUITABLE,
    }
    
    public static ShardDuty copy(final ShardDuty duty) {
        ShardDuty t = new ShardDuty(duty.getDuty());
        t.setStateDates(duty.getStateDates());
        t.setPartitionDates(duty.getPartitionDates());
        t.setUserPayload(duty.getUserPayload());
        t.setState(duty.getState());
        t.setPartitionEvent(duty.getDutyEvent());
        return t;
    }
    
    private void setPartitionEvent(DutyEvent dutyEvent) {
        this.dutyEvent = dutyEvent;
    }

    private void setState(State state) {
        this.state = state;
    }

    private Map<DutyEvent, DateTime> getPartitionDates() {
        return this.partitionDates;
    }

    private void setPartitionDates(Map<DutyEvent, DateTime> partitionDates) {
        this.partitionDates = partitionDates;
    }

    private Map<State, DateTime> getStateDates() {
        return this.stateDates;
    }

    private void setStateDates(Map<State, DateTime> stateDates) {
        this.stateDates = stateDates;
    }

    public static ShardDuty create(final Duty<?> duty) {
        return new ShardDuty(duty);
    }

	/* reserved for the cluster */
	private ShardDuty(final Duty<?> duty) {
	    this.duty = duty;
		dutyEvent = DutyEvent.CREATE;
		state = PREPARED;
		partitionDates = new HashMap<>();
		stateDates = new HashMap<>();		
		final DateTime now = new DateTime(DateTimeZone.UTC);
		partitionDates.put(dutyEvent, now);
		stateDates.put(state, now);
	}
	
	public Duty<?> getDuty() {
        return this.duty;
    }
    
	public void registerEvent(final DutyEvent event, final State state) {
	    this.state = state;
        this.dutyEvent = event;
	    
        partitionDates.put(dutyEvent, new DateTime());
        stateDates.put(state, new DateTime());
	}
    
    public void registerEvent(final State state) {
        this.state = state;
        stateDates.put(state, new DateTime());
    }
    
    public DateTime getEventDateForState(final State state) {
        return stateDates.get(state);
    }
    
    public DateTime getEventDateForPartition(final DutyEvent event) {
        return partitionDates.get(event);
    }

    public boolean is(DutyEvent e) {
        return this.dutyEvent == e;
    }
    
	public DutyEvent getDutyEvent() {
		return this.dutyEvent;
	}
	
	public void setUserPayload(final Serializable userPayload) {
	    this.userPayload = userPayload;
	}

	public Serializable getUserPayload() {
        return this.userPayload;
    }
	
	public static String toStringIds(Collection<ShardDuty> duties) {
	    final StringBuilder sb = new StringBuilder();
	    duties.forEach(i->sb.append(i.getDuty().getId()).append(", "));
	    return sb.toString();
	}
	
	public static String toStringBrief(Collection<ShardDuty> duties) {
        final StringBuilder sb = new StringBuilder();
        duties.forEach(i->sb.append(i.toBrief()).append(", "));
        return sb.toString();
    }
	
	public static String toString(Collection<ShardDuty> duties) {
        final StringBuilder sb = new StringBuilder();
        duties.forEach(i->sb.append(i.toString()).append(", "));
        return sb.toString();
    }
	
	@Override
	public String toString() {
	    return new StringBuilder()
            .append("Duty: ").append(getDuty().getId())
            .append(" (").append(getDuty().getClassType().getSimpleName())
            .append(": ").append(getDutyEvent())
            .append(", ").append(getState())
            .append(", w:").append(getDuty().getWeight().getLoad())
            .append(")")
	        .toString();
	}

    @Override
	public int compareTo(ShardDuty o) {		
		return this.getDuty().getId().compareTo(o.getDuty().getId());
	}


    public State getState() {
        return this.state;
    }

	public String toBrief() {
        return new StringBuilder()
                .append("Duty ID: ").append(getDuty().getId())
                .append(" Type: ").append(getDuty().getClassType().getSimpleName())
                .toString();
	}
    
    public int hashCode() {
        return new HashCodeBuilder()
            .append(getDuty().getId())
            .toHashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj !=null && obj instanceof ShardDuty && getDuty()!=null) {
            ShardDuty st = (ShardDuty)obj;
            return getDuty().getId().equals(st.getDuty().getId());
        } else {
            return false;
        }
    }

    @Override
    public int compare(ShardDuty o1, ShardDuty o2) {
        return o1.compareTo(o2);
    }

    public StuckCause getStuckCause() {
        return this.stuckCause;
    }

    public void setStuckCause(StuckCause stuckCause) {
        this.stuckCause = stuckCause;
    }
    
}
