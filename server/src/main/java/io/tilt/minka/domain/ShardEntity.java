/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package io.tilt.minka.domain;

import static io.tilt.minka.domain.ShardEntity.State.PREPARED;
import static java.util.Objects.requireNonNull;

import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import org.apache.commons.lang.Validate;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;

import io.tilt.minka.api.Duty;
import io.tilt.minka.api.Entity;
import io.tilt.minka.api.EntityPayload;
import io.tilt.minka.api.Pallet;

/**
 * Representation of a {@linkplain Duty} selected for an action in a {@linkplain Shard}  
 * 
 * @author Cristian Gonzalez
 * @since Nov 5, 2015
 */
public class ShardEntity implements Comparable<ShardEntity>, Comparator<ShardEntity>, EntityPayload {

	@JsonIgnore
	private final Logger logger = LoggerFactory.getLogger(getClass());
	private final static SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd.hhmmss");

	private static final long serialVersionUID = 4519763920222729635L;
	@JsonIgnore
	private final Entity<?> from;
	private final Type type;
	private EntityEvent event;
	private State state;
	@JsonIgnore
	private StuckCause stuckCause;
	private EntityPayload userPayload;
	
	private ShardEntity relatedEntity;
	
	public enum Type {
		DUTY, PALLET
	}
	
	public static class DateState implements Serializable {
        private static final long serialVersionUID = -3611519717574368897L;
        private final Date date;
	    private final State state;
        public DateState(final Date date, final State state) {
            super();
            this.date = date;
            this.state = state;
        }
        public Date getDate() {
            return this.date;
        }
        public State getState() {
            return this.state;
        }
	}

	@JsonIgnore
	private LinkedList<EventLog> eventLog;
		
	public static class EventLog implements Serializable {
        private static final long serialVersionUID = -8873965041941783628L;
        private final Date head;
	    private final EntityEvent event;
	    private final List<DateState> states;
	    private final String targetId;
	    private final long planId;
        public EventLog(final Date head, final EntityEvent event, final String targetId, final long planId) {
            super();
            this.head = head;
            this.event = event;
            this.targetId = targetId;
            this.planId = planId;
            this.states = new ArrayList<>(3);
        }
	    public Date getHead() {
            return this.head;
        }
	    public EntityEvent getEvent() {
            return this.event;
        }
	    public List<DateState> getStates() {
            return this.states;
        }
	    private void addState(final State state) {
	        this.states.add(new DateState(new Date(), state));
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
	
	@JsonProperty("event-log-size")
	public int eventSize() {
	    return this.eventLog == null ? 0 : this.eventLog.size();
	}
	
	@JsonProperty("event-log")
	public List<String> getStringEventLog() {
	    final List<String> tmp = new LinkedList<>();
	    for (EventLog el: getEventLog()) {
	        final String main = new StringBuilder(30)
                    .append("ev:").append(el.getEvent()).append(" ")
                    .append("sh:").append(el.getTargetId()).append(" ")
                    .append("p:").append(el.getPlanId()).append(" ")
                    .toString();
	        for (final DateState ds: el.getStates()) {
	            final StringBuilder sb= new StringBuilder(main)
    	            .append("dt:").append(sdf.format(ds.getDate())).append(" ")
                    .append("st:").append(ds.getState());
	            tmp.add(sb.toString());
	        }
	    }
	    return tmp;
	}
	
	
    public void addEvent(final EntityEvent event, final State state, final String targetId, final long id) {
        if (event != null) {
            this.event = event;
        }
        this.state = requireNonNull(state);
        EventLog log = eventLog.isEmpty() ? null : eventLog.getLast();
        if (log == null || (log.getEvent() != event && event != null)
                || (targetId != null && !log.getTargetId().equals(targetId))) {
            this.eventLog.add(log = new EventLog(new Date(), event, targetId, id));
        }
        log.addState(state);
    }

    public void addEvent(final State state) {
        addEvent(null, state, null, 0);
    }
	
	/**
	 * states while the duty travels along the wire and the action is confirmed
	 * because it takes time, and inconsistencies will happen
	 */
	public enum State {
		/* when created */
		PREPARED('p'),
		/* status at leader after being sent */
		PENDING('n'),
		/* status at followet when arrives */
		RECEIVED('r'),
		/* status at leader after the effect is confirmed */
		CONFIRMED('c'),
		/* status at leader when a follower falls, and at follower when absent in its delegate's report */
		DANGLING('d'),
		/* suddenly stop being reported from follower: no solution yet */
		MISSING('m'),
		/* status at a leader or follower when there's no viable solution for a duty */
		STUCK('s'),
		/* status at a follower when absent in delegate's report, only for lazy ones */
		FINALIZED('f')
		;
	    
	    private final char code;
	    State(final char c) {
	        this.code = c;
	    }
	    public State fromCode(final char code) {
	        for (State s: State.values()) {
	            if (s.code == code) {
	                return s;
	            }
	        }
	        throw new IllegalArgumentException("shardentity state code: " + code + " not exists");
	    }
	}

	public enum StuckCause {
		/* at follower: when Delegate does not release or take the duty */
		UNRELEASED, UNTAKEN,
		/*
		 * at leader: at distribution when duty is too big to fit in any
		 * available shard
		 */
		UNSUITABLE,
	}
	
	public boolean hasEverBeenDistributed() {
	    for (final EventLog log: eventLog) {
	        if (log.getEvent()==EntityEvent.ATTACH) {
	            for (final DateState ds: log.getStates()) {
	                if (ds.getState()==State.CONFIRMED) {
	                    return true;
	                }
	            }
	        }
	    }
	    return false;
	}
	
	private ShardEntity(final Entity<?> entity, Type type) {
		this.from = entity;
		this.event = EntityEvent.CREATE;
		this.type = type;
		this.state = PREPARED;
		this.eventLog = new LinkedList<>();
		addEvent(EntityEvent.CREATE, PREPARED, "n/a", -1);
	}
	
	public static class Builder {
		
		private EntityPayload userPayload;
		private EntityEvent event;
		private ShardEntity relatedEntity;
		private final Duty<?> duty;
		private final Pallet<?> pallet;
		private ShardEntity from;

		private Builder(final Entity<?> entity) {
			Validate.notNull(entity);
			if (entity instanceof Duty) {
				this.duty = (Duty<?>) entity;
				this.pallet = null;
			} else {
				this.duty = null;
				this.pallet = (Pallet<?>) entity;
			}
		}
		public Builder withRelatedEntity(final ShardEntity relatedEntity) {
			Validate.notNull(relatedEntity);
			this.relatedEntity = relatedEntity;
			return this;
			
		}
		public Builder withPayload(final EntityPayload userPayload) {
			Validate.notNull(userPayload);
			this.userPayload = userPayload;
			return this;
		}
		public Builder withEvent(final EntityEvent event) {
			Validate.notNull(event);
			this.event = event;
			return this;
		}
		public ShardEntity build() {
			if (from!=null) {
				final ShardEntity t = new ShardEntity(from.getEntity(), from.getType());
				t.setEventLog(from.getEventLog());
				t.setState(from.getState());
				t.setPartitionEvent(from.getDutyEvent());
				if (userPayload==null) {
					t.setUserPayload(from.getUserPayload());
				}
				if (relatedEntity==null) {
					t.setRelatedEntity(from.getRelatedEntity());
				}
				return t;
			} else {
				final ShardEntity ret = new ShardEntity(duty == null ? pallet : duty,
						duty == null ? Type.PALLET : Type.DUTY);
				ret.setUserPayload(userPayload);
				ret.setRelatedEntity(relatedEntity);
				return ret;
			}
		}
		public static Builder builderFrom(final ShardEntity entity) {
			final Builder ret = new Builder(entity.getEntity());
			ret.from = entity;
			return ret;
		}
		
		public static Builder builder(final Entity<?> entity) {
			return new Builder(entity);
		}
	}

	private void setRelatedEntity(final ShardEntity entity){
		this.relatedEntity = entity;
	}
	
	@JsonIgnore
	public ShardEntity getRelatedEntity() {
		return this.relatedEntity;
	}
	private void setPartitionEvent(EntityEvent dutyEvent) {
		this.event = dutyEvent;
	}

	private void setState(State state) {
		this.state = state;
	}

	@JsonIgnore
	public Pallet<?> getPallet() {
		if (this.from instanceof Pallet<?>) {
			return (Pallet<?>) this.from;
		}
		throw new IllegalArgumentException("This entity doesnt hold a Pallet !");
	}

	@JsonIgnore
	public Entity<?> getEntity() {
		return this.from;

	}

	@JsonProperty("id")
	private String getId_() {
		if (this.from instanceof Duty<?>) {
			return getDuty().getId();
		}
		return "[pallet]";
	}
	
	@JsonIgnore
	public Duty<?> getDuty() {
		if (this.from instanceof Duty<?>) {
			return (Duty<?>) this.from;
		}
		throw new IllegalArgumentException("This entity doesnt hold a Duty !");
	}

	public boolean is(EntityEvent e) {
		return this.event == e;
	}

	@JsonProperty("event")
	public EntityEvent getDutyEvent() {
		return this.event;
	}
	
	private void setUserPayload(final EntityPayload userPayload) {
		this.userPayload = userPayload;
	}

	@JsonIgnore
	public EntityPayload getUserPayload() {
		return this.userPayload;
	}

	public String toStringGroupByPallet(Set<ShardEntity> duties) {
		final StringBuilder sb = new StringBuilder(duties.size()*16);
		final Multimap<String, ShardEntity> mm = HashMultimap.create();
		duties.forEach(e -> mm.put(e.getDuty().getPalletId(), e));
		mm.asMap().forEach((k, v) -> {
			sb.append("p").append(k).append(" -> ").append(toStringBrief(v)).append(", ");
		});
		return sb.toString();
	}

	public static String toStringIds(Collection<ShardEntity> duties) {
		final StringBuilder sb = new StringBuilder(duties.size()*10);
		duties.forEach(i -> sb.append(i.getEntity().toString()).append(", "));
		return sb.toString();
	}

	public static String toStringBrief(Collection<ShardEntity> duties) {
		final StringBuilder sb = new StringBuilder(duties.size()*16);
		duties.forEach(i -> sb.append(i.toBrief()).append(", "));
		return sb.toString();
	}

	public static String toString(Collection<ShardEntity> duties) {
		final StringBuilder sb = new StringBuilder(duties.size()*10);
		duties.forEach(i -> sb.append(i.toString()).append(", "));
		return sb.toString();
	}

	@Override
	public String toString() {
		try {
			final String ttype = getEntity().getClassType().getSimpleName();
			final String id = getEntity().toString();

			StringBuilder sb = new StringBuilder(ttype.length() + id.length() + 30);

			if (type == Type.DUTY) {
				sb.append("p:").append(getDuty().getPalletId());
			}
			sb.append(type == Type.DUTY ? " d:" : "p:").append(id);
			if (type == Type.DUTY) {
				sb.append(" w:").append(((Duty<?>) getEntity()).getWeight());
			}
			sb.append(" ev:").append(getDutyEvent());
			sb.append(" s:").append(getState());

			if (!getEventLog().isEmpty()) {
			    sb.append(" c:").append(this.getEventLog().getFirst().getHead());
			}
			return sb.toString();
		} catch (Exception e) {
			logger.error("tostring", e);
		}
		return null;
	}

	public String toBrief() {
		final String load = type == Type.DUTY ? String.valueOf(this.getDuty().getWeight()) : "";
		final String pid = type == Type.DUTY ? getDuty().getPalletId() : getPallet().getId();
		final String id = getEntity().toString();
		final StringBuilder sb = new StringBuilder(10 + load.length() + id.length() + pid.length());
		sb.append("p:").append(pid).append(" ");
		if (type == Type.DUTY) {
			sb.append("d:").append(id);
			sb.append(" w:").append(load);
		}
		return sb.toString();
	}

	@Override
	public int compareTo(ShardEntity o) {
		return this.getEntity().getId().compareTo(o.getEntity().getId());
	}

	@JsonProperty("state")
	public State getState() {
		return this.state;
	}

	public LinkedList<EventLog> getEventLog() {
        return this.eventLog;
    }
	private void setEventLog(final LinkedList<EventLog> log) {
	    this.eventLog = log;
	}
	
	public int hashCode() {
		return new HashCodeBuilder().append(getEntity().getId()).toHashCode();
	}

	@Override
	public boolean equals(Object obj) {
		if (obj != null && obj instanceof ShardEntity && getEntity() != null) {
			ShardEntity st = (ShardEntity) obj;
			return getEntity().getId().equals(st.getEntity().getId());
		} else {
			return false;
		}
	}

	@Override
	public int compare(ShardEntity o1, ShardEntity o2) {
		return o1.compareTo(o2);
	}

	public StuckCause getStuckCause() {
		return this.stuckCause;
	}

	public Type getType() {
		return this.type;
	}

	
	public static class WeightComparer implements Comparator<ShardEntity>, Serializable {
		private static final long serialVersionUID = 2191475545082914908L;
		@Override
		public int compare(final ShardEntity o1, final ShardEntity o2) {
			int ret = Double.compare(o1.getDuty().getWeight(), o2.getDuty().getWeight());
			// break comparator contract about same weight same entity yeah rightttttt
			if (ret == 0) {
				return altCompare(o1, o2);
			} 
			return ret;
		}
	}
	
	public static class CreationComparer implements Comparator<ShardEntity>, Serializable {
		private static final long serialVersionUID = 3709876521530551544L;
		@Override
		public int compare(final ShardEntity o1, final ShardEntity o2) {
			int i = o1.getEventLog().getFirst().getHead()
					.compareTo(o2.getEventLog().getFirst().getHead());
			if (i == 0) {
				i = altCompare(o1, o2);
			}
			return i;
		}
	}
	protected static int altCompare(final ShardEntity o1, final ShardEntity o2) {
		int i = o1.compare(o1, o2);
		if (i == 0) {
			i = Integer.compare(o1.hashCode(), o2.hashCode());
		}
		return i;
	}

}
