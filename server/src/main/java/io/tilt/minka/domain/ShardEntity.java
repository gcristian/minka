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

import java.io.Serializable;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang.Validate;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
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

	private final Logger logger = LoggerFactory.getLogger(getClass());

	private static final long serialVersionUID = 4519763920222729635L;

	private final Entity<?> from;
	private final Type type;
	private EntityEvent event;
	private State state;
	private StuckCause stuckCause;
	private EntityPayload userPayload;

	private Map<EntityEvent, DateTime> partitionDates;
	private Map<State, DateTime> stateDates;
	private ShardEntity relatedEntity;
	
	public enum Type {
		DUTY, PALLET
	}

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
		/* status at leader when a follower falls, and at follower when absent in its delegate's report */
		DANGLING,
		/* suddenly stop being reported from follower: no solution yet */
		MISSING,
		/* status at a leader or follower when there's no viable solution for a duty */
		STUCK,
		/* status at a follower when absent in delegate's report, only for lazy ones */
		FINALIZED,
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
	
	private ShardEntity(final Entity<?> entity, Type type) {
		this.from = entity;
		this.event = EntityEvent.CREATE;
		this.type = type;
		this.state = PREPARED;
		this.partitionDates = new HashMap<>();
		this.stateDates = new HashMap<>();
		final DateTime now = new DateTime(DateTimeZone.UTC);
		this.partitionDates.put(event, now);
		this.stateDates.put(state, now);
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
				t.setStateDates(from.getStateDates());
				t.setPartitionDates(from.getPartitionDates());
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
				if (event!=null) {
					ret.registerEvent(event, PREPARED);
				}
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
	private Map<EntityEvent, DateTime> getPartitionDates() {
		return this.partitionDates;
	}

	private void setPartitionDates(Map<EntityEvent, DateTime> partitionDates) {
		this.partitionDates = partitionDates;
	}

	@JsonIgnore
	private Map<State, DateTime> getStateDates() {
		return this.stateDates;
	}

	private void setStateDates(Map<State, DateTime> stateDates) {
		this.stateDates = stateDates;
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

	public void registerEvent(final EntityEvent event, final State state) {
		this.state = state;
		this.event = event;

		partitionDates.put(event, new DateTime());
		stateDates.put(state, new DateTime());
	}

	public void registerEvent(final State state) {
		this.state = state;
		stateDates.put(state, new DateTime());
	}

	public DateTime getEventDateForState(final State state) {
		return stateDates.get(state);
	}

	public DateTime getEventDateForPartition(final EntityEvent event) {
		return partitionDates.get(event);
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
		final StringBuilder sb = new StringBuilder();
		final Multimap<String, ShardEntity> mm = HashMultimap.create();
		duties.forEach(e -> mm.put(e.getDuty().getPalletId(), e));
		mm.asMap().forEach((k, v) -> {
			sb.append("p").append(k).append(" -> ").append(toStringBrief(v)).append(", ");
		});
		return sb.toString();
	}

	public static String toStringIds(Collection<ShardEntity> duties) {
		final StringBuilder sb = new StringBuilder();
		duties.forEach(i -> sb.append(i.getEntity().toString()).append(", "));
		return sb.toString();
	}

	public static String toStringBrief(Collection<ShardEntity> duties) {
		final StringBuilder sb = new StringBuilder();
		duties.forEach(i -> sb.append(i.toBrief()).append(", "));
		return sb.toString();
	}

	public static String toString(Collection<ShardEntity> duties) {
		final StringBuilder sb = new StringBuilder();
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

			sb.append(" c:").append(this.getEventDateForPartition(EntityEvent.CREATE));
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
			int i = o1.getEventDateForPartition(EntityEvent.CREATE)
					.compareTo(o2.getEventDateForPartition(EntityEvent.CREATE));
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
