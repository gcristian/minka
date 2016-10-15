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

import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang.builder.HashCodeBuilder;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

	private final Entity<?> entity;
	private final Type type;
	private EntityEvent event;
	private State state;
	private StuckCause stuckCause;
	private EntityPayload userPayload;

	private Map<EntityEvent, DateTime> partitionDates;
	private Map<State, DateTime> stateDates;
	private boolean checkForChange;
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
		/* status at leader when a follower falls, and at follower when lack of
		 * its registry presence
		 */
		DANGLING,
		/* suddenly stop being reported from follower: no solution yet */
		MISSING,
		/* status at a leader or follower when there's no viable solution for a duty */
		STUCK
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

	public static ShardEntity copy(final ShardEntity entity) {
		ShardEntity t = new ShardEntity(entity.getEntity(), entity.getType());
		t.setStateDates(entity.getStateDates());
		t.setPartitionDates(entity.getPartitionDates());
		t.setUserPayload(entity.getUserPayload());
		t.setState(entity.getState());
		t.setPartitionEvent(entity.getDutyEvent());
		t.setRelatedEntity(entity.getRelatedEntity());
		return t;
	}

	public void setRelatedEntity(final ShardEntity entity){
		this.relatedEntity = entity;
	}
	
	public ShardEntity getRelatedEntity() {
		return this.relatedEntity;
	}
	private void setPartitionEvent(EntityEvent dutyEvent) {
		this.event = dutyEvent;
	}

	private void setState(State state) {
		this.state = state;
	}

	private Map<EntityEvent, DateTime> getPartitionDates() {
		return this.partitionDates;
	}

	private void setPartitionDates(Map<EntityEvent, DateTime> partitionDates) {
		this.partitionDates = partitionDates;
	}

	private Map<State, DateTime> getStateDates() {
		return this.stateDates;
	}

	private void setStateDates(Map<State, DateTime> stateDates) {
		this.stateDates = stateDates;
	}

	public static ShardEntity create(final Duty<?> duty) {
		return new ShardEntity(duty, Type.DUTY);
	}

	public static ShardEntity create(final Pallet<?> pallet) {
		return new ShardEntity(pallet, Type.PALLET);
	}

	/* reserved for the cluster */
	private ShardEntity(final Entity<?> entity, Type type) {
		this.entity = entity;
		this.event = EntityEvent.CREATE;
		this.type = type;
		this.state = PREPARED;
		this.partitionDates = new HashMap<>();
		this.stateDates = new HashMap<>();
		final DateTime now = new DateTime(DateTimeZone.UTC);
		this.partitionDates.put(event, now);
		this.stateDates.put(state, now);
	}

	public Pallet<?> getPallet() {
		if (this.entity instanceof Pallet<?>) {
			return (Pallet<?>) this.entity;
		}
		throw new IllegalArgumentException("This entity doesnt hold a Pallet !");
	}

	public Entity<?> getEntity() {
		return this.entity;

	}

	public Duty<?> getDuty() {
		if (this.entity instanceof Duty<?>) {
			return (Duty<?>) this.entity;
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

	public EntityEvent getDutyEvent() {
		return this.event;
	}

	public void setUserPayload(final EntityPayload userPayload) {
		this.userPayload = userPayload;
	}

	public EntityPayload getUserPayload() {
		return this.userPayload;
	}

	public String toStringGroupByPallet(Set<ShardEntity> duties) {
		final StringBuilder sb = new StringBuilder();
		final Multimap<String, ShardEntity> mm = HashMultimap.create();
		duties.forEach(e -> mm.put(e.getDuty().getPallet().getId(), e));
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

			sb.append(" t:").append(type);
			return sb.toString();
		} catch (Exception e) {
			logger.error("tostring", e);
		}
		return null;
	}

	public String toBrief() {
		final String load = String.valueOf(this.getDuty().getWeight());
		final String pid = getDuty().getPalletId();
		final String id = getEntity().toString();
		final StringBuilder sb = new StringBuilder(10 + load.length() + id.length() + pid.length());
		if (type == Type.DUTY) {
			sb.append("p:").append(pid).append(" ");
		}
		sb.append("d:").append(id);
		sb.append(" w:").append(load);
		return sb.toString();
	}

	@Override
	public int compareTo(ShardEntity o) {
		return this.getEntity().getId().compareTo(o.getEntity().getId());
	}

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

	public void setStuckCause(StuckCause stuckCause) {
		this.stuckCause = stuckCause;
	}

	public Type getType() {
		return this.type;
	}

}
