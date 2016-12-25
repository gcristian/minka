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
package io.tilt.minka.core.leader.distributor;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

import io.tilt.minka.core.leader.PartitionTable;
import io.tilt.minka.core.leader.distributor.Roadmap.Delivery;
import io.tilt.minka.domain.EntityEvent;
import io.tilt.minka.domain.Shard;
import io.tilt.minka.domain.ShardEntity;
import io.tilt.minka.domain.ShardEntity.State;

/**
 * Acts as a driveable distribution in progress created thru {@linkplain Migrator} 
 * by the {@linkplain Balancer} analyzing the {@linkplain PartitionTable}.
 * Composed of deliveries, migrations, deletions, creations, etc.
 * s 
 * Such operations takes coordination to avoid parallelism and inconsistencies 
 * while they're yet to confirm, and needs to stay still while shards react, they also may fall.
 * 
 * No new distributions are made while this isn't finished
 * 
 * @author Cristian Gonzalez
 * @since Dec 11, 2015
 */
@JsonAutoDetect
public class Roadmap implements Comparable<Roadmap>, Iterator<Delivery> {

	private static final Logger logger = LoggerFactory.getLogger(Roadmap.class);
	private static List<EntityEvent> sortedEvents = Arrays.asList(EntityEvent.REMOVE, EntityEvent.DETACH, EntityEvent.CREATE, EntityEvent.ATTACH);
	
	private static final AtomicLong sequence = new AtomicLong();
	private final long id;
	private final DateTime created;
	private final Map<EntityEvent, Map<Shard, List<ShardEntity>>> work;
	
	private Delivery lastDelivery;
	private int index;
	private Iterator<Delivery> iterator;
	private List<Delivery> deliveries;
	private DateTime started;
	private DateTime ended;
	private int retryCounter;

	/* plainer of the work map  */
	public static class Delivery {
		private EntityEvent event;
		private Shard shard;
		private final List<ShardEntity> duties;
		private final Map<ShardEntity.State, StringBuilder> states;
		private Delivery(final List<ShardEntity> duties, final Shard shard, final EntityEvent event) {
			super();
			this.duties = duties;
			this.shard = shard;
			this.event = event;
			this.states = new HashMap<>();
		}
		public List<ShardEntity> getDuties() {
			return this.duties;
		}
		public Shard getShard() {
			return this.shard;
		}
		protected EntityEvent getEvent() {
			return event;
		}
		@JsonProperty("state")
		private final Map<ShardEntity.State, StringBuilder> getState() {
			final Map<ShardEntity.State, StringBuilder> ret = new HashMap<>();
			duties.forEach(duty-> getOrPut(states, duty.getState(), ()->new StringBuilder())
					.append(duty.getDuty().getId()).append(", "));
			return ret;
		}
		boolean allConfirmed() {
			final Set<ShardEntity> sortedLog = new TreeSet<>();
			for (final ShardEntity duty : duties) {
				if (duty.getState() != State.CONFIRMED) {
					// TODO get Partition TAble and check if Shard has long fell offline
					sortedLog.add(duty);
					logger.info("{}: waiting Shard: {} for at least Duties: {}", getClass().getSimpleName(), shard,
							ShardEntity.toStringIds(sortedLog));
					return false;
				}
			}
			return true;
		}
	}

	public List<ShardEntity> getPending() {
		final List<ShardEntity> ret = new ArrayList<>();
		for (final Delivery delivery: deliveries) {
			for (final ShardEntity duty: delivery.getDuties()) {
				if (duty.getState()==State.SENT) {
					ret.add(duty);
				}
			}
			if (delivery.equals(lastDelivery)) {
				break;
			}
		}
		return ret;
	}
	
	@JsonProperty("deliveries")
	private List<Delivery> getDeliveries() {
		return this.deliveries;
	}
	
	public Roadmap() {
		this.id = sequence.incrementAndGet();
		this.created = new DateTime(DateTimeZone.UTC);		
		this.work = new HashMap<>();
	}

	public Delivery getDelivery(final Shard shard) {
		for (Delivery d: deliveries) {
			if (d.getShard().equals(shard)) {
				return d;
			}
		}
		return null;
	}
	
	public void open() {
		this.started= new DateTime(DateTimeZone.UTC);
		for (final EntityEvent event: sortedEvents) {
			final Map<Shard, List<ShardEntity>> map = getOrPut(work, event, ()->new HashMap<>());
			for (final Entry<Shard, List<ShardEntity>> e: map.entrySet()) {
				if (deliveries == null) {
					deliveries = new ArrayList<>();
				}
				deliveries.add(new Delivery(e.getValue(), e.getKey(), event));
			}
		}
		this.work.clear();
		iterator = deliveries.iterator();
	}

	@Override
	public Delivery next() {
		if (!hasPermission()) {
			throw new IllegalAccessError("no permission to advance forward");
		}
		index++;
		lastDelivery = iterator.next();
		return lastDelivery;
	}

	public boolean hasPermission() {
		if (lastDelivery != null && (deliveries.size()==index || (deliveries.size() <index &&
			 deliveries.get(index).getEvent()!=lastDelivery.getEvent()))) {
				return lastDelivery.allConfirmed();
		}
		return true;
	}
	@Override
	public boolean hasNext() {
		if (started==null) {
			throw new IllegalStateException("roadmap not started yet !");
		}
		return iterator.hasNext();
	}

	public void incrementRetry() {
		retryCounter++;
	}
	
	public int getRetryCount() {
		return retryCounter;
	}
	

	public void close() {
		if (!hasPermission()) {
			throw new IllegalStateException("roadmap not allowed to close last delivery unconfirmed !");
		}
		this.ended = new DateTime(DateTimeZone.UTC);
	}
	
	static <K, V>V getOrPut(final Map<K, V> map, final K key, final Supplier<V> sup) {
		if (map == null || key == null || sup == null) {
			throw new IllegalArgumentException("null map key or supplier");
		}
		V v = map.get(key);
		if (v == null) {
			map.put(key, v = sup.get());
		}
		return v;
	}
		
	/* declare a dettaching or attaching step to deliver on a shard */
	public synchronized void ship(final Shard shard, final ShardEntity duty) {
		final List<ShardEntity> list = getOrPut(
				getOrPut(work, duty.getDutyEvent(), ()->new HashMap<>()), 
				shard, ()->new ArrayList<>());
		list.add(duty);
	}

	public boolean isClosed() {
		return this.ended !=null;
	}
	
	public boolean isEmpty() {
		return work.isEmpty();
	}
	
	public DateTime getEnded() {
		return this.ended;
	}
	
	public DateTime getStarted() {
		return this.started;
	}

	@JsonProperty("created")
	private String getCreation_() {
		return created.toString();
	}
	@JsonProperty("started")
	private String getStarted_() {
		if (started==null) {
			return "";
		}
		return started.toString();
	}
	@JsonProperty("ended")
	private String getEnded_() {
		if (ended==null) {
			return "";
		}return ended.toString();
	}

	@JsonIgnore
	public DateTime getCreation() {
		return this.created;
	}

	public long getId() {
		return this.id;
	}

	@Override
	public int compareTo(Roadmap o) {
		return o.getCreation().compareTo(getCreation());
	}

}
