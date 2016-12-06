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
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicLong;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.SetMultimap;

import io.tilt.minka.core.leader.PartitionTable;
import io.tilt.minka.core.leader.PartitionTable.ClusterCapacity;
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
public class Roadmap implements Comparable<Roadmap> {

	private static final Logger logger = LoggerFactory.getLogger(Roadmap.class);
	
	private final static int MAX_STEPS = 2;
	private final static int STEP_DETACH = 0;
	private final static int STEP_ATTACH = 1;
	
	@JsonIgnore private final List<SetMultimap<Shard, ShardEntity>> deliveriesImmanent;
	@JsonIgnore private final List<SetMultimap<Shard, ShardEntity>> deliveries;
	@JsonIgnore private SetMultimap<Shard, ShardEntity> problems;
	@JsonIgnore private SetMultimap<Shard, ShardEntity> currentGroup;
	@JsonIgnore private Iterator<SetMultimap<Shard, ShardEntity>> it;
	private ClusterCapacity capacityStatus;

	private final long id;
	@JsonIgnore private final DateTime created;
	@JsonIgnore private DateTime started;
	@JsonIgnore private DateTime ended;
	private int retryCounter;

	private static final AtomicLong sequence = new AtomicLong();

	public Roadmap() {
		this.deliveries = new ArrayList<>(MAX_STEPS);
		this.deliveriesImmanent = new ArrayList<>(MAX_STEPS);
		this.currentGroup = null;
		this.id = sequence.incrementAndGet();
		this.created = new DateTime(DateTimeZone.UTC);
		this.problems = HashMultimap.create();
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
	@JsonProperty("deliveries") /* only for status representation */
	public List<Map<Shard, Collection<ShardEntity>>> getDeliveries() {
		return toMap(deliveriesImmanent);
	}
	private List<Map<Shard, Collection<ShardEntity>>> toMap(final List<SetMultimap<Shard, ShardEntity>> multimap) {
		final List<Map<Shard, Collection<ShardEntity>>> ret = new ArrayList<>();
		multimap.forEach(mm->ret.add(mm.asMap()));
		return ret;
 	}
	
	public void incrementRetry() {
		retryCounter++;
	}
	
	public int getRetryCount() {
		return retryCounter;
	}
	
	public void open() {
		this.started= new DateTime(DateTimeZone.UTC);
		deliveries.forEach(i->deliveriesImmanent.add(i));
	}

	public void close() {
		this.deliveries.clear();
		this.ended = new DateTime(DateTimeZone.UTC);
	}

	private SetMultimap<Shard, ShardEntity> init(int idx) {
		SetMultimap<Shard, ShardEntity> dutyGroup = deliveries.size() > idx ? deliveries.get(idx) : null;
		if (dutyGroup == null) {
			for (int i = deliveries.size(); deliveries.size() < idx; i++) {
				deliveries.add(i, HashMultimap.create());
			}
			deliveries.add(idx, dutyGroup = HashMultimap.create());
		}
		return dutyGroup;
	}
	
	public void setCapacityStatus(final ClusterCapacity status) {
		this.capacityStatus = status;
	}
	public ClusterCapacity getCapacityStatus() {
		return this.capacityStatus;
	}
	
	/** declare a dettaching or attaching step to deliver on a shard */
	public void ship(final Shard shard, final ShardEntity duty) {
		if (duty.getDutyEvent().is(EntityEvent.CREATE) || duty.getDutyEvent().is(EntityEvent.ATTACH)) {
			init(STEP_ATTACH).put(shard, duty);
		} else if (duty.getDutyEvent().is(EntityEvent.REMOVE) || duty.getDutyEvent().is(EntityEvent.DETACH)) {
			init(STEP_DETACH).put(shard, duty);
		}
	}

	public boolean isEmpty() {
		return deliveries.isEmpty();
	}

	public boolean hasFinished() {
		return it == null || !it.hasNext();
	}

	public void nextStep() {
		if (it == null) {
			it = deliveries.iterator();
		}
		if (it.hasNext()) {
			currentGroup = it.next();
		}
	}

	public boolean hasCurrentStepFinished() {
		final Set<ShardEntity> sortedLog = new TreeSet<>();
		for (final Shard shard : currentGroup.keySet()) {
			for (final ShardEntity duty : currentGroup.get(shard)) {
				if (duty.getState() != State.CONFIRMED) {
					// TODO get Partition TAble and check if Shard has long fell offline
					sortedLog.add(duty);
					logger.info("{}: waiting Shard: {} for at least Duties: {}", getClass().getSimpleName(), shard,
							ShardEntity.toStringIds(sortedLog));
					return false;
				}
			}
		}
		return true;
	}

	@JsonIgnore
	public DateTime getCreation() {
		return this.created;
	}

	@JsonIgnore
	public SetMultimap<Shard, ShardEntity> getGroupedDeliveries() {
		return currentGroup;
	}

	public long getId() {
		return this.id;
	}
	public DateTime getEnded() {
		return this.ended;
	}
	public DateTime getStarted() {
		return this.started;
	}

	@JsonIgnore
	public SetMultimap<Shard, ShardEntity> getProblems() {
		return this.problems;
	}

	public void setProblems(SetMultimap<Shard, ShardEntity> problems) {
		this.problems = problems;
	}

	@Override
	public int compareTo(Roadmap o) {
		return o.getCreation().compareTo(getCreation());
	}

}
