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
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicLong;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.SetMultimap;

import io.tilt.minka.core.leader.PartitionTable;
import io.tilt.minka.domain.EntityEvent;
import io.tilt.minka.domain.Shard;
import io.tilt.minka.domain.ShardEntity;
import io.tilt.minka.domain.ShardEntity.State;

/**
 * A distribution change in progress, modified indirectly by the {@linkplain Balancer}.
 * analyzing the {@linkplain PartitionTable}.
 * Composed of duties migrations from a shard to another, deletions, creations, etc. 
 * 
 * Such operations takes coordination to avoid parallelism and inconsistencies 
 * while they're yet to confirm, and needs to stay still while shards react, they also may fall.
 * 
 * No new distributions are made while this isn't finished
 * 
 * @author Cristian Gonzalez
 * @since Dec 11, 2015
 */
public class Roadmap implements Comparable<Roadmap> {

	private final Logger logger = LoggerFactory.getLogger(getClass());

	private SetMultimap<Shard, ShardEntity> problems;
	private SetMultimap<Shard, ShardEntity> currentGroup;
	private Iterator<SetMultimap<Shard, ShardEntity>> it;
	private final List<SetMultimap<Shard, ShardEntity>> issues;

	private final long id;
	private final DateTime creation;
	private int retryCounter;

	private static final AtomicLong sequence = new AtomicLong();

	private final static int MAX_STEPS = 2;
	private final static int STEP_DETACH = 0;
	private final static int STEP_ATTACH = 1;
	

	public Roadmap() {
		this.issues = new ArrayList<>(MAX_STEPS);
		this.currentGroup = null;
		this.id = sequence.incrementAndGet();
		this.creation = new DateTime(DateTimeZone.UTC);
		this.problems = HashMultimap.create();
	}

	public void incrementRetry() {
		retryCounter++;
	}

	public int getRetryCount() {
		return retryCounter;
	}

	public void reset() {
		this.issues.clear();
	}

	private SetMultimap<Shard, ShardEntity> init(int idx) {
		SetMultimap<Shard, ShardEntity> dutyGroup = issues.size() > idx ? issues.get(idx) : null;
		if (dutyGroup == null) {
			for (int i = issues.size(); issues.size() < idx; i++) {
				issues.add(i, HashMultimap.create());
			}
			issues.add(idx, dutyGroup = HashMultimap.create());
		}
		return dutyGroup;
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
		return issues.isEmpty();
	}

	public boolean hasFinished() {
		return it == null || !it.hasNext();
	}

	public void nextStep() {
		if (it == null) {
			it = issues.iterator();
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

	public DateTime getCreation() {
		return this.creation;
	}

	public SetMultimap<Shard, ShardEntity> getGroupedIssues() {
		return currentGroup;
	}

	public long getId() {
		return this.id;
	}

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
