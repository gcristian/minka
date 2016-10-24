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
package io.tilt.minka.core.follower.impl;

import static io.tilt.minka.domain.ShardEntity.State.CONFIRMED;
import static io.tilt.minka.domain.ShardEntity.State.DANGLING;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.tilt.minka.api.Config;
import io.tilt.minka.api.DependencyPlaceholder;
import io.tilt.minka.api.Duty;
import io.tilt.minka.api.Pallet;
import io.tilt.minka.core.follower.HeartbeatBuilder;
import io.tilt.minka.core.task.LeaderShardContainer;
import io.tilt.minka.domain.AttachedPartition;
import io.tilt.minka.domain.DomainInfo;
import io.tilt.minka.domain.EntityEvent;
import io.tilt.minka.domain.Heartbeat;
import io.tilt.minka.domain.ShardCapacity;
import io.tilt.minka.domain.ShardCapacity.Capacity;
import io.tilt.minka.domain.ShardEntity;
import io.tilt.minka.utils.LogUtils;

/**
 * It keeps sending heartbeats to the leader as long as is alive
 * 
 * @author Cristian Gonzalez
 * @since Nov 17, 2015
 */
@SuppressWarnings({"rawtypes", "unchecked"})
public class HeartbeatBuilderImpl implements HeartbeatBuilder {

	private final Logger logger = LoggerFactory.getLogger(getClass());

	private final DependencyPlaceholder dependencyPlaceholder;
	private final AttachedPartition partition;
	private final AtomicLong sequence;
	private final Config config;
	
	private DomainInfo domain; 

	public HeartbeatBuilderImpl(final Config config, final DependencyPlaceholder holder, final AttachedPartition partition,
			final LeaderShardContainer leaderShardContainer) {

		super();
		this.config = config;
		this.dependencyPlaceholder = holder;
		this.partition = partition;
		this.sequence = new AtomicLong();
	}

	public Heartbeat build() {
		Set<Duty<?>> reportedDuties;
		try {
			reportedDuties = dependencyPlaceholder.getDelegate().reportTaken();
		} catch (Exception e) {
			logger.error("{}: ({}) PartitionDelegate failure", getClass().getSimpleName(), config.getLoggingShardId(), e);
			reportedDuties = new HashSet();
		}

		final List<ShardEntity> shardingDuties = new ArrayList<>();
		boolean warning = false;

		// add reported: as confirmed if previously assigned, dangling otherwise. 
		for (final Duty<?> duty : reportedDuties) {
			ShardEntity shardedDuty = partition.forDuty(duty);
			if (shardedDuty != null) {
				shardedDuty.registerEvent(CONFIRMED);
			} else {
				shardedDuty = ShardEntity.create(duty);
				// shardedDuty.registerEvent(PartitionEvent.ASSIGN, State.DANGLING);
				shardedDuty.registerEvent(EntityEvent.CREATE, DANGLING);
				logger.error("{}: ({}) Reporting a Dangling Duty (by Addition): {}", getClass().getSimpleName(),
						partition.getId(), shardedDuty);
				warning = true;
			}
			shardingDuties.add(shardedDuty);
		}

		addAbsentAsDangling(reportedDuties, shardingDuties);

		final Heartbeat hb = Heartbeat.create(shardingDuties, warning, partition.getId(), 
				sequence.getAndIncrement(), buildCapacities());
		if (logger.isDebugEnabled()) {
			logDebugNicely(hb);
		} else {
			logger.debug("{}: ({}) {} SeqID: {}, Duties: {}", getClass().getSimpleName(), hb.getShardId(),
					LogUtils.HB_CHAR, hb.getSequenceId(), hb.getDuties().size());
		}
		return hb;
	}

	private Map<Pallet<?>, ShardCapacity.Capacity> buildCapacities() {
		final Map<Pallet<?>, ShardCapacity.Capacity> capacities = new HashMap<>();
		if (domain!=null && domain.getDomainPallets()!=null) {
			for (ShardEntity s: domain.getDomainPallets()) {
				double capacity = 0;
				try {
					capacity = dependencyPlaceholder.getDelegate().getTotalCapacity(s.getPallet());
				} catch (Exception e) {
					logger.error("{}: ({}) Error ocurred while asking for total capacity on Pallet: {}", getClass().getSimpleName(),
							partition.getId(), s.getPallet(), e);
				} finally {
					final Capacity cap = new Capacity(s.getPallet(), capacity);
					capacities.put(s.getPallet(), cap);
				}
			}
		}
		return capacities;
	}

	public void setDomain(DomainInfo domain) {
		this.domain = domain;
	}

	private void addAbsentAsDangling(Set<Duty<?>> reportedDuties, final List<ShardEntity> shardingDuties) {
		// add non-reported: as dangling
		for (final ShardEntity existing : partition.getDuties()) {
			if (!reportedDuties.contains(existing.getEntity())) {
				existing.registerEvent(EntityEvent.REMOVE, DANGLING);
				logger.error("{}: ({}) Reporting a Dangling Duty (by Erasure): {}", getClass().getSimpleName(),
						partition.getId(), existing);
				shardingDuties.add(existing);
			}
		}
	}

	private void logDebugNicely(final Heartbeat hb) {
		final StringBuilder sb = new StringBuilder();
		List<ShardEntity> sorted = hb.getDuties();
		if (!sorted.isEmpty()) {
			sorted.sort(sorted.get(0));
		}

		long totalWeight = 0;
		for (ShardEntity i : hb.getDuties()) {
			sb.append(i.getDuty().getId()).append("(").append(i.getDuty().getWeight()).append(")").append(", ");
			totalWeight += i.getDuty().getWeight();
		}

		logger.debug("{}: ({}) {} SeqID: {}, Duties: {}, Weight: {} = [ {}] {}", getClass().getSimpleName(),
				hb.getShardId(), LogUtils.HB_CHAR, hb.getSequenceId(), hb.getDuties().size(), totalWeight,
				sb.toString(), hb.getDuties().isEmpty() ? "" : "WITH CHANGE");
	}

	@Override
	public void setDomainInfo(DomainInfo domain) {
		this.domain = domain;
	}
}
