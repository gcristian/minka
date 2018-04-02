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


import static java.util.Objects.requireNonNull;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.tilt.minka.api.Config;
import io.tilt.minka.api.DependencyPlaceholder;
import io.tilt.minka.api.Duty;
import io.tilt.minka.core.follower.HeartbeatFactory;
import io.tilt.minka.core.leader.distributor.Plan;
import io.tilt.minka.core.task.LeaderShardContainer;
import io.tilt.minka.domain.DomainInfo;
import io.tilt.minka.domain.DutyDiff;
import io.tilt.minka.domain.EntityEvent;
import io.tilt.minka.domain.EntityJournal.Log;
import io.tilt.minka.domain.NetworkShardIdentifier;
import io.tilt.minka.domain.Heartbeat;
import io.tilt.minka.domain.ShardCapacity.Capacity;
import io.tilt.minka.domain.ShardEntity;
import io.tilt.minka.domain.ShardIdentifier;
import io.tilt.minka.domain.ShardedPartition;
import io.tilt.minka.domain.EntityState;
import io.tilt.minka.utils.LogUtils;

/**
 * It keeps sending heartbeats to the leader as long as is alive
 * 
 * @author Cristian Gonzalez
 * @since Nov 17, 2015
 */
public class HeartbeatFactoryImpl implements HeartbeatFactory {

	private final Logger log = LoggerFactory.getLogger(getClass());

	private final DependencyPlaceholder dependencyPlaceholder;
	private final ShardedPartition partition;
	private final AtomicLong sequence;
	private final Config config;
	private final LeaderShardContainer leaderShardContainer;
	
	private DomainInfo domain; 
	private long includeTimestamp;
	private long includeFrequency = 10 * 1000l;
	private ShardIdentifier lastLeader;
	
	public HeartbeatFactoryImpl(
			final Config config, 
			final DependencyPlaceholder holder, 
			final ShardedPartition partition, 
			final LeaderShardContainer leaderShardContainer) {
		super();
		this.config = requireNonNull(config);
		this.dependencyPlaceholder = requireNonNull(holder);
		this.partition = requireNonNull(partition);
		this.sequence = new AtomicLong();
		this.leaderShardContainer = leaderShardContainer;
	}

	@Override
	public Heartbeat create() {
		final long now = System.currentTimeMillis();
		// this's used only if there's nothing important to report (differences, absences, etc)
		final Heartbeat.Builder builder = Heartbeat.builder(sequence.getAndIncrement(), partition.getId());
		final Set<Duty<?>> reportedDuties = askCurrentCapture();
		// add reported: as confirmed if previously assigned, dangling otherwise.
		final List<ShardEntity> tmp = new ArrayList<>(reportedDuties.size() + partition.getDuties().size()); 
		boolean issues = analyzeAndDetect(builder, reportedDuties, tmp);
		issues |= detectAbsents(reportedDuties, tmp);
		
		final boolean exclusionExpired = includeTimestamp == 0 || (now - includeTimestamp) > includeFrequency;
				
		boolean newLeader = false; 
		final NetworkShardIdentifier leader = leaderShardContainer.getLeaderShardId();
		if (leader!=null && !leader.equals(lastLeader)) {
			this.lastLeader = leader;
			newLeader = true;
		}
		
		if (issues || exclusionExpired || partition.wasRecentlyUpdated() || newLeader) {	
			tmp.forEach(d->builder.addReportedCapturedDuty(d));
			builder.reportsDuties();
			includeTimestamp = now;
		}
		addReportedCapacities(builder);
		final Heartbeat hb = builder.build();
		if (log.isDebugEnabled()) {
			logDebugNicely(hb);
		} else {
			log.info("{}: ({}) {} SeqID: {}, {}", 
				getClass().getSimpleName(), hb.getShardId(),LogUtils.HB_CHAR, hb.getSequenceId(), 
				hb.reportsDuties() ? new StringBuilder("Duties: (")
					.append(ShardEntity.toStringIds(hb.getReportedCapturedDuties()))
					.append(")").toString() : "");
		}
		return hb;
	}
	
	/* analyze reported duties and return if there're issues */
	private boolean analyzeAndDetect(
	        final Heartbeat.Builder builder,
			final Set<Duty<?>> reportedDuties, 
			final List<ShardEntity> temp) {
	    
		boolean includeDuties = false;
		for (final Duty<?> duty : reportedDuties) {
			ShardEntity shardedDuty = partition.getFromRawDuty(duty);
			if (shardedDuty != null) {
				final DutyDiff ddiff = new DutyDiff(duty, shardedDuty.getDuty());
				if (ddiff.hasDiffs()) {
					log.error("{}: ({}) Delegate reports a different duty than originally attached ! {}", 
						getClass().getSimpleName(), partition.getId(), ddiff.getDiff());
					builder.addDifference(ddiff);
					builder.withWarning();
					includeDuties = true;
				} else {
					includeDuties |= detectReception(duty, shardedDuty);
				}
			} else {
				includeDuties = true;
				shardedDuty = ShardEntity.Builder.builder(duty).build();
				shardedDuty.getJournal().addEvent(EntityEvent.ATTACH, 
				        EntityState.DANGLING, 
				        this.partition.getId(), 
                        Plan.PLAN_UNKNOWN);
				log.error("{}: ({}) Reporting a Dangling Duty (by Addition): {}", getClass().getSimpleName(),
						partition.getId(), shardedDuty);
				builder.withWarning();
			}
			temp.add(shardedDuty);
		}
		return includeDuties;
	}
	
	private boolean detectReception(final Duty<?> duty, final ShardEntity shardedDuty) {
		// consider only the last action logged to this shard
		final Log found = shardedDuty.getJournal().find(partition.getId()); 
		final EntityState stamp = EntityState.CONFIRMED;
		if (found.getLastState()!=stamp) {
			log.info("{}: ({}) Changing {} to {} duty: {}", getClass().getSimpleName(), partition.getId(),
					found.getLastState(), stamp, duty.getId());
			shardedDuty.getJournal().addEvent(
					found.getEvent(), 
					stamp,
					partition.getId(), 
					found.getPlanId());
			return true;
		}
		return false;
	}
	
	public enum Issue {
		DIFFERENCES,
		DANGLING,
		CLEARANCE_EXPIRED,
		REBEL_SHARD,
	}

	@SuppressWarnings("unchecked")
	private Set<Duty<?>> askCurrentCapture() {
		Set<Duty<?>> reportedDuties;
		try {
			reportedDuties = dependencyPlaceholder.getDelegate().reportCapture();
		} catch (Exception e) {
			log.error("{}: ({}) PartitionDelegate failure", getClass().getSimpleName(), config.getLoggingShardId(), e);
			reportedDuties = Collections.emptySet();
		}
		return reportedDuties;
	}
	
	@SuppressWarnings("unchecked")	
	private void addReportedCapacities(final Heartbeat.Builder builder) {
		if (domain!=null && domain.getDomainPallets()!=null) {
			for (ShardEntity pallet: domain.getDomainPallets()) {
				double capacity = 0;
				try {
					capacity = dependencyPlaceholder.getDelegate().getTotalCapacity(pallet.getPallet());
				} catch (Exception e) {
					log.error("{}: ({}) Error ocurred while asking for total capacity on Pallet: {}", getClass().getSimpleName(),
							partition.getId(), pallet.getPallet(), e);
				} finally {
					builder.addCapacity(pallet.getPallet(), new Capacity(pallet.getPallet(), capacity));
				}
			}
		}
	}

	/* if there were absent o not */
	private boolean detectAbsents(final Set<Duty<?>> reportedDuties, final List<ShardEntity> tmp) {
		boolean ret = false;
		// add non-reported: as dangling
		for (final ShardEntity existing : partition.getDuties()) {
			if (ret = !reportedDuties.contains(existing.getEntity())) {
				existing.getJournal().addEvent(EntityEvent.REMOVE, 
					existing.getDuty().isLazyFinalized() ? EntityState.FINALIZED : EntityState.DANGLING, 
			        this.partition.getId(), 
			        Plan.PLAN_UNKNOWN);
				log.error("{}: ({}) Reporting a Dangling Duty (by Erasure): {}", getClass().getSimpleName(),
						partition.getId(), existing);
				tmp.add(existing);
			}
		}
		return ret;
	}


	private void logDebugNicely(final Heartbeat hb) {
	    if (!log.isDebugEnabled()) {
	        return;
	    }
		final StringBuilder sb = new StringBuilder();
		List<ShardEntity> sorted = hb.getReportedCapturedDuties();
		if (!sorted.isEmpty()) {
			sorted.sort(sorted.get(0));
		}

		long totalWeight = 0;
		for (ShardEntity i : hb.getReportedCapturedDuties()) {
			sb.append(i.getDuty().getId()).append("(").append(i.getDuty().getWeight()).append(")").append(", ");
			totalWeight += i.getDuty().getWeight();
		}

		log.debug("{}: ({}) {} SeqID: {}, Duties: {}, Weight: {} = [ {}] {}", getClass().getSimpleName(),
				hb.getShardId(), 
				LogUtils.HB_CHAR, 
				hb.getSequenceId(), 
				hb.getReportedCapturedDuties().size(), 
				totalWeight,
				sb.toString(), 
				hb.getReportedCapturedDuties().isEmpty() ? "" : "reportDuties"
				);
	}

	@Override
	public void setDomainInfo(final DomainInfo domain) {
		this.domain = domain;
	}
}
