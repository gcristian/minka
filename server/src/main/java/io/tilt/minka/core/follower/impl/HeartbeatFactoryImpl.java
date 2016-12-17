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
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.tilt.minka.api.Config;
import io.tilt.minka.api.DependencyPlaceholder;
import io.tilt.minka.api.Duty;
import io.tilt.minka.core.follower.HeartbeatFactory;
import io.tilt.minka.core.task.LeaderShardContainer;
import io.tilt.minka.domain.ShardedPartition;
import io.tilt.minka.domain.DomainInfo;
import io.tilt.minka.domain.EntityEvent;
import io.tilt.minka.domain.Heartbeat;
import io.tilt.minka.domain.Heartbeat.DutyDiff;
import io.tilt.minka.domain.ShardCapacity.Capacity;
import io.tilt.minka.domain.ShardEntity;
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
	
	private DomainInfo domain; 
	private long lastIncludedDutiesTimestamp;
	private long includeDutiesFrequency = 10 * 1000l;
	
	public HeartbeatFactoryImpl(final Config config, final DependencyPlaceholder holder, 
			final ShardedPartition partition) {
		super();
		this.config = config;
		this.dependencyPlaceholder = holder;
		this.partition = partition;
		this.sequence = new AtomicLong();
	}

	@Override
	public Heartbeat create() {
		final long now = System.currentTimeMillis();
		// this's used only if there's nothing important to report (differences, absences, etc)
		final Heartbeat.Builder builder = Heartbeat.Builder.builder(sequence.getAndIncrement(), partition.getId());
		final Set<Duty<?>> reportedDuties = askCurrentCapture();
		// add reported: as confirmed if previously assigned, dangling otherwise.
		final List<ShardEntity> temp = new ArrayList<>(reportedDuties.size() + partition.getDuties().size()); 
		boolean issues = addReported(builder, reportedDuties, temp);
		issues|=addAbsents(reportedDuties, temp);
		final boolean exclusionExpired = lastIncludedDutiesTimestamp == 0 || now - lastIncludedDutiesTimestamp > includeDutiesFrequency;
		if (issues || exclusionExpired || partition.wasRecentlyUpdated()) {			
			temp.forEach(d->builder.addReportedCapturedDuty(d));
			lastIncludedDutiesTimestamp = now;
		}
		addReportedCapacities(builder);
		final Heartbeat hb = builder.build();
		if (log.isDebugEnabled()) {
			logDebugNicely(hb);
		} else {
			log.debug("{}: ({}) {} SeqID: {}, Duties: {}", getClass().getSimpleName(), hb.getShardId(),
					LogUtils.HB_CHAR, hb.getSequenceId(), hb.isReportedCapturedDuties() ? 
							hb.getReportedCapturedDuties().size() : "single");
		}
		return hb;
	}

	/* analyze reported duties and return if there're issues */
	private boolean addReported(final Heartbeat.Builder builder,
			final Set<Duty<?>> reportedDuties, final List<ShardEntity> temp) {
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
					shardedDuty.registerEvent(CONFIRMED);
				}
			} else {
				includeDuties = true;
				shardedDuty = ShardEntity.Builder.builder(duty).build();
				// shardedDuty.registerEvent(PartitionEvent.ASSIGN, State.DANGLING);
				shardedDuty.registerEvent(EntityEvent.CREATE, DANGLING);
				log.error("{}: ({}) Reporting a Dangling Duty (by Addition): {}", getClass().getSimpleName(),
						partition.getId(), shardedDuty);
				builder.withWarning();
			}
			temp.add(shardedDuty);
		}
		return includeDuties;
	}

	@SuppressWarnings("unchecked")
	private Set<Duty<?>> askCurrentCapture() {
		Set<Duty<?>> reportedDuties;
		try {
			reportedDuties = dependencyPlaceholder.getDelegate().reportCapture();
		} catch (Exception e) {
			log.error("{}: ({}) PartitionDelegate failure", getClass().getSimpleName(), config.getLoggingShardId(), e);
			reportedDuties = new HashSet<>();
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
	private boolean addAbsents(final Set<Duty<?>> reportedDuties, final List<ShardEntity> duties) {
		boolean ret = false;
		// add non-reported: as dangling
		for (final ShardEntity existing : partition.getDuties()) {
			if (ret = !reportedDuties.contains(existing.getEntity())) {
				if (existing.getDuty().isLazyFinalized()) { 
					existing.registerEvent(EntityEvent.REMOVE, ShardEntity.State.FINALIZED);
				} else {
					existing.registerEvent(EntityEvent.REMOVE, DANGLING);
				}
				log.error("{}: ({}) Reporting a Dangling Duty (by Erasure): {}", getClass().getSimpleName(),
						partition.getId(), existing);
				duties.add(existing);
			}
		}
		return ret;
	}


	private void logDebugNicely(final Heartbeat hb) {
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
				hb.getShardId(), LogUtils.HB_CHAR, hb.getSequenceId(), hb.getReportedCapturedDuties().size(), totalWeight,
				sb.toString(), hb.getReportedCapturedDuties().isEmpty() ? "" : "WITH CHANGE");
	}

	@Override
	public void setDomainInfo(final DomainInfo domain) {
		this.domain = domain;
	}
}