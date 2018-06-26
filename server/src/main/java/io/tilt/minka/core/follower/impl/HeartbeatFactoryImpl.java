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
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.tilt.minka.api.Config;
import io.tilt.minka.core.follower.HeartbeatFactory;
import io.tilt.minka.core.task.LeaderAware;
import io.tilt.minka.domain.Capacity;
import io.tilt.minka.domain.DependencyPlaceholder;
import io.tilt.minka.domain.DomainInfo;
import io.tilt.minka.domain.EntityJournal.Log;
import io.tilt.minka.domain.EntityRecord;
import io.tilt.minka.domain.EntityState;
import io.tilt.minka.domain.Heartbeat;
import io.tilt.minka.domain.NetworkShardIdentifier;
import io.tilt.minka.domain.ShardEntity;
import io.tilt.minka.domain.ShardIdentifier;
import io.tilt.minka.domain.ShardedPartition;
import io.tilt.minka.utils.LogUtils;

/**
 * It keeps sending heartbeats to the leader as long as is alive
 * 
 * @author Cristian Gonzalez
 * @since Nov 17, 2015
 */
public class HeartbeatFactoryImpl implements HeartbeatFactory {

	private final Logger log = LoggerFactory.getLogger(getClass());
	private final String classname = getClass().getSimpleName();

	private final DependencyPlaceholder dependencyPlaceholder;
	private final ShardedPartition partition;
	private final AtomicLong sequence;
	private final LeaderAware leaderAware;
	private final long includeFrequency;
	
	private DomainInfo domain; 
	private long includeTimestamp;
	private boolean logBeat;
	private ShardIdentifier lastLeader;
	
	public HeartbeatFactoryImpl(
			final Config config, 
			final DependencyPlaceholder holder, 
			final ShardedPartition partition, 
			final LeaderAware leaderAware) {
		super();
		this.dependencyPlaceholder = requireNonNull(holder);
		this.partition = requireNonNull(partition);
		this.sequence = new AtomicLong();
		this.leaderAware = requireNonNull(leaderAware);
		this.includeFrequency = config.beatToMs(50);
	}

	@Override
	public Heartbeat create(final boolean forceFullReport) {
		final long now = System.currentTimeMillis();
		// this's used only if there's nothing important to report (differences, absences, etc)
		final Heartbeat.Builder builder = Heartbeat.builder(sequence.getAndIncrement(), partition.getId());
		// add reported: as confirmed if previously assigned, dangling otherwise.
		final List<EntityRecord> tmp = new ArrayList<>(partition.getDuties().size()); 
		boolean issues = detectChangesOnReport(builder, d->tmp.add(EntityRecord.fromEntity(d)));
		logBeat |=issues;
		
		boolean newLeader = false; 
		final NetworkShardIdentifier leader = leaderAware.getLeaderShardId();
		if (leader!=null && !leader.equals(lastLeader)) {
			this.lastLeader = leader;
			newLeader = true;
			// put last inc. timestamp older so exclusion expires and full report beats follows 
			includeTimestamp = (now - includeFrequency) + 1;
		}
		logBeat |=newLeader;

		final boolean exclusionExpired = includeTimestamp == 0 || (now - includeTimestamp) > includeFrequency;
		
		final boolean doFullReport = forceFullReport || issues || exclusionExpired || partition.wasRecentlyUpdated() || newLeader;
		if (doFullReport) {
			tmp.forEach(builder::addReportedCapturedDuty);
			builder.reportsDuties();
			includeTimestamp = now;
		}
		addReportedCapacities(builder);
		final Heartbeat hb = builder.build();
		if (log.isDebugEnabled()) {
			logDebugNicely(hb);
		} else if (log.isInfoEnabled() && logBeat) {
			logBeat = false;
			log.info("{}: ({}) {} SeqID: {}, {}", 
				getClass().getSimpleName(), hb.getShardId(), LogUtils.HB_CHAR, hb.getSequenceId(), 
				hb.reportsDuties() ? new StringBuilder("Duties: (")
					.append(EntityRecord.toStringIds(hb.getReportedCapturedDuties()))
					.append(")").toString() : "");
		}
		return hb;
	}
	
	/* analyze reported duties and return if there're issues */
	private boolean detectChangesOnReport(
	        final Heartbeat.Builder builder,
			final Consumer<ShardEntity> c) {
	    
		boolean includeDuties = false;
		final StringBuilder tmp = new StringBuilder();
		for (ShardEntity shardedDuty: partition.getDuties()) {
			includeDuties |= detectReception(shardedDuty, tmp);
			c.accept(shardedDuty);
		}
		if (tmp.length()>0 && log.isInfoEnabled()) {
			log.info(tmp.toString());
		}
		return includeDuties;
	}
	
	/** this confirms action to the leader */
	private boolean detectReception(final ShardEntity shardedDuty, final StringBuilder tmp) {
		// consider only the last action logged to this shard
		final Log found = shardedDuty.getJournal().find(partition.getId()); 
		final EntityState stamp = EntityState.CONFIRMED;
		if (found.getLastState()!=stamp) {
			if (log.isInfoEnabled()) {
				if (tmp.length()==0) {
					tmp.append(String.format("%s: (%s) Changing %s to %s duties: ", 
							classname, partition.getId(), found.getLastState(), stamp));
				}
				tmp.append(shardedDuty.getDuty().getId()).append(',');
			}
			shardedDuty.getJournal().addEvent(
					found.getEvent(), 
					stamp,
					partition.getId(), 
					found.getPlanId());
			return true;
		}
		return false;
	}
	
	@SuppressWarnings("unchecked")	
	private void addReportedCapacities(final Heartbeat.Builder builder) {
		if (domain!=null && domain.getDomainPallets()!=null) {
			for (ShardEntity pallet: domain.getDomainPallets()) {
				double capacity = 0;
				try {
					capacity = dependencyPlaceholder.getDelegate().getTotalCapacity(pallet.getPallet());
				} catch (Exception e) {
					log.error("{}: ({}) Error ocurred while asking for total capacity on Pallet: {}", classname,
							partition.getId(), pallet.getPallet(), e);
				} finally {
					builder.addCapacity(pallet.getPallet(), new Capacity(pallet.getPallet(), capacity));
				}
			}
		}
	}

	private void logDebugNicely(final Heartbeat hb) {
	    if (!log.isDebugEnabled()) {
	        return;
	    }
		final StringBuilder sb = new StringBuilder();
		List<EntityRecord> sorted = hb.getReportedCapturedDuties();
		if (!sorted.isEmpty()) {
			sorted.sort(sorted.get(0));
		}

		log.debug("{}: ({}) {} SeqID: {}, Duties: {} = [ {}] {}", classname,
				hb.getShardId(), 
				LogUtils.HB_CHAR, 
				hb.getSequenceId(), 
				hb.getReportedCapturedDuties().size(), 
				EntityRecord.toStringIds(hb.getReportedCapturedDuties()), 
				hb.getReportedCapturedDuties().isEmpty() ? "" : "reportDuties"
				);
	}

	@Override
	public void setDomainInfo(final DomainInfo domain) {
		this.domain = domain;
	}
}
