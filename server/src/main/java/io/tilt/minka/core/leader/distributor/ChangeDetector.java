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

import java.util.Date;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import org.apache.logging.log4j.util.BiConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.tilt.minka.core.leader.PartitionScheme;
import io.tilt.minka.domain.EntityEvent;
import io.tilt.minka.domain.EntityJournal.Log;
import io.tilt.minka.domain.EntityState;
import io.tilt.minka.domain.Heartbeat;
import io.tilt.minka.domain.Shard;
import io.tilt.minka.domain.ShardEntity;
import io.tilt.minka.domain.ShardIdentifier;

/**
 * Contains the mechanism and knowledge for determining reallocation changes  
 * Inspects {@linkplain Heartbeat}'s looking for expected 
 * changes according the current distribution {@linkplain ChangePlan}
 *  
 * @author Cristian Gonzalez
 * @since Mar 7, 2018
 */
public class ChangeDetector {

	private static final int MAX_EVENT_DATE_FOR_DIRTY = 10000;
	
	private final Logger logger = LoggerFactory.getLogger(getClass());
	private final String classname = getClass().getSimpleName();
	
	private final PartitionScheme partitionScheme;
	
	/** solely instance */
	public ChangeDetector(final PartitionScheme partitionScheme) {
		this.partitionScheme = partitionScheme;
	}

	/** Finds sharding changes by matching entity journals for planned reallocations, and updating the scheme */
	public boolean findChanges(
			final Delivery delivery, 
			final ChangePlan changePlan, 
			final Heartbeat beat, 
			final Shard source,
			final BiConsumer<Log, ShardEntity> bicons) {
		long lattestPlanId = 0;
		boolean found = false;
		if (beat.reportsDuties()) {
			lattestPlanId = latestPlan(beat);
			if (lattestPlanId==changePlan.getId()) {
				found|=onFoundAttachments(source, beat.getReportedCapturedDuties(), delivery, bicons);
			}
		}
		found|=onFoundDetachments(source, beat.getReportedCapturedDuties(), delivery, bicons);

		if (!found && lattestPlanId==changePlan.getId()) {
			// delivery found, no change but expected ? 
			logger.warn("{}: Ignoring shard's ({}) heartbeats with a previous Journal: {} (current: {})", 
					getClass().getSimpleName(), source.getShardID().toString(), lattestPlanId, changePlan.getId());			
		}
		return found;
	}

	/* latest entity's journal plan ID among the beat */
	private long latestPlan(final Heartbeat beat) {
		long lattestPlanId = 0;
		for (ShardEntity e: beat.getReportedCapturedDuties()) {
			final long pid = e.getJournal().getLast().getPlanId();
			if (pid > lattestPlanId) {
				lattestPlanId = pid;
			}
		}
		return lattestPlanId;
	}
	
	
	/** 
	 * find the up-coming
	 * @return if there were changes 
	 */
	private boolean onFoundAttachments(
			final Shard shard, 
			final List<ShardEntity> beatedDuties, 
			final Delivery delivery,
			final BiConsumer<Log, ShardEntity> c) {
		Set<ShardEntity> sortedLogConfirmed = null;
		Set<ShardEntity> sortedLogDirty = null;
		boolean found = false;
		for (final ShardEntity beated : beatedDuties) {
			for (ShardEntity delivered : delivery.getDuties()) {
				if (delivered.equals(beated)) {
					final Log expected = findConfirmationPair(beated, delivered, shard.getShardID());
					if (expected != null) {
						found = true;
						if (logger.isInfoEnabled()) {
							if (sortedLogConfirmed == null) {
								sortedLogConfirmed = new TreeSet<>();
							}
							sortedLogConfirmed.add(beated);
						}
						c.accept(expected, delivered);
					} else {
						final Date fact = delivered.getJournal().getLast().getHead();
						final long now = System.currentTimeMillis();
						if (now - fact.getTime() > MAX_EVENT_DATE_FOR_DIRTY) {
							if (sortedLogDirty == null) {
								sortedLogDirty = new TreeSet<>();
							}
							sortedLogDirty.add(beated);
						}
					}
					break;
				}
			}
		}
		if (sortedLogConfirmed!=null) {
			logger.info("{}: ShardID: {}, Confirming partition event for Duties: {}", classname,
					shard.getShardID(), ShardEntity.toStringIds(sortedLogConfirmed));
		}
		if (sortedLogDirty!=null) {
			logger.warn("{}: ShardID: {}, Reporting DIRTY partition event for Duties: {}", classname,
					shard.getShardID(), ShardEntity.toStringIds(sortedLogDirty));
		}
		return found;
	}

	/** @return the expected delivered event matching the beated logs */
	private Log findConfirmationPair(
			final ShardEntity beated,
			final ShardEntity delivered,
			final ShardIdentifier shardid) {
		Log ret = null;
		// beated duty must belong to current non-expired plan
		final long pid = partitionScheme.getCurrentPlan().getId();
		final Log beatedLog = beated.getJournal().find(pid, shardid, EntityEvent.ATTACH);
		if (beatedLog != null) {
			final EntityState beatedState = beatedLog.getLastState();
			if (beatedState == EntityState.CONFIRMED || beatedState == EntityState.RECEIVED) {
				final Log deliLog = delivered.getJournal().find(pid, shardid, EntityEvent.ATTACH);
				if (deliLog != null) {
					final EntityState deliState = deliLog.getLastState();
					if (deliState == EntityState.PENDING || deliState !=EntityState.CONFIRMED) {
						// expected normal situation
						ret = deliLog;
					} else if (deliState == EntityState.CONFIRMED) {
						// when cluster unstable: bad state but possible 
						final Shard location = partitionScheme.getScheme().getDutyLocation(delivered.getDuty());
						if (location==null || !location.getShardID().equals(shardid)) {
							ret = deliLog;	
						}
					} else {
						logger.error("{}: confirmation pair found erroneous state: {} on duty: {}",
								getClass().getSimpleName(), deliState, delivered);
					}
				}
			} else {
				logger.error("{}: Reporting state: {} on Duty: {}", classname, beatedState);
			}
		}
		return ret;
	}
	
	/** treat un-coming as detaches
	/** @return whether or not there was an absence confirmed 
	 * @param c */
	private boolean onFoundDetachments(
			final Shard shard,
			final List<ShardEntity> beatedDuties,
			final Delivery delivery,
			final BiConsumer<Log, ShardEntity> c) {
		Set<ShardEntity> sortedLog = null;
		final long pid = partitionScheme.getCurrentPlan().getId();
		boolean found = false;
		for (ShardEntity prescripted : delivery.getDuties()) {
			if (!beatedDuties.contains(prescripted)) {
				final Log changelog = prescripted.getJournal().find(pid, shard.getShardID(), EntityEvent.DETACH, EntityEvent.REMOVE);
				if (changelog!=null && (changelog.getLastState()==EntityState.PENDING || changelog.getLastState()==EntityState.MISSING)) {
					found = true;
					if (logger.isInfoEnabled()) {
						if (sortedLog==null) {
							sortedLog = new TreeSet<>();
						}
						sortedLog.add(prescripted);
					}
					c.accept(changelog, prescripted);
				}
			}
		}
		
		if (sortedLog!=null) {
			logger.info("{}: ShardID: {}, Confirming (by absence) partioning event for Duties: {}",
					getClass().getSimpleName(), shard.getShardID(), ShardEntity.toStringIds(sortedLog));
		}
		return found;
	}
}