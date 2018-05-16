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

import static io.tilt.minka.domain.EntityEvent.ATTACH;
import static io.tilt.minka.domain.EntityEvent.DETACH;
import static io.tilt.minka.domain.EntityEvent.REMOVE;
import static io.tilt.minka.domain.EntityState.CONFIRMED;
import static io.tilt.minka.domain.EntityState.MISSING;
import static io.tilt.minka.domain.EntityState.PENDING;
import static io.tilt.minka.domain.EntityState.RECEIVED;

import java.util.Date;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.BiConsumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.tilt.minka.core.leader.PartitionScheme;
import io.tilt.minka.domain.EntityJournal.Log;
import io.tilt.minka.domain.EntityState;
import io.tilt.minka.domain.Heartbeat;
import io.tilt.minka.domain.Shard;
import io.tilt.minka.domain.ShardEntity;
import io.tilt.minka.domain.ShardIdentifier;

/**
 * Contains the mechanism and knowledge for determining reallocation changes <br>
 * Inspects {@linkplain Heartbeat}'s looking for expected 
 * changes according the current distribution {@linkplain ChangePlan}<br>
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

	/** Finds sharding changes by matching entity journals for planned reallocations */
	public boolean findPlannedChanges(
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
				found|=onFoundAttachments(source, beat.getReportedCapturedDuties(), delivery, bicons, changePlan.getId());
			}
		}
		found|=onFoundDetachments(source, beat.getReportedCapturedDuties(), delivery, bicons, changePlan.getId());

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
			final BiConsumer<Log, ShardEntity> c,
			final long pid) {
		Set<ShardEntity> sortedLogConfirmed = null;
		Set<ShardEntity> sortedLogDirty = null;
		boolean found = false;
		for (final ShardEntity beated : beatedDuties) {
			for (ShardEntity delivered : delivery.getDuties()) {
				if (delivered.equals(beated)) {
					final Log expected = findConfirmationPair(beated, delivered, shard.getShardID(), pid);
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
			logger.info("{}: ShardID: {}, {} {} for Duties: {}", classname,
					shard.getShardID(), ATTACH, CONFIRMED, 
					ShardEntity.toStringIds(sortedLogConfirmed));
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
			final ShardIdentifier shardid, 
			final long pid) {
		Log ret = null;
		// beated duty must belong to current non-expired plan
		final Log beatedLog = beated.getJournal().find(pid, shardid, ATTACH);
		if (beatedLog != null) {
			final EntityState beatedState = beatedLog.getLastState();
			if (beatedState == CONFIRMED || beatedState == RECEIVED) {
				final Log deliLog = delivered.getJournal().find(pid, shardid, ATTACH);
				if (deliLog != null) {
					final EntityState deliState = deliLog.getLastState();
					if (deliState == PENDING || deliState !=CONFIRMED) {
						// expected normal situation
						ret = deliLog;
					} else if (deliState == CONFIRMED) {
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
	
	/**
	 * @return TRUE if there was a planned absence confirmed */
	private boolean onFoundDetachments(
			final Shard shard,
			final List<ShardEntity> beatedDuties,
			final Delivery delivery,
			final BiConsumer<Log, ShardEntity> c, 
			final long pid) {
		Set<ShardEntity> sortedLog = null;
		boolean found = false;
		for (ShardEntity prescripted : delivery.getDuties()) {
			if (!beatedDuties.contains(prescripted)) {
				final Log changelog = prescripted.getJournal().find(pid, shard.getShardID(), DETACH, REMOVE);
				if (changelog!=null && (changelog.getLastState()==PENDING || changelog.getLastState()==MISSING)) {
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
			logger.info("{}: ShardID: {}, {} {} for Duties: {}",
					getClass().getSimpleName(), shard.getShardID(), DETACH, CONFIRMED, 
					ShardEntity.toStringIds(sortedLog));
		}
		return found;
	}
}
