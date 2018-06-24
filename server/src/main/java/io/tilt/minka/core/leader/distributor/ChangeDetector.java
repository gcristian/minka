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

import io.tilt.minka.core.leader.data.ShardingScheme;
import io.tilt.minka.domain.EntityJournal.Log;
import io.tilt.minka.domain.EntityState;
import io.tilt.minka.domain.Heartbeat;
import io.tilt.minka.domain.Shard;
import io.tilt.minka.domain.ShardEntity;
import io.tilt.minka.domain.ShardIdentifier;
import io.tilt.minka.domain.EntityRecord;

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
	
	private final ShardingScheme shardingScheme;
	
	/** solely instance */
	public ChangeDetector(final ShardingScheme shardingScheme) {
		this.shardingScheme = shardingScheme;
	}

	/** Finds sharding changes by matching entity journals for planned reallocations */
	public boolean findPlannedChanges(
			final Delivery delivery, 
			final long pid, 
			final Heartbeat beat, 
			final ShardIdentifier sourceid,
			final BiConsumer<Log, ShardEntity> bicons) {
		long lattestPlanId = 0;
		boolean found = false;
		if (beat.reportsDuties()) {
			lattestPlanId = latestPlan(beat);
			if (lattestPlanId==pid) {
				found|=onFoundAttachments(
						sourceid, 
						beat.getReportedCapturedDuties(), 
						delivery.getDuties(), 
						bicons, 
						pid);
			}
		}
		found|=onFoundDetachments(
				sourceid, 
				beat.getReportedCapturedDuties(), 
				delivery.getDuties(), 
				bicons, 
				pid);

		if (lattestPlanId<pid && !found) {
			// delivery found, no change but expected ? 
			logger.warn("{}: Ignoring shard's ({}) heartbeats with a previous Journal: {} (current: {})", 
				getClass().getSimpleName(), sourceid.toString(), lattestPlanId, pid);
		}
		return found;
	}

	/* latest entity's journal plan ID among the beat */
	private static long latestPlan(final Heartbeat beat) {
		long lattestPlanId = 0;
		for (EntityRecord e: beat.getReportedCapturedDuties()) {
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
			final ShardIdentifier shardid, 
			final List<EntityRecord> beatedDuties,
			final List<ShardEntity> deliveryDuties,
			final BiConsumer<Log, ShardEntity> c,
			final long pid) {
		Set<EntityRecord> sortedLogConfirmed = null;
		Set<EntityRecord> sortedLogDirty = null;
		boolean found = false;
		for (final EntityRecord beated : beatedDuties) {
			for (ShardEntity delivered : deliveryDuties) {
				if (delivered.getEntity().getId().equals(beated.getId())) {
					final Log expected = findConfirmationPair(beated, delivered, shardid, pid);
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
					shardid, ATTACH, CONFIRMED, 
					EntityRecord.toStringIds(sortedLogConfirmed));
		}
		if (sortedLogDirty!=null) {
			logger.warn("{}: ShardID: {}, Reporting DIRTY partition event for Duties: {}", classname,
					shardid, EntityRecord.toStringIds(sortedLogDirty));
		}
		return found;
	}

	/** @return the expected delivered event matching the beated logs */
	private Log findConfirmationPair(
			final EntityRecord beated,
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
						final Shard location = shardingScheme.getScheme().findDutyLocation(delivered.getDuty());
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
			final ShardIdentifier shardid,
			final List<EntityRecord> beatedDuties,
			final List<ShardEntity> deliveryDuties,
			final BiConsumer<Log, ShardEntity> c, 
			final long pid) {
		Set<ShardEntity> sortedLog = null;
		boolean found = false;
		for (ShardEntity prescripted : deliveryDuties) {
			if (!beatedDuties.stream()
				.filter(r->r.getId().equals(prescripted.getEntity().getId()))
				.findFirst().isPresent()) {
				final Log changelog = prescripted.getJournal().find(pid, shardid, DETACH, REMOVE);
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
					getClass().getSimpleName(), shardid, DETACH, CONFIRMED, 
					ShardEntity.toStringIds(sortedLog));
		}
		return found;
	}
}
