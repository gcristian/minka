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

import java.time.Instant;
import java.util.Date;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import org.apache.logging.log4j.util.BiConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.tilt.minka.core.leader.PartitionTable;
import io.tilt.minka.domain.EntityEvent;
import io.tilt.minka.domain.EntityJournal.Log;
import io.tilt.minka.domain.EntityState;
import io.tilt.minka.domain.Heartbeat;
import io.tilt.minka.domain.Shard;
import io.tilt.minka.domain.Shard.ShardState;
import io.tilt.minka.domain.ShardEntity;
import io.tilt.minka.domain.ShardIdentifier;

/**
 * Inspects {@linkplain Heartbeat}'s looking for expected 
 * changes according the current distribution {@linkplain Plan}
 *  
 * @author Cristian Gonzalez
 * @since Mar 7, 2018
 */
public class SchemeWriter {

	private static final int MAX_EVENT_DATE_FOR_DIRTY = 10000;
	
	private final Logger logger = LoggerFactory.getLogger(getClass());
	
	private final PartitionTable partitionTable;
	
	/** solely instance */
	public SchemeWriter(final PartitionTable partitionTable) {
		this.partitionTable = partitionTable;
	}

	/** @return true if the scheme wrote the change */
	private boolean write(final ShardEntity entity, final Log log, final Shard source, final long planId) {
		entity.getJournal().addEvent(log.getEvent(),
				EntityState.CONFIRMED,
				source.getShardID(),
				planId);
		return partitionTable.getScheme().writeDuty(entity, source, log.getEvent());
	}

	/** match entity journals for planned changes and write to the scheme */
	public void detectChanges(final Delivery delivery, final Plan plan, final Heartbeat beat, final Shard source) {
		long lattestPlanId = 0;
		boolean changed[] = new boolean[1];
		if (beat.reportsDuties()) {
			lattestPlanId = latestPlan(beat);
			if (lattestPlanId==plan.getId()) {
				onFoundAttachments(source, beat.getReportedCapturedDuties(), delivery,
					(log, entity) -> {
						if (write(entity, log, source, plan.getId())) {
							// TODO warn: if a remove comes from client while a running plan, we might be
							// avoidint it
							partitionTable.getBackstage().removeCrud(entity);
							changed[0] |= true;
						}
					});
			}
		}
		onFoundDetachments(source, beat.getReportedCapturedDuties(), delivery,
			(log, entity) -> {
				changed[0] |= write(entity, log, source, plan.getId());
				if (changed[0] && log.getEvent().isCrud()) {
					// remove it from the backstage
					for (ShardEntity duty : partitionTable.getBackstage().getDutiesCrud()) {
						if (duty.equals(entity)) {
							final Instant lastEventOnCrud = duty.getJournal().getLast().getHead().toInstant();
							if (log.getHead().toInstant().isAfter(lastEventOnCrud)) {
								partitionTable.getBackstage().removeCrud(entity);
								break;
							}
						}
					}
				}
			});
		if (changed[0]) {
			delivery.checkState();
		} else if (lattestPlanId==plan.getId()) {
			// delivery found, no change but expected ? 
			logger.warn("{}: Ignoring shard's ({}) heartbeats with a previous Journal: {} (current: {})", 
					getClass().getSimpleName(), source.getShardID().toString(), lattestPlanId, plan.getId());			
		}
		if (!plan.getResult().isClosed() && plan.hasUnlatched()) {
			if (logger.isInfoEnabled()) {
				logger.info("{}: Plan unlatched, fwd >> distributor agent ", getClass().getSimpleName());
			}
			// scheduler.forward(scheduler.get(Semaphore.Action.DISTRIBUTOR));
		}
		
		if (plan.getResult().isClosed() && logger.isInfoEnabled()) {
			logger.info("{}: Plan finished ! (all changes in scheme)", getClass().getSimpleName());
		}
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
	private void onFoundAttachments(
			final Shard shard, 
			final List<ShardEntity> beatedDuties, 
			final Delivery delivery,
			final BiConsumer<Log, ShardEntity> c) {
		Set<ShardEntity> sortedLogConfirmed = null;
		Set<ShardEntity> sortedLogDirty = null;
		for (final ShardEntity beated : beatedDuties) {
			for (ShardEntity delivered : delivery.getDuties()) {
				if (delivered.equals(beated)) {
					final Log expected = findConfirmationPair(beated, delivered, shard.getShardID());
					if (expected != null) {
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
			logger.info("{}: ShardID: {}, Confirming partition event for Duties: {}", getClass().getSimpleName(),
					shard.getShardID(), ShardEntity.toStringIds(sortedLogConfirmed));
		}
		if (sortedLogDirty!=null) {
			logger.warn("{}: ShardID: {}, Reporting DIRTY partition event for Duties: {}", getClass().getSimpleName(),
					shard.getShardID(), ShardEntity.toStringIds(sortedLogDirty));
		}
	}

	/** @return the expected delivered event matching the beated logs */
	private Log findConfirmationPair(
			final ShardEntity beated,
			final ShardEntity delivered,
			final ShardIdentifier shardid) {
		Log ret = null;
		// beated duty must belong to current non-expired plan
		final long pid = partitionTable.getCurrentPlan().getId();
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
						final Shard location = partitionTable.getScheme().getDutyLocation(delivered.getDuty());
						if (location==null || !location.getShardID().equals(shardid)) {
							ret = deliLog;	
						}
					} else {
						logger.error("{}: confirmation pair found erroneous state: {} on duty: {}",
								getClass().getSimpleName(), deliState, delivered);
					}
				}
			} else {
				logger.error("{}: Reporting state: {} on Duty: {}", getClass().getSimpleName(), beatedState);
			}
		}
		return ret;
	}
	
	/** treat un-coming as detaches
	/** @return whether or not there was an absence confirmed 
	 * @param c */
	private void onFoundDetachments(
			final Shard shard,
			final List<ShardEntity> beatedDuties,
			final Delivery delivery,
			final BiConsumer<Log, ShardEntity> c) {
		Set<ShardEntity> sortedLog = null;
		final long pid = partitionTable.getCurrentPlan().getId();

		for (ShardEntity prescripted : delivery.getDuties()) {
			if (!beatedDuties.contains(prescripted)) {
				final Log found = prescripted.getJournal().find(pid, shard.getShardID(), EntityEvent.DETACH, EntityEvent.REMOVE);
				if (found!=null && (found.getLastState()==EntityState.PENDING || found.getLastState()==EntityState.MISSING)) {
					if (logger.isInfoEnabled()) {
						if (sortedLog==null) {
							sortedLog = new TreeSet<>();
						}
						sortedLog.add(prescripted);
					}
					c.accept(found, prescripted);
				}
			}
		}
		
		if (sortedLog!=null) {
			logger.info("{}: ShardID: {}, Confirming (by absence) partioning event for Duties: {}",
					getClass().getSimpleName(), shard.getShardID(), ShardEntity.toStringIds(sortedLog));
		}
	}

	public void shardStateChange(final Shard shard, final ShardState prior, final ShardState newState) {
		shard.setState(newState);
		logger.info("{}: ShardID: {} changes to: {}", getClass().getSimpleName(), shard, newState);
		switch (newState) {
		case GONE:
		case QUITTED:
			recoverAndRetire(shard);
			// distributor will decide plan obsolecy if it must
			break;
		case ONLINE:
			// TODO get ready
			break;
		case QUARANTINE:
			// TODO lot of consistency checks here on duties
			// to avoid chain of shit from heartbeats reporting doubtful stuff
			break;
		default:
			break;
		}
	}

	/*
	 * dangling duties are set as already confirmed, change wont wait for this
	 * to be confirmed
	 */
	private void recoverAndRetire(final Shard shard) {
		final Set<ShardEntity> dangling = partitionTable.getScheme().getDutiesByShard(shard);
		if (logger.isInfoEnabled()) {
			logger.info("{}: Removing fallen Shard: {} from ptable. Saving: #{} duties: {}", 
				getClass().getSimpleName(), shard, dangling.size(), ShardEntity.toStringIds(dangling));
		}
		partitionTable.getScheme().removeShard(shard);
		partitionTable.getBackstage().addDangling(dangling);
	}

}
