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
package io.tilt.minka.core.leader;

import static io.tilt.minka.domain.EntityEvent.DETACH;
import static io.tilt.minka.domain.EntityEvent.DROP;
import static io.tilt.minka.domain.EntityEvent.REMOVE;
import static io.tilt.minka.domain.EntityState.ACK;
import static io.tilt.minka.domain.EntityState.COMMITED;
import static io.tilt.minka.domain.EntityState.MISSING;
import static io.tilt.minka.domain.EntityState.PENDING;

import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.BiConsumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.tilt.minka.core.leader.data.Scheme;
import io.tilt.minka.core.leader.distributor.ChangePlan;
import io.tilt.minka.core.leader.distributor.Delivery;
import io.tilt.minka.domain.CommitTree.Log;
import io.tilt.minka.domain.EntityEvent;
import io.tilt.minka.domain.EntityRecord;
import io.tilt.minka.domain.EntityState;
import io.tilt.minka.domain.Heartbeat;
import io.tilt.minka.domain.ShardEntity;
import io.tilt.minka.shard.Shard;
import io.tilt.minka.shard.ShardIdentifier;

/**
 * Contains the mechanism and knowledge for determining reallocation changes <br>
 * Inspects {@linkplain Heartbeat}'s looking for expected 
 * changes according the current distribution {@linkplain ChangePlan}<br>
 *  
 * @author Cristian Gonzalez
 * @since Mar 7, 2018
 */
public class StateExpected {

	private static final int MAX_EVENT_DATE_FOR_DIRTY = 10000;
	
	private final Logger logger = LoggerFactory.getLogger(getClass());
	private final String classname = getClass().getSimpleName();
	
	private final Scheme scheme;
	
	/** solely instance */
	public StateExpected(final Scheme scheme) {
		this.scheme = scheme;
	}

	/** Finds sharding changes by matching entity journals for planned reallocations */
	public boolean detect(
			final Delivery delivery, 
			final long pid, 
			final Heartbeat beat, 
			final ShardIdentifier source,
			final BiConsumer<Log, ShardEntity> bicons,
			final EntityEvent...events) {
		long lattestPlanId = 0;
		boolean found = false;
		if (beat.reportsDuties()) {
			lattestPlanId = latestPlan(beat.getCaptured());
			if (lattestPlanId==pid) {
				found|=onFoundPairings(source, beat.getCaptured(), delivery.getDuties(), bicons, pid, events);
			}
		}
		found|=onFoundDetachments(source, beat.getCaptured(), delivery.getDuties(), bicons, pid);

		if (lattestPlanId<pid && !found) {
			// delivery found, no change but expected ? 
			logger.warn("{}: Ignoring shard's ({}) heartbeats with a previous Journal: {} (current: {})", 
				getClass().getSimpleName(), source.toString(), lattestPlanId, pid);
		}
		return found;
	}

	/* latest entity's journal plan ID among the beat */
	private static long latestPlan(final Collection<EntityRecord> ents) {
		long lattestPlanId = 0;
		for (EntityRecord e: ents) {
			final long pid = e.getCommitTree().getLast().getPlanId();
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
	private boolean onFoundPairings(
			final ShardIdentifier shardid, 
			final List<EntityRecord> beatedDuties,
			final List<ShardEntity> deliveryDuties,
			final BiConsumer<Log, ShardEntity> c,
			final long pid,
			final EntityEvent...events) {
		final Map<EntityEvent, Set<EntityRecord>> byEvent = new HashMap<>(events.length);
		Set<EntityRecord> dirty = null;
		boolean found = false;
		for (final EntityRecord beated : beatedDuties) {
			for (ShardEntity delivered : deliveryDuties) {
				if (delivered.getEntity().getId().equals(beated.getId())) {
					for (EntityEvent ee: events) {
						final Log expected = findConfirmationPair(beated, delivered, shardid, pid, ee);
						if (expected != null) {
							found = true;
							if (logger.isInfoEnabled()) {
								Set<EntityRecord> log = byEvent.get(ee);
								if (log == null) {
									byEvent.put(ee, log = new TreeSet<>());
								}
								log.add(beated);
							}
							c.accept(expected, delivered);
						} else {
							final Date fact = delivered.getCommitTree().getLast().getHead();
							final long now = System.currentTimeMillis();
							if (now - fact.getTime() > MAX_EVENT_DATE_FOR_DIRTY) {
								if (dirty == null) {
									dirty = new TreeSet<>();
								}
								dirty.add(beated);
							}
						}
					}
					break;
				}
			}
		}
		if (!byEvent.isEmpty()) {
			byEvent.entrySet().forEach(e->
				logger.info("{}: ShardID: {}, {} {} for Duties: {}", classname,
					shardid, e.getKey(), COMMITED, 
					EntityRecord.toStringIds(e.getValue())));
		}
		if (dirty!=null) {
			logger.warn("{}: ShardID: {}, Reporting DIRTY partition event for Duties: {}", classname,
					shardid, EntityRecord.toStringIds(dirty));
		}
		return found;
	}

	/** @return the expected delivered event matching the beated logs */
	private Log findConfirmationPair(
			final EntityRecord beated,
			final ShardEntity delivered,
			final ShardIdentifier shardid, 
			final long pid,
			final EntityEvent...events) {
		Log ret = null;
		// beated duty must belong to current non-expired plan
		final Log beatedLog = beated.getCommitTree().findOne(pid, shardid, events);
		if (beatedLog != null) {
			final EntityState beatedState = beatedLog.getLastState();
			if (beatedState == COMMITED || beatedState == ACK) {
				final Log deliLog = delivered.getCommitTree().findOne(pid, shardid, events);
				if (deliLog != null) {
					final EntityState deliState = deliLog.getLastState();
					if (deliState == PENDING || deliState !=COMMITED) {
						// expected normal situation
						ret = deliLog;
					} else if (deliState == COMMITED) {
						// when cluster unstable: bad state but possible 
						final Shard location = scheme.getCommitedState().findDutyLocation(delivered.getDuty());
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
		Map<EntityEvent, List<ShardEntity>> log = new HashMap<>(3);
		boolean found = false;
		for (ShardEntity prescripted : deliveryDuties) {
			if (!beatedDuties.stream()
				.filter(r->r.getId().equals(prescripted.getEntity().getId()))
				.findFirst().isPresent()) {
				for (final Log changelog : prescripted.getCommitTree().findAll(pid, shardid, DETACH, REMOVE, DROP)) {
					if (changelog.getLastState()==PENDING || changelog.getLastState()==MISSING) {
						found = true;
						logging(log, prescripted, changelog);
						c.accept(changelog, prescripted);
					}
				}
			}
		}
		
		if (log!=null) {
			log.entrySet().forEach(e->
				logger.info("{}: ShardID: {}, {} {} for Duties: {}",
					getClass().getSimpleName(), shardid, e.getKey(), COMMITED, 
					ShardEntity.toStringIds(e.getValue())));
		}
		return found;
	}

	private Map<EntityEvent, List<ShardEntity>> logging(Map<EntityEvent, List<ShardEntity>> log,
			ShardEntity prescripted, final Log changelog) {
		if (logger.isInfoEnabled()) {
			if (log==null) {
				log = new HashMap<>();
			}
			List<ShardEntity> list = log.get(changelog.getEvent());
			if (list==null) {
				log.put(changelog.getEvent(), list = new LinkedList<>());
			}
			list.add(prescripted);
		}
		return log;
	}
}
