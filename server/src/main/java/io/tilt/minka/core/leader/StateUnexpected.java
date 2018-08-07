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

import static io.tilt.minka.domain.EntityEvent.REMOVE;
import static io.tilt.minka.domain.EntityState.DANGLING;
import static io.tilt.minka.domain.EntityState.MISSING;
import static java.util.Collections.emptyMap;

import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.tilt.minka.core.leader.data.Scheme;
import io.tilt.minka.domain.CommitTree.Log;
import io.tilt.minka.domain.EntityEvent;
import io.tilt.minka.domain.EntityRecord;
import io.tilt.minka.domain.EntityState;
import io.tilt.minka.domain.ShardEntity;
import io.tilt.minka.shard.Shard;

/**
 * Detect any inconsistency comming within the Heartbeat
 */
class StateUnexpected {

	private final Logger logger = LoggerFactory.getLogger(getClass());
	private final String classname = getClass().getSimpleName();
	private final Scheme scheme;
	
	StateUnexpected(final Scheme scheme) {
		this.scheme = scheme;
	}
	
	/*this checks partition table looking for missing duties (not declared dangling, that's diff) */
	void detect(final Shard shard, final List<EntityRecord> reportedDuties) {
		final Set<Entry<EntityState, List<ShardEntity>>> entrySet = findLost(shard, reportedDuties).entrySet();
		StringBuilder log = new StringBuilder();
		
		for (Map.Entry<EntityState, List<ShardEntity>> e: entrySet) {
			boolean uncommitted = false;
			if (e.getKey()==DANGLING) { 
				uncommitted = scheme.getUncommited().addDangling(e.getValue());
			} else if (e.getKey()==MISSING) {
				uncommitted = scheme.getUncommited().addMissing(e.getValue());
			} else {
				logger.warn("{} From ({}) comes Unexpected state {} for duty {}", getClass().getSimpleName(), 
						shard, e.getKey(), ShardEntity.toStringBrief(e.getValue()));
			}
			// copy the event so it's marked for later consideration
			if (uncommitted) {
				if (logger.isInfoEnabled()) {
					log.append(ShardEntity.toStringIds(e.getValue()));
				}
				e.getValue().forEach(d->scheme.getCommitedState().commit(d, shard, REMOVE, ()->{
					d.getCommitTree().addEvent(
							d.getLastEvent(),
							e.getKey(), 
							"N/A", // the last shard id 
							d.getCommitTree().getLast().getPlanId());
				}));
			}
		}
		if (log.length()>0) {
			logger.info("{}: Written unexpected absents ({}) at [{}] on: {}", getClass().getSimpleName(), 
					REMOVE.name(), shard, EntityRecord.toStringIds(reportedDuties));
		}
		if (scheme.getCurrentPlan()!=null) {
			detectInvalidSpots(shard, reportedDuties);
		}
	}

	private Map<EntityState, List<ShardEntity>> findLost(final Shard shard, final List<EntityRecord> reportedDuties) {
		Map<EntityState, List<ShardEntity>> lost = null;
		for (final ShardEntity committed : scheme.getCommitedState().getDutiesByShard(shard)) {
			boolean found = false;
			EntityState wrongState = null;
			for (EntityRecord reported : reportedDuties) {
				if (committed.getEntity().getId().equals(reported.getId())) {
					found = true;
					wrongState = lookupWrongState(shard, committed, reported);
					break;
				}
			}
			if (!found || wrongState!=null) {
				if (lost == null) {
					lost = new HashMap<>();
				}
				final EntityState k = !found? MISSING : wrongState;
				List<ShardEntity> list = lost.get(k);
				if (list==null) {
					lost.put(k, list = new LinkedList<>());
				}
				list.add(committed);
			}
		}
		if (lost!=null) {
			logger.error("{}: ShardID: {}, absent duties in Heartbeat: {},{} (backing up to stage)",
				getClass().getSimpleName(), shard.getShardID(), ShardEntity.toStringIds(lost.get(DANGLING)), 
				ShardEntity.toStringIds(lost.get(MISSING)));
		}			
		return lost !=null ? lost : emptyMap();
	}

	/** @return NULL for Correct state or NO Match */
	private EntityState lookupWrongState(
			final Shard shard, 
			final ShardEntity committed, 
			final EntityRecord reported) {
		
		for (final Log r: reported.getCommitTree().findAll(shard.getShardID())) {
			for (final Log c: committed.getCommitTree().findAll(shard.getShardID())) {
				if (c.getEvent()==r.getEvent()) {
					// commited-state truth is only on ATTACH and STOCK...
					if (c.getEvent() == EntityEvent.ATTACH || c.getEvent()==EntityEvent.STOCK) {
						final EntityState stateForThatShard = r.getLastState();
						switch (stateForThatShard) {
						case COMMITED:
						case FINALIZED:
							return null;
						default:
							logger.error("{}: Follower beated duty {} as {} when in scheme is: {}",
									classname, reported.getId(), r.getLastState(), committed.getLastState());
							return stateForThatShard;
						}
					}
				}
			}
		}
		return null;
	}
	
	private void detectInvalidSpots(final Shard sourceShard, final List<EntityRecord> reportedCapturedDuties) {
		for (final EntityRecord e : reportedCapturedDuties) {
			for (final Log log: e.getCommitTree().findAll(sourceShard.getShardID())) {
				final boolean commited = log.getLastState()==EntityState.COMMITED;
				if (log.getEvent()==EntityEvent.ATTACH && commited) {
					checkAttachExistance(sourceShard, e);
				} else if (log.getEvent()==EntityEvent.STOCK && commited) {
					checkReplicaExistance(sourceShard, e);
				}
			}
		}
	}

	/** only log */
	private void checkAttachExistance(final Shard sourceShard, final EntityRecord e) {
		final Shard should = scheme.getCommitedState().findDutyLocation(e.getId());
		if (should==null) {
			logger.error("{}: Non-attached duty: {} reported by shard {} ", classname, e.toString(), sourceShard);
		} else if (!should.equals(sourceShard)) {
			logger.error("{}: Relocated? duty: {} reported by shard {} ", classname, e.toString(), sourceShard);
		}
	}

	/** only log */
	private void checkReplicaExistance(final Shard sourceShard, final EntityRecord e) {
		final Collection<ShardEntity> replicas = scheme.getCommitedState().getReplicasByShard(sourceShard);
		boolean found = false;
		if (e.getEntity()!=null) {
			found = replicas.contains(e.getEntity());
		} else {
			for (ShardEntity r: replicas) {
				if (found|=r.getDuty().getId().equals(e.getId())) {
					break;
				}
			}
		}
		if (!found) {
			logger.error("{}: Non-stocked duty: {} reported by shard {} ", classname, e.toString(), sourceShard);							
		}
	}

}
