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
import static io.tilt.minka.domain.EntityEvent.REMOVE;
import static io.tilt.minka.domain.EntityState.COMMITED;
import static io.tilt.minka.domain.EntityState.DANGLING;
import static io.tilt.minka.domain.EntityState.MISSING;
import static java.util.Collections.emptyMap;

import java.time.Instant;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.function.BiConsumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.tilt.minka.core.leader.data.CommitedState;
import io.tilt.minka.core.leader.data.Scheme;
import io.tilt.minka.core.leader.data.UncommitedChanges;
import io.tilt.minka.core.leader.distributor.ChangeDetector;
import io.tilt.minka.core.leader.distributor.ChangePlan;
import io.tilt.minka.core.leader.distributor.Delivery;
import io.tilt.minka.core.task.Scheduler;
import io.tilt.minka.domain.CommitTree.Log;
import io.tilt.minka.domain.EntityEvent;
import io.tilt.minka.domain.EntityRecord;
import io.tilt.minka.domain.EntityState;
import io.tilt.minka.domain.Heartbeat;
import io.tilt.minka.domain.ShardEntity;
import io.tilt.minka.shard.Shard;
import io.tilt.minka.shard.ShardState;
import io.tilt.minka.shard.Transition;
/**
 * Single-point of write access to the {@linkplain CommitedState}
 * Watches follower's heartbeats taking action on any update
 * Beats with changes are delegated to a {@linkplain ChangeDetector} 
 * Anomalies and CRUD ops. are recorded into {@linkplain UncommitedChanges}
 * 
 * @author Cristian Gonzalez
 * @since Jan 4, 2016
 */
public class StateSentry implements BiConsumer<Heartbeat, Shard> {

	private final Logger logger = LoggerFactory.getLogger(getClass());
	private final String classname = getClass().getSimpleName();
	
	private final Scheme scheme;
	private final ChangeDetector changeDetector;
	
	StateSentry(final Scheme scheme, final Scheduler scheduler) {
		this.scheme = scheme;
		this.changeDetector = new ChangeDetector(scheme);
	}

	/**
	 * Mission: watch Follower's heartbeats for changes.
	 */
	@Override
	public void accept(final Heartbeat beat, final Shard shard) {
		if (beat.getShardChange() != null) {
			logger.info("{}: ShardID: {} changes to: {}", classname, shard, beat.getShardChange());
   			shardStateTransition(shard, shard.getState(), beat.getShardChange());
			if (beat.getShardChange().getState() == ShardState.QUITTED) {
				return;
			}
		}	
		// proctor will need it
		shard.enterHeartbeat(beat);
		
		// contrast current change-plan deliveries and discover new answers
		detectExpectedChanges(shard, beat);

		// look for problems
		if ((beat.reportsDuties()) && shard.getState().isAlive()) {
			detectUnexpectedChanges(shard, beat.getCaptured());
			if (scheme.getCurrentPlan()!=null) {
				//detectInvalidSpots(shard, beat.getCaptured());
			}
		}
		beat.clear();
	}
	
	private void detectExpectedChanges(final Shard shard, final Heartbeat beat) {
		final ChangePlan changePlan = scheme.getCurrentPlan();
		if (changePlan!=null && !changePlan.getResult().isClosed()) {
			
			final Delivery delivery = changePlan.getDelivery(shard);
			if (delivery!=null && delivery.getStep()==Delivery.Step.PENDING) {
				final Map<EntityEvent, StringBuilder> logg = new HashMap<>(EntityEvent.values().length);				
				if (changeDetector.detect(
						delivery, 
						changePlan.getId(), 
						beat, 
						shard.getShardID(), 
						(log, del)-> commit(shard, log, del, logg),
						EntityEvent.ATTACH, EntityEvent.STOCK, EntityEvent.DROP)) { 
					delivery.calculateState(logger::info);
				}
				logging(shard, changePlan, logg);
			} else if (logger.isDebugEnabled()) {
				logger.debug("{}: no {} Delivery for heartbeat's shard: {}", getClass().getSimpleName(), 
						Delivery.Step.PENDING, shard.getShardID().toString());
			}			
		} else if (isLazyOrSurvivor(beat, changePlan)) {
			scheme.getLearningState().learn(beat.getCaptured(), shard);
		}
	}

	/**
	 * @return TRUE if we must consider heartbeat reports as part of previous state
	 */
	private boolean isLazyOrSurvivor(final Heartbeat beat, final ChangePlan changePlan) {
		final boolean survivor = changePlan == null && beat.reportsDuties();
		boolean lazy = false;
		if (changePlan != null && changePlan.getResult().isClosed() && beat.reportsDuties()) {
			long max = 0;
			for (EntityRecord er : beat.getCaptured()) {
				final Log l = er.getCommitTree().findOne(0, beat.getShardId(), EntityEvent.ATTACH);
				if (l != null && max < l.getPlanId()) {
					max = l.getPlanId();
				}
			}
			lazy = max < scheme.getFirstPlanId();
			// wait only 1 change-plan for the lazy followers
			if (lazy && changePlan.getId() != scheme.getFirstPlanId()) {
				lazy = false;
				logger.error("{}: follower out of range for survivor: {}", getClass().getSimpleName(), beat.getShardId());
			}
		}
		return lazy || survivor;
	}

	private void logging(final Shard shard, final ChangePlan changePlan, final Map<EntityEvent, StringBuilder> logg) {
		if (logg.size()>0 && logger.isInfoEnabled()) {
			logg.entrySet().forEach(e-> logger.info("{}: Written expected change {} at [{}] on: {}", 
					getClass().getSimpleName(), e.getKey().name(), shard, e.getValue().toString()));
		}
		
		if (changePlan.getResult().isClosed()) {
			if (logger.isInfoEnabled()) {
				logger.info("{}: ChangePlan finished ! (promoted) duration: {}ms", classname, 
						System.currentTimeMillis()-changePlan.getCreation().toEpochMilli());
			}
		} else if (logger.isInfoEnabled() && changePlan.hasUnlatched()) {
			logger.info("{}: ChangePlan unlatched, fwd >> distributor agent ", classname);
			//scheduler.forward(scheduler.get(Semaphore.Action.DISTRIBUTOR));
		}
	}

	private void commit(final Shard shard, final Log changelog, final ShardEntity entity, 
			final Map<EntityEvent, StringBuilder> logging) {
		final Runnable r = ()-> {
			if (logger.isInfoEnabled()) {
				StringBuilder sb = logging.get(changelog.getEvent());
				if (sb==null) {
					logging.put(changelog.getEvent(), sb = new StringBuilder());
				}
				sb.append(entity.getEntity().getId()).append(',');
			}

			// copy the found situation to the instance we care
			entity.getCommitTree().addEvent(changelog.getEvent(),
					COMMITED,
					shard.getShardID(),
					changelog.getPlanId());
			};
			
		if (scheme.getCommitedState().commit(entity, shard, changelog.getEvent(), r)
			&& changelog.getEvent().getType()==EntityEvent.Type.ALLOC) {
			clearUncommited(changelog, entity, shard);
		}
		// REMOVES go this way:
		if (changelog.getEvent()==EntityEvent.DETACH) {
			final Log previous = entity.getCommitTree().getPreviousLog(shard.getShardID().getId());
			if (previous!=null && previous.getEvent()==EntityEvent.REMOVE) {
				scheme.getCommitedState().commit(entity, shard, previous.getEvent(), ()->{
					logger.info("{}: Removing duty at request: {}", classname, entity);
				});
			}
		}
	}

	private void clearUncommited(final Log changelog, final ShardEntity entity, Shard shard) {
		// remove it from the stage
		final ShardEntity crud = scheme.getUncommited().getCrudByDuty(entity.getDuty());
		if (crud!=null) {
			try {
			for (Log f: crud.getCommitTree().findAll(shard.getShardID())) {
				final Instant lastEventOnCrud = f.getHead().toInstant();
				boolean previousThanCrud = changelog.getHead().toInstant().isBefore(lastEventOnCrud);
				// if the update corresponds to the last CRUD OR they're both the same event (duplicated operation)
				if (!previousThanCrud || changelog.getEvent().getRootCause()==crud.getLastEvent()) {
					if (!scheme.getUncommited().removeCrud(entity)) {
						logger.warn("{} Backstage CRUD didnt existed: {}", classname, entity);
					}
				} else {
					logger.warn("{}: Avoiding UncommitedChanges remove (diff & after last event: {})", 
							classname, entity, previousThanCrud);
				}
			}
			} catch (Exception e) {
				e.printStackTrace();
				
			}
		} else {
			// they were not crud: (dangling/missing/..)
		}
	}
	
	/*this checks partition table looking for missing duties (not declared dangling, that's diff) */
	private void detectUnexpectedChanges(final Shard shard, final List<EntityRecord> reportedDuties) {
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

	void shardStateTransition(final Shard shard, final ShardState prior, final Transition transition) {
		shard.applyChange(transition);
		scheme.getCommitedState().stealthChange(true);		
		switch (transition.getState()) {
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
		final Collection<ShardEntity> dangling = scheme.getCommitedState().getDutiesByShard(shard);
		if (logger.isInfoEnabled()) {
			logger.info("{}: Removing fallen Shard: {} from ptable. Saving: #{} duties: {}", classname, shard,
				dangling.size(), ShardEntity.toStringIds(dangling));
		}
		for (ShardEntity e: dangling) {
			if (scheme.getUncommited().addDangling(e)) {
				e.getCommitTree().addEvent(
						DETACH, 
						COMMITED, 
						shard.getShardID(), 
						e.getCommitTree().getLast().getPlanId());
			}
		}
		scheme.getCommitedState().removeShard(shard);		
	}


}
