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

import io.tilt.minka.api.Pallet;
import io.tilt.minka.core.leader.data.CommitedState;
import io.tilt.minka.core.leader.data.ShardingState;
import io.tilt.minka.core.leader.data.UncommitedChanges;
import io.tilt.minka.core.leader.distributor.ChangeDetector;
import io.tilt.minka.core.leader.distributor.ChangePlan;
import io.tilt.minka.core.leader.distributor.Delivery;
import io.tilt.minka.core.task.Scheduler;
import io.tilt.minka.domain.EntityEvent;
import io.tilt.minka.domain.EntityJournal.Log;
import io.tilt.minka.domain.EntityRecord;
import io.tilt.minka.domain.EntityState;
import io.tilt.minka.domain.Heartbeat;
import io.tilt.minka.domain.ShardEntity;
import io.tilt.minka.shard.ShardCapacity;
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
	
	private final ShardingState shardingState;
	private final ChangeDetector changeDetector;
	
	public StateSentry(final ShardingState shardingState, final Scheduler scheduler) {
		this.shardingState = shardingState;
		this.changeDetector = new ChangeDetector(shardingState);
	}

	/**
	 * this's raw from Broker's message reception
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
	
		shard.enterHeartbeat(beat);
		final Map<Pallet, ShardCapacity> cap = beat.getCapacities();
		if (cap!=null) {
			shard.setCapacities(new HashMap<>(cap));
		}
		
		detectExpectedChanges(shard, beat);

		if ((beat.reportsDuties()) && shard.getState().isAlive()) {
			detectUnexpectedChanges(shard, beat.getCaptured());
			if (shardingState.getCurrentPlan()!=null) {
				detectInvalidShards(shard, beat.getCaptured());
			}
		}
		beat.clear();
	}
	
	private void detectExpectedChanges(final Shard shard, final Heartbeat beat) {
		final ChangePlan changePlan = shardingState.getCurrentPlan();
		if (changePlan!=null && !changePlan.getResult().isClosed()) {
			
			final Delivery delivery = changePlan.getDelivery(shard);
			if (delivery!=null && delivery.getStep()==Delivery.Step.PENDING) {
				final Map<EntityEvent, StringBuilder> logg = new HashMap<>(EntityEvent.values().length);
				final BiConsumer<Log, ShardEntity> committer = (log, del)-> commit(shard, log, del, logg);
				
				if (changeDetector.detect(delivery, changePlan.getId(), beat, shard.getShardID(), committer,
						EntityEvent.ATTACH, EntityEvent.STOCK, EntityEvent.DROP)) { 
					delivery.calculateState(logger::info);
				}
				
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
			} else if (logger.isDebugEnabled()) {
				logger.debug("{}: no {} Delivery for heartbeat's shard: {}", getClass().getSimpleName(), 
						Delivery.Step.PENDING, shard.getShardID().toString());
			}			
		} else if (changePlan == null && beat.reportsDuties()) {
			// changePlan is only NULL before 1st distribution
			// there's been a change of leader: i'm initiating with older followers
			final StringBuilder log = new StringBuilder();
			for (EntityRecord e: beat.getCaptured()) {
				//L: prepared, L: pending, F: received, C: confirmed, L: ack.
				boolean learnt = shardingState.getUncommited().learnPreviousDistribution(e, shard);
				if (learnt && logger.isInfoEnabled()) {
					log.append(e.getId()).append(',');
				}
			}
			if (log.length()>0) {
				logger.info("{}: Learnt at [{}] {}", getClass().getSimpleName(), shard, log.toString());
			}
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
			entity.getJournal().addEvent(changelog.getEvent(),
					COMMITED,
					shard.getShardID(),
					changelog.getPlanId());
			};
			
		if (shardingState.getCommitedState().commit(entity, shard, changelog.getEvent(), r)) {
			// && changelog.getEvent().getType()==EntityEvent.Type.ALLOCATION) {
			clearUncommited(changelog, entity);
		}
		// REMOVES go this way:
		if (changelog.getEvent()==EntityEvent.DETACH) {
			final Log previous = entity.getJournal().descendingIterator().next();
			if (previous.getEvent()==EntityEvent.REMOVE) {
				shardingState.getCommitedState().commit(entity, shard, previous.getEvent(), ()->{
					logger.info("{}: Removing duty at request: {}", classname, entity);
				});
			}
		}
	}

	private void clearUncommited(final Log changelog, final ShardEntity entity) {
		// remove it from the stage
		final ShardEntity crud = shardingState.getUncommited().getCrudByDuty(entity.getDuty());
		if (crud!=null) {
			final Instant lastEventOnCrud = crud.getJournal().getLast().getHead().toInstant();
			boolean previousThanCrud = changelog.getHead().toInstant().isBefore(lastEventOnCrud);
			// if the update corresponds to the last CRUD OR they're both the same event (duplicated operation)
			if (!previousThanCrud || changelog.getEvent().getRootCause()==crud.getLastEvent()) {
				if (!shardingState.getUncommited().removeCrud(entity)) {
					logger.warn("{} Backstage CRUD didnt existed: {}", classname, entity);
				}
			} else {
				logger.warn("{}: Avoiding UncommitedChanges removal of CRUD as it's different and after the last event", 
						classname, entity);
			}
		} else {
			// they were not crud: (dangling/missing/..)
		}
	}
	
	/*this checks partition table looking for missing duties (not declared dangling, that's diff) */
	private void detectUnexpectedChanges(final Shard shard, final List<EntityRecord> reportedDuties) {
		final Set<Entry<EntityState, List<ShardEntity>>> entrySet = findAbsent(shard, reportedDuties).entrySet();
		StringBuilder log = new StringBuilder();
		
		for (Map.Entry<EntityState, List<ShardEntity>> e: entrySet) {
			boolean done = false;
			if (e.getKey()==DANGLING) { 
				done = shardingState.getUncommited().addDangling(e.getValue());
			} else {
				done = shardingState.getUncommited().addMissing(e.getValue());
			}
			// copy the event so it's marked for later consideration
			if (done) {
				if (logger.isInfoEnabled()) {
					log.append(ShardEntity.toStringIds(e.getValue()));
				}
				e.getValue().forEach(d->shardingState.getCommitedState().commit(d, shard, REMOVE, ()->{
					d.getJournal().addEvent(
							d.getLastEvent(),
							e.getKey()==DANGLING ? DANGLING : MISSING, 
							null, // the last shard id 
							d.getJournal().getLast().getPlanId());
				}));
			}
		}
		if (log.length()>0) {
			logger.info("{}: Written unexpected absents ({}) at [{}] on: {}", getClass().getSimpleName(), 
					REMOVE.name(), shard, EntityRecord.toStringIds(reportedDuties));
		}
	}

	private Map<EntityState, List<ShardEntity>> findAbsent(final Shard shard, final List<EntityRecord> reportedDuties) {
		Map<EntityState, List<ShardEntity>> lost = null;
		for (final ShardEntity duty : shardingState.getCommitedState().getDutiesByShard(shard)) {
			boolean found = false;
			boolean foundAsDangling = false;
			for (EntityRecord reportedDuty : reportedDuties) {
				if (duty.getEntity().getId().equals(reportedDuty.getId())) {
					final EntityState lastState = reportedDuty.getLastState();
					switch (lastState) {
					case COMMITED:
					case FINALIZED:
						found = true;
						break;
					case DANGLING:
						foundAsDangling = true;
						break;
					default:
						logger.error("{}: Follower beated a duty {} when in scheme is: {}",
								classname, reportedDuty.getLastState(), duty.getLastState());
						found = true;
						break;
					}
					break;
				}
			}
			if (!found) {
				List<ShardEntity> l;
				if (lost == null) {
					lost = new HashMap<>();
				}
				final EntityState k = foundAsDangling ? DANGLING : MISSING;
				if ((l = lost.get(k))==null) {
					lost.put(k, l = new LinkedList<>());
				}
				l.add(duty);
			}
		}
		if (lost!=null) {
			logger.error("{}: ShardID: {}, absent duties in Heartbeat: {},{} (backing up to stage)",
				getClass().getSimpleName(), shard.getShardID(), ShardEntity.toStringIds(lost.get(DANGLING)), 
				ShardEntity.toStringIds(lost.get(MISSING)));
		}			
		return lost !=null ? lost : emptyMap();
	}
	

	private void detectInvalidShards(final Shard sourceShard, final List<EntityRecord> reportedCapturedDuties) {
		for (final EntityRecord e : reportedCapturedDuties) {
			if (e.getJournal().getLast().getEvent().getType()==EntityEvent.Type.ALLOCATION) {
				final Shard should = shardingState.getCommitedState().findDutyLocation(e.getId());
				if (should==null) {
					logger.error("unexisting duty: " + e.toString() + " reported by shard: " + sourceShard);
				} else if (!should.equals(sourceShard)) {
					logger.error("relocated duty: " + e.toString() + " being reported by shard: " + sourceShard);
				}
			}
		}
	}

	public void shardStateTransition(final Shard shard, final ShardState prior, final Transition transition) {
		shard.applyChange(transition);
		shardingState.getCommitedState().stealthChange(true);		
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
		final Collection<ShardEntity> dangling = shardingState.getCommitedState().getDutiesByShard(shard);
		if (logger.isInfoEnabled()) {
			logger.info("{}: Removing fallen Shard: {} from ptable. Saving: #{} duties: {}", classname, shard,
				dangling.size(), ShardEntity.toStringIds(dangling));
		}
		for (ShardEntity e: dangling) {
			if (shardingState.getUncommited().addDangling(e)) {
				e.getJournal().addEvent(
						DETACH, 
						COMMITED, 
						shard.getShardID(), 
						e.getJournal().getLast().getPlanId());
			}
		}
		shardingState.getCommitedState().removeShard(shard);		
	}


}
