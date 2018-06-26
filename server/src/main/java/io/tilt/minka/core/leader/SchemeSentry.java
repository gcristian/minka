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
import static io.tilt.minka.domain.EntityState.CONFIRMED;
import static io.tilt.minka.domain.EntityState.DANGLING;
import static io.tilt.minka.domain.EntityState.MISSING;
import static io.tilt.minka.domain.Shard.ShardState.QUITTED;
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
import io.tilt.minka.core.leader.data.Scheme;
import io.tilt.minka.core.leader.data.ShardingScheme;
import io.tilt.minka.core.leader.data.Stage;
import io.tilt.minka.core.leader.distributor.ChangeDetector;
import io.tilt.minka.core.leader.distributor.ChangePlan;
import io.tilt.minka.core.leader.distributor.Delivery;
import io.tilt.minka.core.task.Scheduler;
import io.tilt.minka.domain.Capacity;
import io.tilt.minka.domain.EntityEvent;
import io.tilt.minka.domain.EntityJournal.Log;
import io.tilt.minka.domain.EntityRecord;
import io.tilt.minka.domain.EntityState;
import io.tilt.minka.domain.Heartbeat;
import io.tilt.minka.domain.Shard;
import io.tilt.minka.domain.Shard.ShardState;
import io.tilt.minka.domain.ShardEntity;
/**
 * Single-point of write access to the {@linkplain Scheme}
 * Watches follower's heartbeats taking action on any update
 * Beats with changes are delegated to a {@linkplain ChangeDetector} 
 * Anomalies and CRUD ops. are recorded into {@linkplain Stage}
 * 
 * @author Cristian Gonzalez
 * @since Jan 4, 2016
 */
public class SchemeSentry implements BiConsumer<Heartbeat, Shard> {

	private final Logger logger = LoggerFactory.getLogger(getClass());
	private final String classname = getClass().getSimpleName();
	
	private final ShardingScheme shardingScheme;
	private final ChangeDetector changeDetector;
	
	public SchemeSentry(final ShardingScheme shardingScheme, final Scheduler scheduler) {
		this.shardingScheme = shardingScheme;
		this.changeDetector = new ChangeDetector(shardingScheme);
	}

	/**
	 * this's raw from Broker's message reception
	 */
	@Override
	public void accept(final Heartbeat beat, final Shard shard) {
		if (beat.getShardChange() != null) {
			logger.info("{}: ShardID: {} changes to: {}", classname, shard, beat.getShardChange());
   			shardStateChange(shard, shard.getState(), beat.getShardChange());
			if (beat.getShardChange().getState() == QUITTED) {
				return;
			}
		}
	
		shard.enterHeartbeat(beat);
		final Map<Pallet, Capacity> cap = beat.getCapacities();
		if (cap!=null) {
			shard.setCapacities(new HashMap<>(cap));
		}
		
		detectExpectedChanges(shard, beat);

		if ((beat.reportsDuties()) && shard.getState().isAlive()) {
			detectUnexpectedChanges(shard, beat.getReportedCapturedDuties());
			if (shardingScheme.getCurrentPlan()!=null) {
				detectInvalidShards(shard, beat.getReportedCapturedDuties());
			}
		}
		beat.clear();
	}
	
	private void detectExpectedChanges(final Shard shard, final Heartbeat beat) {
		final ChangePlan changePlan = shardingScheme.getCurrentPlan();
		if (changePlan!=null && !changePlan.getResult().isClosed()) {
			final Delivery delivery = changePlan.getDelivery(shard);
			if (delivery!=null && delivery.getStep()==Delivery.Step.PENDING) {
				final Map<EntityEvent, StringBuilder> logging = new HashMap<>(4);
				if (changeDetector.findPlannedChanges(delivery, changePlan.getId(), beat, shard.getShardID(), 
						(l,d)-> writesOnChange(shard, l, d, logging))) {
					delivery.calculateState(s->logger.info(s));
				}
				if (logging.size()>0 && logger.isInfoEnabled()) {
					logging.entrySet().forEach(e-> logger.info("{}: Written expected change {} at [{}] on: {}", 
							getClass().getSimpleName(), e.getKey().name(), shard, e.getValue().toString()));
				}
				changePlan.calculateState();
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
			for (EntityRecord e: beat.getReportedCapturedDuties()) {
				//L: prepared, L: pending, F: received, C: confirmed, L: ack.
				boolean learnt = shardingScheme.getScheme().learnPreviousDistribution(e, shard);
				if (learnt && logger.isInfoEnabled()) {
					log.append(e.getId()).append(',');
				}
			}
			if (log.length()>0) {
				logger.info("{}: Learnt at [{}] {}", getClass().getSimpleName(), shard, log.toString());
			}
		}
	}

	private void writesOnChange(final Shard shard, final Log changelog, final ShardEntity entity, 
			final Map<EntityEvent, StringBuilder> logging) {
		if (shardingScheme.getScheme().write(entity, shard, changelog.getEvent(), ()-> {
			if (logger.isInfoEnabled()) {
				StringBuilder sb = logging.get(changelog.getEvent());
				if (sb==null) {
					logging.put(changelog.getEvent(), sb = new StringBuilder());
				}
				sb.append(entity.getEntity().getId()).append(',');
			}

			// copy the found situation to the instance we care
			entity.getJournal().addEvent(changelog.getEvent(),
					CONFIRMED,
					shard.getShardID(),
					changelog.getPlanId());
			})) {
			updateStage(changelog, entity);
		}
		// REMOVES go this way:
		if (changelog.getEvent()==EntityEvent.DETACH) {
			final Log previous = entity.getJournal().descendingIterator().next();
			if (previous.getEvent()==EntityEvent.REMOVE) {
				shardingScheme.getScheme().write(entity, shard, previous.getEvent(), ()->{
					logger.info("{}: Removing duty at request: {}", classname, entity);
				});
			}
		}
	}

	private void updateStage(final Log changelog, final ShardEntity entity) {
		// remove it from the stage
		final ShardEntity crud = shardingScheme.getStage().getCrudByDuty(entity.getDuty());
		if (crud!=null) {
			final Instant lastEventOnCrud = crud.getJournal().getLast().getHead().toInstant();
			boolean previousThanCrud = changelog.getHead().toInstant().isBefore(lastEventOnCrud);
			// if the update corresponds to the last CRUD OR they're both the same event (duplicated operation)
			if (!previousThanCrud || changelog.getEvent().getRootCause()==crud.getLastEvent()) {
				if (!shardingScheme.getStage().removeCrud(entity)) {
					logger.warn("{} Backstage CRUD didnt existed: {}", classname, entity);
				}
			} else {
				logger.warn("{}: Avoiding Stage removal of CRUD as it's different and after the last event", 
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
				done = shardingScheme.getStage().addDangling(e.getValue());
			} else {
				done = shardingScheme.getStage().addMissing(e.getValue());
			}
			// copy the event so it's marked for later consideration
			if (done) {
				if (logger.isInfoEnabled()) {
					log.append(ShardEntity.toStringIds(e.getValue()));
				}
				e.getValue().forEach(d->shardingScheme.getScheme().write(d, shard, REMOVE, ()->{
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
		for (final ShardEntity duty : shardingScheme.getScheme().getDutiesByShard(shard)) {
			boolean found = false;
			boolean foundAsDangling = false;
			for (EntityRecord reportedDuty : reportedDuties) {
				if (duty.getEntity().getId().equals(reportedDuty.getId())) {
					final EntityState lastState = reportedDuty.getLastState();
					switch (lastState) {
					case CONFIRMED:
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
			final Shard should = shardingScheme.getScheme().findDutyLocation(e.getId());
			if (should==null) {
				logger.error("unexisting duty: " + e.toString() + " reported by shard: " + sourceShard);
			} else if (!should.equals(sourceShard)) {
				logger.error("relocated duty: " + e.toString() + " being reported by shard: " + sourceShard);
			}
		}
	}

	public void shardStateChange(final Shard shard, final ShardState prior, final Shard.Change change) {
		shard.applyChange(change);
		shardingScheme.getScheme().stealthChange(true);		
		switch (change.getState()) {
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
		final Collection<ShardEntity> dangling = shardingScheme.getScheme().getDutiesByShard(shard);
		if (logger.isInfoEnabled()) {
			logger.info("{}: Removing fallen Shard: {} from ptable. Saving: #{} duties: {}", classname, shard,
				dangling.size(), ShardEntity.toStringIds(dangling));
		}
		for (ShardEntity e: dangling) {
			if (shardingScheme.getStage().addDangling(e)) {
				e.getJournal().addEvent(
						DETACH, 
						CONFIRMED, 
						shard.getShardID(), 
						e.getJournal().getLast().getPlanId());
			}
		}
		shardingScheme.getScheme().removeShard(shard);		
	}


}
