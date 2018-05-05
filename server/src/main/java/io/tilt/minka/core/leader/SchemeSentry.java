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
import java.util.function.BiConsumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.tilt.minka.core.leader.ShardingScheme.Backstage;
import io.tilt.minka.core.leader.ShardingScheme.Scheme;
import io.tilt.minka.core.leader.distributor.ChangeDetector;
import io.tilt.minka.core.leader.distributor.ChangePlan;
import io.tilt.minka.core.leader.distributor.Delivery;
import io.tilt.minka.core.task.Scheduler;
import io.tilt.minka.domain.EntityEvent;
import io.tilt.minka.domain.EntityJournal.Log;
import io.tilt.minka.domain.EntityState;
import io.tilt.minka.domain.Heartbeat;
import io.tilt.minka.domain.Shard;
import io.tilt.minka.domain.Shard.ShardState;
import io.tilt.minka.domain.ShardEntity;
/**
 * Single-point of write access to the {@linkplain Scheme}
 * Watches follower's heartbeats taking action on any update
 * Beats with changes are delegated to a {@linkplain ChangeDetector} 
 * Anomalies and CRUD ops. are recorded into {@linkplain Backstage}
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
		if (beat.getStateChange() != null) {
			logger.info("{}: ShardID: {} changes to: {}", classname, shard, beat.getStateChange());
			shardStateChange(shard, shard.getState(), beat.getStateChange());
			if (beat.getStateChange() == QUITTED) {
				return;
			}
		}

		shard.enterHeartbeat(beat);
		if (beat.getCapacities()!=null) {
			shard.setCapacities(beat.getCapacities());
		}
		
		detectAndWriteChanges(shard, beat);

		if ((beat.reportsDuties()) && shard.getState().isAlive()) {
			detectAndSaveAbsents(shard, beat.getReportedCapturedDuties());
			if (shardingScheme.getCurrentPlan()!=null) {
				detectInvalidShards(shard, beat.getReportedCapturedDuties());
			}
		}
	}
	
	public void detectAndWriteChanges(final Shard shard, final Heartbeat beat) {
		final ChangePlan changePlan = shardingScheme.getCurrentPlan();
		if (changePlan!=null && !changePlan.getResult().isClosed()) {
			final Delivery delivery = changePlan.getDelivery(shard);
			if (delivery!=null) {
				if (changeDetector.findPlannedChanges(delivery, changePlan, beat, shard, 
						(l,d)-> writesOnChange(shard, l, d))) {
					delivery.calculateState(s->logger.info(s));
				}
				if (changePlan.getResult().isClosed()) {
					if (logger.isInfoEnabled()) {
						logger.info("{}: ChangePlan finished ! (all changes in scheme)", classname);
					}
				} else if (logger.isInfoEnabled() && changePlan.hasUnlatched()) {
					logger.info("{}: ChangePlan unlatched, fwd >> distributor agent ", classname);
					//scheduler.forward(scheduler.get(Semaphore.Action.DISTRIBUTOR));
				}
			} else if (logger.isDebugEnabled()){
				logger.debug("{}: no pending Delivery for heartbeat's shard: {}", 
						getClass().getSimpleName(), shard.getShardID().toString());
			}			
		} else if (changePlan == null && beat.reportsDuties()) {
			// changePlan is only NULL before 1st distribution
			// there's been a change of leader: i'm initiating with older followers
			for (ShardEntity e: beat.getReportedCapturedDuties()) {
				//L: prepared, L: pending, F: received, C: confirmed, L: ack.
				if (!shardingScheme.getScheme().dutyExistsAt(e, shard)) {
					if (shardingScheme.getScheme().write(e, shard, e.getLastEvent(), null)) {
						logger.info("{}: Scheme learning from Followers: {}", classname, e);
					}
				}
			}
		}
	}

	private void writesOnChange(final Shard shard, final Log changelog, final ShardEntity entity) {
		if (shardingScheme.getScheme().write(entity, shard, changelog.getEvent(), ()-> {
			// copy the found situation to the instance we care
			entity.getJournal().addEvent(changelog.getEvent(),
					CONFIRMED,
					shard.getShardID(),
					changelog.getPlanId());
			})) {
			updateBackstage(changelog, entity);
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

	private void updateBackstage(final Log changelog, final ShardEntity entity) {
		// remove it from the backstage
		final ShardEntity crud = shardingScheme.getBackstage().getCrudByDuty(entity.getDuty());
		if (crud!=null) {
			final Instant lastEventOnCrud = crud.getJournal().getLast().getHead().toInstant();
			boolean previousThanCrud = changelog.getHead().toInstant().isBefore(lastEventOnCrud);
			// if the update corresponds to the last CRUD OR they're both the same event (duplicated operation)
			if (!previousThanCrud || changelog.getEvent()==crud.getLastEvent()) {
				shardingScheme.getBackstage().removeCrud(entity);
			} else {
				logger.warn("{}: Avoiding Backstage removal of CRUD as it's different and after the last event", 
						classname, entity);
			}
		} else {
			logger.warn("{}: Avoiding Backstage removal of CRUD as was not found: {}", classname, entity);
		}
	}
	
	/*this checks partition table looking for missing duties (not declared dangling, that's diff) */
	private void detectAndSaveAbsents(final Shard shard, final List<ShardEntity> reportedDuties) {
		for (Map.Entry<EntityState, List<ShardEntity>> e: findAbsent(shard, reportedDuties).entrySet()) {
			boolean done = false;
			if (e.getKey()==DANGLING) { 
				done = shardingScheme.getBackstage().addDangling(e.getValue());
			} else {
				done = shardingScheme.getBackstage().addMissing(e.getValue());
			}
			// copy the event so it's marked for later consideration
			if (done) {
				e.getValue().forEach(d->shardingScheme.getScheme().write(d, shard, REMOVE, ()->{
					d.getJournal().addEvent(
							d.getLastEvent(),
							e.getKey()==DANGLING ? DANGLING : MISSING, 
							null, // the last shard id 
							d.getJournal().getLast().getPlanId());
				}));
			}
		}
	}

	private Map<EntityState, List<ShardEntity>> findAbsent(final Shard shard, final List<ShardEntity> reportedDuties) {
		Map<EntityState, List<ShardEntity>> lost = null;
		for (final ShardEntity duty : shardingScheme.getScheme().getDutiesByShard(shard)) {
			boolean found = false;
			boolean foundAsDangling = false;
			for (ShardEntity reportedDuty : reportedDuties) {
				if (duty.equals(reportedDuty)) {
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
			logger.error("{}: ShardID: {}, absent duties in Heartbeat: {},{} (backing up to backstage)",
				getClass().getSimpleName(), shard.getShardID(), ShardEntity.toStringIds(lost.get(DANGLING)), 
				ShardEntity.toStringIds(lost.get(MISSING)));
		}			
		return lost !=null ? lost : emptyMap();
	}
	

	private void detectInvalidShards(final Shard sourceShard, final List<ShardEntity> reportedCapturedDuties) {
		for (final ShardEntity e : reportedCapturedDuties) {
			final Shard should = shardingScheme.getScheme().findDutyLocation(e);
			if (should==null) {
				logger.error("unexisting duty: " + e.toBrief() + " reported by shard: " + sourceShard);
			} else if (!should.equals(sourceShard)) {
				logger.error("relocated duty: " + e.toBrief() + " being reported by shard: " + sourceShard);
			}
		}
	}

	public void shardStateChange(final Shard shard, final ShardState prior, final ShardState newState) {
		shard.setState(newState);
		shardingScheme.getScheme().stealthChange(true);		
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
		final Collection<ShardEntity> dangling = shardingScheme.getScheme().getDutiesByShard(shard);
		if (logger.isInfoEnabled()) {
			logger.info("{}: Removing fallen Shard: {} from ptable. Saving: #{} duties: {}", classname, shard,
				dangling.size(), ShardEntity.toStringIds(dangling));
		}
		for (ShardEntity e: dangling) {
			if (shardingScheme.getBackstage().addDangling(e)) {
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
