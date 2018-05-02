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

import static io.tilt.minka.domain.EntityEvent.CREATE;
import static io.tilt.minka.domain.EntityEvent.DETACH;
import static io.tilt.minka.domain.EntityEvent.REMOVE;
import static io.tilt.minka.domain.EntityState.CONFIRMED;
import static io.tilt.minka.domain.EntityState.DANGLING;
import static io.tilt.minka.domain.EntityState.MISSING;
import static io.tilt.minka.domain.EntityState.PREPARED;
import static io.tilt.minka.domain.Shard.ShardState.QUITTED;
import static java.util.Collections.emptyMap;

import java.time.Instant;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.BiConsumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.tilt.minka.api.Duty;
import io.tilt.minka.api.Pallet;
import io.tilt.minka.core.leader.PartitionScheme.Backstage;
import io.tilt.minka.core.leader.PartitionScheme.Scheme;
import io.tilt.minka.api.Reply;
import io.tilt.minka.api.ReplyResult;
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
	
	private final PartitionScheme partitionScheme;
	private final ChangeDetector changeDetector;
	
	public SchemeSentry(final PartitionScheme partitionScheme, final Scheduler scheduler) {
		this.partitionScheme = partitionScheme;
		this.changeDetector = new ChangeDetector(partitionScheme);
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
			if (partitionScheme.getCurrentPlan()!=null) {
				detectInvalidShards(shard, beat.getReportedCapturedDuties());
			}
		}
	}
	
	public void detectAndWriteChanges(final Shard shard, final Heartbeat beat) {
		final ChangePlan changePlan = partitionScheme.getCurrentPlan();
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
				if (!partitionScheme.getScheme().dutyExistsAt(e, shard)) {
					if (partitionScheme.getScheme().write(e, shard, e.getLastEvent(), null)) {
						logger.info("{}: Scheme learning from Followers: {}", classname, e);
					}
				}
			}
		}
	}

	private void writesOnChange(final Shard shard, final Log changelog, final ShardEntity entity) {
		if (partitionScheme.getScheme().write(entity, shard, changelog.getEvent(), ()-> {
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
				partitionScheme.getScheme().write(entity, shard, previous.getEvent(), ()->{
					logger.info("{}: Removing duty at request: {}", classname, entity);
				});
			}
		}
	}

	private void updateBackstage(final Log changelog, final ShardEntity entity) {
		// remove it from the backstage
		final ShardEntity crud = partitionScheme.getBackstage().getCrudByDuty(entity.getDuty());
		if (crud!=null) {
			final Instant lastEventOnCrud = crud.getJournal().getLast().getHead().toInstant();
			boolean previousThanCrud = changelog.getHead().toInstant().isBefore(lastEventOnCrud);
			// if the update corresponds to the last CRUD OR they're both the same event (duplicated operation)
			if (!previousThanCrud || changelog.getEvent()==crud.getLastEvent()) {
				partitionScheme.getBackstage().removeCrud(entity);
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
				done = partitionScheme.getBackstage().addDangling(e.getValue());
			} else {
				done = partitionScheme.getBackstage().addMissing(e.getValue());
			}
			// copy the event so it's marked for later consideration
			if (done) {
				e.getValue().forEach(d->partitionScheme.getScheme().write(d, shard, REMOVE, ()->{
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
		for (final ShardEntity duty : partitionScheme.getScheme().getDutiesByShard(shard)) {
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
			final Shard should = partitionScheme.getScheme().getDutyLocation(e);
			if (should==null) {
				logger.error("unexisting duty: " + e.toBrief() + " reported by shard: " + sourceShard);
			} else if (!should.equals(sourceShard)) {
				logger.error("relocated duty: " + e.toBrief() + " being reported by shard: " + sourceShard);
			}
		}
	}

	public void shardStateChange(final Shard shard, final ShardState prior, final ShardState newState) {
		shard.setState(newState);
		partitionScheme.getScheme().stealthChange(true);		
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
		final Collection<ShardEntity> dangling = partitionScheme.getScheme().getDutiesByShard(shard);
		if (logger.isInfoEnabled()) {
			logger.info("{}: Removing fallen Shard: {} from ptable. Saving: #{} duties: {}", classname, shard,
				dangling.size(), ShardEntity.toStringIds(dangling));
		}
		for (ShardEntity e: dangling) {
			if (partitionScheme.getBackstage().addDangling(e)) {
				e.getJournal().addEvent(
						DETACH, 
						CONFIRMED, 
						shard.getShardID(), 
						e.getJournal().getLast().getPlanId());
			}
		}
		partitionScheme.getScheme().removeShard(shard);		
	}

	private boolean presentInPartition(final ShardEntity duty) {
		final Shard shardLocation = partitionScheme.getScheme().getDutyLocation(duty.getDuty());
		return shardLocation != null && shardLocation.getState().isAlive();
	}

	private boolean presentInPartition(final Duty<?> duty) {
		final Shard shardLocation = partitionScheme.getScheme().getDutyLocation(duty);
		return shardLocation != null && shardLocation.getState().isAlive();
	}

	public void enterDutiesFromSource(final List<Duty<?>> dutiesFromSource) {
		Set<Duty<?>> sortedLog = null;
		final Iterator<Duty<?>> it = dutiesFromSource.iterator();
		while (it.hasNext()) {
			final Duty<?> duty = it.next();
			final ShardEntity pallet = partitionScheme.getScheme().getPalletById(duty.getPalletId());
			if (presentInPartition(duty)) {
				if (sortedLog == null) {
					sortedLog = new TreeSet<>();
				}
				sortedLog.add(duty);
				it.remove();
			} else {
				if (pallet!=null) {
					final ShardEntity newone = ShardEntity.Builder
							.builder(duty)
							.withRelatedEntity(pallet)
							.build();
					if (partitionScheme.getBackstage().addCrudDuty(newone)) {
						if (logger.isInfoEnabled()) {
							logger.info("{}: Adding New Duty: {}", classname, newone);
						}
					} else {
						// TRY FIXING LEARNT AFTER REELECTION
						for (ShardEntity e: partitionScheme.getBackstage().getDutiesCrud()) {
							if (e.equals(newone)) {
								if (e.getPallet()==null) {
									
								}
							}
						}
					}
				} else {
					logger.error("{}: Skipping Duty CRUD {}: Pallet Not found (pallet id: {})", classname,
							duty, duty.getPalletId());
				}
			}
		}
		if (sortedLog!=null && logger.isInfoEnabled()) {
			logger.info("{}: Skipping Duty CRUD already in PTable: {}", classname, sortedLog);
		}
	}

	public void enterPalletsFromSource(final List<Pallet<?>> palletsFromSource) {
		final Set<ShardEntity> sortedLog = new TreeSet<>();
		final Iterator<Pallet<?>> it = palletsFromSource.iterator();
		while (it.hasNext()) {
			final ShardEntity she = ShardEntity.Builder.builder(it.next()).build();
			if (partitionScheme.getScheme().filterPallets(she::equals)) {
				sortedLog.add(she);
				it.remove();
			} else {
				if (logger.isInfoEnabled()) {
					logger.info("{}: Adding New Pallet: {} with Balancer: {}", classname, she, 
						she.getPallet().getMetadata());
				}
				partitionScheme.addCrudPallet(she);
			}
		}
		if (!sortedLog.isEmpty() && logger.isInfoEnabled()) {
			logger.info("{}: Skipping Pallet CRUD already in PTable: {}", classname,
					ShardEntity.toStringIds(sortedLog));
		}
	}

	/**
	 * Check valid actions to client sent duties/pallets, 
	 * according their action and the current partition table
	 * @param 	dutiesFromAction entities to act on
	 * @return 
	 */
	public Reply enterCRUD(final ShardEntity entity) {
		Reply reply = null; 
		final boolean typeDuty = entity.getType()==ShardEntity.Type.DUTY;
		final boolean found = (typeDuty && presentInPartition(entity)) || 
				(!typeDuty && partitionScheme.getScheme().filterPallets(entity::equals));
		final EntityEvent event = entity.getLastEvent();
		if (!event.isCrud()) {
			throw new RuntimeException("Bad call");
		}
		
		if ((!found && event == CREATE) || (found && event == REMOVE)) {
			if (logger.isInfoEnabled()) {
				logger.info("{}: CRUD Request {} {}: {}", classname, entity.getLastEvent(), entity.getType(), entity);
			}
			if (typeDuty) {
				if (event == CREATE) {
					final ShardEntity pallet = partitionScheme.getScheme().getPalletById(entity.getDuty().getPalletId());
					if (pallet!=null) {
						final ShardEntity current  = partitionScheme.getScheme().getByDuty(entity.getDuty());
						if (current ==null) {
							final boolean added = partitionScheme.getBackstage().addCrudDuty(
									ShardEntity.Builder.builder(entity.getEntity())
										.withRelatedEntity(pallet)
										.build());
							if (added) {
								reply = new Reply(ReplyResult.SUCCESS, entity.getEntity(), PREPARED, CREATE, null);
							} else {
								reply = new Reply(ReplyResult.SUCCESS_OPERATION_ALREADY_SUBMITTED, 
										entity.getEntity(), null, EntityEvent.CREATE, 
										String.format("%s: Added already !: %s", classname, entity));
							}
						} else {
							reply = new Reply(ReplyResult.ERROR_ENTITY_ALREADY_EXISTS, 
									entity.getEntity(), current .getLastState(), current .getLastEvent(), 
									String.format("%s: Skipping Crud Event %s: already in scheme !", classname, entity));
						}
					} else {
						reply = new Reply(ReplyResult.ERROR_ENTITY_INCONSISTENT,
								entity.getEntity(), null, null, String.format(
								"%s: Skipping Crud Event %s: Pallet ID :%s set not found or yet created", classname,
								event, entity, entity.getDuty().getPalletId()));
					}
				} else {
					final ShardEntity current  = partitionScheme.getScheme().getByDuty(entity.getDuty());
					if (current!=null) {
						final boolean added = partitionScheme.getBackstage().addCrudDuty(entity);
						if (added) {
							reply = new Reply(ReplyResult.SUCCESS, entity.getEntity(), PREPARED, REMOVE, null);
						} else {
							reply = new Reply(ReplyResult.SUCCESS_OPERATION_ALREADY_SUBMITTED, 
									entity.getEntity(), null, EntityEvent.REMOVE, 
									String.format("%s: Added already !: %s", classname, entity));
						}
					} else {
						reply = new Reply(ReplyResult.ERROR_ENTITY_NOT_FOUND, entity.getEntity(), null, null, 
								String.format("%s: Deletion request not found on scheme: %s", classname, entity));
					}						
				}
			} else {
				final boolean original = partitionScheme.addCrudPallet(entity);
				reply = new Reply(original ? ReplyResult.SUCCESS : ReplyResult.SUCCESS_OPERATION_ALREADY_SUBMITTED, 
						entity.getEntity(), PREPARED, REMOVE, null);
			}
		} else {
			reply = new Reply(found ? ReplyResult.ERROR_ENTITY_ALREADY_EXISTS : ReplyResult.ERROR_ENTITY_NOT_FOUND, 
					entity.getEntity(), null, null, String.format("%s: Skipping Crud Event %s %s in Partition Table: %s", 
					getClass().getSimpleName(), event, found ? "already found": " Not found", entity));
		}
		if (reply.getMessage()!=null) {
			logger.warn(reply.toString());
		}
		return reply;
	}

}
