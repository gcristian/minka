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
import static io.tilt.minka.domain.ShardEntity.State.CONFIRMED;
import static io.tilt.minka.domain.ShardEntity.State.DANGLING;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.tilt.minka.api.Duty;
import io.tilt.minka.api.Pallet;
import io.tilt.minka.core.leader.PartitionTable.ClusterHealth;
import io.tilt.minka.core.leader.distributor.Delivery;
import io.tilt.minka.core.leader.distributor.Plan;
import io.tilt.minka.core.task.Scheduler;
import io.tilt.minka.core.task.Semaphore;
import io.tilt.minka.domain.EntityEvent;
import io.tilt.minka.domain.Heartbeat;
import io.tilt.minka.domain.Shard;
import io.tilt.minka.domain.ShardEntity;
import io.tilt.minka.domain.ShardState;

/**
 * Maintainer and only writer of {@linkplain PartitionTable} 
 * Accounts coming heartbeats, detects anomalies
 * Only write-access to 
 * 
 * @author Cristian Gonzalez
 * @since Jan 4, 2016
 */
public class Bookkeeper {

	private final Logger logger = LoggerFactory.getLogger(getClass());

	private static final int MAX_EVENT_DATE_FOR_DIRTY = 10000;

	private final PartitionTable partitionTable;
	   private final Scheduler scheduler;
	public Bookkeeper(final PartitionTable partitionTable, final Scheduler scheduler) {
		this.partitionTable = partitionTable;
		     this.scheduler = java.util.Objects.requireNonNull(scheduler);
	}

	public void check(final Heartbeat beat, final Shard sourceShard) {
		if (beat.getStateChange() == ShardState.QUITTED) {
			checkShardChangingState(sourceShard);
			return;
		}
		sourceShard.addHeartbeat(beat);
		if (beat.getCapacities()!=null) {
			sourceShard.setCapacities(beat.getCapacities());
		}
		if (!beat.isReportedCapturedDuties()) {
			return;
		}
			
		if (partitionTable.getCurrentPlan() == null || partitionTable.getCurrentPlan().isClosed()) {
			// believe only when online: to avoid Dirty efects after follower's hangs/stucks
			// so it clears itself before trusting their HBs
			for (final ShardEntity duty : beat.getReportedCapturedDuties()) {
				if (duty.getState() == CONFIRMED) {
					try {
						changeStage(sourceShard, duty);
					} catch (ConcurrentDutyException cde) {
						if (partitionTable.getHealth() == ClusterHealth.STABLE) {
							// TODO 
						}
						logger.error("{}: Rebell shard: {}", getClass().getSimpleName(), sourceShard, cde);
					}
				} else if (duty.getState() == DANGLING) {
					logger.error("{}: Shard {} reported Dangling Duty (follower's unconfident: {}): {}",
							getClass().getSimpleName(), sourceShard.getShardID(), duty.getDutyEvent(), duty);
					if (duty.getDutyEvent().is(EntityEvent.CREATE)) {
					} else if (duty.getDutyEvent().is(EntityEvent.REMOVE)) {
					}
				}
			}
		} else {
			analyzeReportedDuties(sourceShard, beat.getReportedCapturedDuties());
		}
		// TODO perhaps in presence of Reallocation not ?
		if (sourceShard.getState().isAlive()) {
			declareHeartbeatAbsencesAsMissing(sourceShard, beat.getReportedCapturedDuties());
		}
	}

	private void analyzeReportedDuties(final Shard source, final List<ShardEntity> heartbeatDuties) {
		final Plan plan = partitionTable.getCurrentPlan();
		final Delivery delivery = plan.getDelivery(source);
		confirmReallocated(source, heartbeatDuties, delivery);
		confirmAbsences(source, heartbeatDuties, delivery);
		if (plan.hasFinalized()) {
		    logger.info("{}: Plan finished ! (all changes in stage)", getClass().getSimpleName());
		    plan.close();
		} else if (plan.hasUnlatched()) {
		    scheduler.forward(scheduler.get(Semaphore.Action.DISTRIBUTOR));
		}
	}
	
	 /*this checks partition table looking for missing duties (not declared dangling, that's diff) */
	private void declareHeartbeatAbsencesAsMissing(final Shard shard, final List<ShardEntity> heartbeatDuties) {
		final Set<ShardEntity> sortedLog = new TreeSet<>();
		final Map<String, AtomicInteger> absencesPerHeartbeat = new HashMap<>();
		for (final ShardEntity duty : partitionTable.getStage().getDutiesByShard(shard)) {
			boolean found = false;
			for (ShardEntity hbDuty : heartbeatDuties) {
				if (duty.equals(hbDuty)) {
					found = true;
					break;
				}
			}
			if (!found) {
				AtomicInteger ai = absencesPerHeartbeat.get(duty.getEntity().getId());
				if (ai == null) {
					absencesPerHeartbeat.put(duty.getEntity().getId(), ai = new AtomicInteger(0));
				}
				sortedLog.add(duty);
				partitionTable.getNextStage().getDutiesMissing().add(duty);
			}
		}
		if (!sortedLog.isEmpty()) {
			logger.warn("{}: ShardID: {}, Missing Duties in heartbeat {}, Added to PartitionTable",
					getClass().getSimpleName(), shard.getShardID(), ShardEntity.toStringIds(sortedLog));
		}
	}

	/* find the up-coming */
	private void confirmReallocated(final Shard shard, final List<ShardEntity> heartbeatDuties, final Delivery delivery) {

		final Set<ShardEntity> sortedLogConfirmed = new TreeSet<>();
		final Set<ShardEntity> sortedLogDirty = new TreeSet<>();
		for (final ShardEntity heartbeatDuty : heartbeatDuties) {
			for (ShardEntity prescriptedDuty : delivery.getDuties()) {
				if (prescriptedDuty.equals(heartbeatDuty)) {
					if (heartbeatDuty.getState() == CONFIRMED
							&& prescriptedDuty.getDutyEvent() == heartbeatDuty.getDutyEvent()) {
						sortedLogConfirmed.add(heartbeatDuty);
						// remove the one holding older State
						prescriptedDuty.registerEvent(CONFIRMED);
						changeStage(shard, heartbeatDuty);
						break;
					} else {
						final DateTime fact = prescriptedDuty.getEventDateForState(prescriptedDuty.getState());
						final long now = System.currentTimeMillis();
						if (now - fact.getMillis() > MAX_EVENT_DATE_FOR_DIRTY) {
							sortedLogDirty.add(heartbeatDuty);
						}
					}
				}
			}
		}
		if (!sortedLogConfirmed.isEmpty()) {
			logger.info("{}: ShardID: {}, Confirming partition event for Duties: {}", getClass().getSimpleName(),
					shard.getShardID(), ShardEntity.toStringIds(sortedLogConfirmed));
		}
		if (!sortedLogDirty.isEmpty()) {
			logger.warn("{}: ShardID: {}, Reporting DIRTY partition event for Duties: {}", getClass().getSimpleName(),
					shard.getShardID(), ShardEntity.toStringIds(sortedLogDirty));
		}
	}

	private void changeStage(final Shard shard, final ShardEntity duty) {
		if (partitionTable.getStage().confirmDutyAboutShard(duty, shard)) {
			if (!partitionTable.getNextStage().getDutiesCrud().remove(duty)) {
				
			}
		}
	}

	/* check un-coming as unassign */
	private void confirmAbsences(final Shard shard, final List<ShardEntity> heartbeatDuties, final Delivery delivery) {
		final Set<ShardEntity> sortedLog = new TreeSet<>();
		for (ShardEntity reallocatedDuty :delivery.getDuties()) {
			if ((reallocatedDuty.getDutyEvent().is(EntityEvent.DETACH)
					|| reallocatedDuty.getDutyEvent().is(EntityEvent.REMOVE))
					&& !heartbeatDuties.stream().anyMatch(
							i -> i.equals(reallocatedDuty) && i.getDutyEvent() != reallocatedDuty.getDutyEvent())) {
				sortedLog.add(reallocatedDuty);
				reallocatedDuty.registerEvent(CONFIRMED);
				changeStage(shard, reallocatedDuty);
			}
		}
		if (!sortedLog.isEmpty()) {
			logger.info("{}: ShardID: {}, Confirming (by absence) partioning event for Duties: {}",
					getClass().getSimpleName(), shard.getShardID(), ShardEntity.toStringIds(sortedLog));
		}
	}

	public void checkShardChangingState(final Shard shard) {
		switch (shard.getState()) {
		case GONE:
		case QUITTED:
			//if (getCurrentReallocation().isEmpty()) {
			recoverAndRetire(shard);
			//} else {
			// TODO esta situacion estaba contemplada pero aparecio un bug en 
			// el manejo del transporte por el cambio de broker que permite
			// resetear el realloc cuando se cayo un shard
			//}
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
		final Set<ShardEntity> dangling = new TreeSet<>();
		final Set<ShardEntity> fallenDuties = partitionTable.getStage().getDutiesByShard(shard);
		dangling.addAll(fallenDuties);

		logger.info("{}: Saved from fallen Shard: {}, {} Duties: {}", getClass().getSimpleName(), shard,
				dangling.size(), ShardEntity.toStringIds(dangling));

		logger.info("{}: Removing Shard: {} from partition table", getClass().getSimpleName(), shard);
		partitionTable.getStage().removeShard(shard);
		partitionTable.getNextStage().getDutiesDangling().addAll(dangling);
	}

	private boolean presentInPartition(final ShardEntity duty) {
		final Shard shardLocation = partitionTable.getStage().getDutyLocation(duty);
		return shardLocation != null && shardLocation.getState().isAlive();
	}

	public void cleanTemporaryDuties() {
		// partitionTable.getNextStage().removeCrudDuties();
	    partitionTable.getNextStage().getDutiesDangling().removeAll(
	            partitionTable.getNextStage().getDutiesDangling().stream()
    		        .filter(e->e.getState()!=ShardEntity.State.STUCK)
    		        .collect(Collectors.toList()));
		
	}

	public void registerDutiesFromSource(final List<Duty<?>> dutiesFromSource) {
		final Set<ShardEntity> sortedLog = new TreeSet<>();
		final Iterator<Duty<?>> it = dutiesFromSource.iterator();
		while (it.hasNext()) {
			final Duty<?> duty = it.next();
			final ShardEntity pallet = partitionTable.getStage().getPalletById(duty.getPalletId());
			final ShardEntity.Builder builder = ShardEntity.Builder.builder(duty);
			if (pallet!=null) {
				builder.withRelatedEntity(pallet);
			}
			final ShardEntity potential = builder.build();
			if (presentInPartition(potential)) {
				sortedLog.add(potential);
				it.remove();
			} else {
				if (pallet!=null) {
					logger.info("{}: Adding New Duty: {}", getClass().getSimpleName(), potential);
					partitionTable.getNextStage().addCrudDuty(potential);
				} else {
					logger.error("{}: Skipping Duty CRUD {}: Pallet Not found (pallet id: {})", getClass().getSimpleName(),
							potential, potential.getDuty().getPalletId());
				}
			}
		}
		if (!sortedLog.isEmpty()) {
			logger.info("{}: Skipping Duty CRUD already in PTable: {}", getClass().getSimpleName(),
					ShardEntity.toStringIds(sortedLog));
		}
	}

	public void registerPalletsFromSource(final List<Pallet<?>> palletsFromSource) {
		final Set<ShardEntity> sortedLog = new TreeSet<>();
		final Iterator<Pallet<?>> it = palletsFromSource.iterator();
		while (it.hasNext()) {
			final ShardEntity she = ShardEntity.Builder.builder(it.next()).build();
			if (palletsFromSource.contains(she)) {
				sortedLog.add(she);
				it.remove();
			} else {
				logger.info("{}: Adding New Pallet: {} with Balancer: {}", getClass().getSimpleName(), she, 
						she.getPallet().getMetadata());
				partitionTable.addCrudPallet(she);
			}
		}
		if (!sortedLog.isEmpty()) {
			logger.info("{}: Skipping Pallet CRUD already in PTable: {}", getClass().getSimpleName(),
					ShardEntity.toStringIds(sortedLog));
		}
	}

	/**
	 * Check valid actions to client sent duties/pallets, 
	 * according their action and the current partition table
	 * @param 	dutiesFromAction entities to act on
	 */
	public void registerCRUD(ShardEntity... dutiesFromAction) {
		for (final ShardEntity entity : dutiesFromAction) {
			final boolean typeDuty = entity.getType()==ShardEntity.Type.DUTY;
			final boolean found = (typeDuty && presentInPartition(entity)) || 
					(!typeDuty && partitionTable.getStage().getPalletss().contains(entity));
			final EntityEvent event = entity.getDutyEvent();
			if (!event.isCrud()) {
				throw new RuntimeException("Bad call");
			}
			if ((!found && event == CREATE) || (found && event == EntityEvent.REMOVE)) {
				logger.info("{}: Registering Crud {}: {}", getClass().getSimpleName(), typeDuty ? "Duty": "Pallet", entity);
				if (typeDuty) {
					if (event == CREATE) {
						final ShardEntity pallet = partitionTable.getStage().getPalletById(entity.getDuty().getPalletId());
						if (pallet!=null) {
							partitionTable.getNextStage().addCrudDuty(
									ShardEntity.Builder.builder(entity.getEntity())
										.withRelatedEntity(pallet).build());
						} else {
							logger.error("{}: Skipping Crud Event {}: Pallet ID :{} set not found or yet created", getClass().getSimpleName(),
									event, entity, entity.getDuty().getPalletId());
						}
					} else {
						partitionTable.getNextStage().addCrudDuty(entity);
					}
				} else {
					partitionTable.addCrudPallet(entity);
				}
			} else {
				logger.warn("{}: Skipping Crud Event {} {} in Partition Table: {}", 
						getClass().getSimpleName(), event, found ? "already found": " Not found", entity);
			}
		}
	}

}
