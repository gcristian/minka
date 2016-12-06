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
import static io.tilt.minka.domain.EntityEvent.FINALIZED;
import static io.tilt.minka.domain.ShardEntity.State.CONFIRMED;
import static io.tilt.minka.domain.ShardEntity.State.DANGLING;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicInteger;

import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.tilt.minka.api.Duty;
import io.tilt.minka.api.Pallet;
import io.tilt.minka.core.leader.PartitionTable.ClusterHealth;
import io.tilt.minka.core.leader.distributor.Roadmap;
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

	public Bookkeeper(final PartitionTable partitionTable) {
		this.partitionTable = partitionTable;
	}

	public void check(final Heartbeat hb, final Shard shard) {
		if (hb.getStateChange() == ShardState.QUITTED) {
			checkShardChangingState(shard);
			return;
		}
		shard.addHeartbeat(hb);
		if (hb.getCapacities()!=null) {
			shard.setCapacities(hb.getCapacities());
		}
		if (partitionTable.getCurrentRoadmap().isEmpty()) {
			// believe only when online: to avoid Dirty efects after follower's hangs/stucks
			// so it clears itself before trusting their HBs
			for (final ShardEntity duty : hb.getDuties()) {
				if (duty.getState() == CONFIRMED) {
					try {
						changeStage(shard, duty);
					} catch (ConcurrentDutyException cde) {
						if (partitionTable.getHealth() == ClusterHealth.STABLE) {
							// TODO 
						}
						logger.error("{}: Rebell shard: {}", getClass().getSimpleName(), shard, cde);
					}
				} else if (duty.getState() == DANGLING) {
					logger.error("{}: Shard {} reported Dangling Duty (follower's unconfident: {}): {}",
							getClass().getSimpleName(), shard.getShardID(), duty.getDutyEvent(), duty);
					if (duty.getDutyEvent().is(EntityEvent.CREATE)) {
					} else if (duty.getDutyEvent().is(EntityEvent.REMOVE)) {
					}
				} else if (duty.getState() == ShardEntity.State.FINALIZED) {
					// TODO remove from Stage directly...
				}
			}
		} else {
			analyzeReportedDuties(shard, hb.getDuties());
		}
		// TODO perhaps in presence of Reallocation not ?
		if (shard.getState().isAlive()) {
			declareHeartbeatAbsencesAsMissing(shard, hb.getDuties());
		}
	}

	private void analyzeReportedDuties(final Shard shard, final List<ShardEntity> heartbeatDuties) {
		final Roadmap road = partitionTable.getCurrentRoadmap();
		final Set<ShardEntity> currentChanges = road.getGroupedDeliveries().get(shard);

		confirmReallocated(shard, heartbeatDuties, currentChanges);
		confirmAbsences(shard, heartbeatDuties, currentChanges);

		if (road.hasCurrentStepFinished() && road.hasFinished()) {
			logger.info("{}: Roadmap finished ! (all changes in stage)", getClass().getSimpleName());
			road.close();
		} else if (road.hasCurrentStepFinished()) {
			//scheduler.forward(scheduler.get(Action.DISTRIBUTOR));
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
	private void confirmReallocated(final Shard shard, final List<ShardEntity> heartbeatDuties,
			final Set<ShardEntity> currentChanges) {

		final Set<ShardEntity> sortedLogConfirmed = new TreeSet<>();
		final Set<ShardEntity> sortedLogDirty = new TreeSet<>();
		for (final ShardEntity heartbeatDuty : heartbeatDuties) {
			for (ShardEntity prescriptedDuty : currentChanges) {
				if (prescriptedDuty.equals(heartbeatDuty)) {
					if (heartbeatDuty.getState() == CONFIRMED
							&& prescriptedDuty.getDutyEvent() == heartbeatDuty.getDutyEvent()) {
						sortedLogConfirmed.add(heartbeatDuty);
						// remove the one holding older State
						prescriptedDuty.registerEvent(CONFIRMED);
						currentChanges.remove(prescriptedDuty);
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
	private void confirmAbsences(final Shard shard, final List<ShardEntity> heartbeatDuties,
			final Set<ShardEntity> currentChanges) {

		final Iterator<ShardEntity> it = currentChanges.iterator();
		final Set<ShardEntity> sortedLog = new TreeSet<>();
		while (it.hasNext()) {
			final ShardEntity reallocatedDuty = it.next();
			if ((reallocatedDuty.getDutyEvent().is(EntityEvent.DETACH)
					|| reallocatedDuty.getDutyEvent().is(EntityEvent.REMOVE))
					&& !heartbeatDuties.stream().anyMatch(
							i -> i.equals(reallocatedDuty) && i.getDutyEvent() != reallocatedDuty.getDutyEvent())) {
				sortedLog.add(reallocatedDuty);
				reallocatedDuty.registerEvent(CONFIRMED);
				changeStage(shard, reallocatedDuty);
				it.remove();
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
		partitionTable.getNextStage().getDutiesDangling().clear();
	}

	public void registerDutiesFromSource(final List<Duty<?>> dutiesFromSource) {
		final Set<ShardEntity> sortedLog = new TreeSet<>();
		final Iterator<Duty<?>> it = dutiesFromSource.iterator();
		while (it.hasNext()) {
			final ShardEntity potential = ShardEntity.create(it.next());
			if (presentInPartition(potential)) {
				sortedLog.add(potential);
				it.remove();
			} else {
				final ShardEntity pallet = partitionTable.getStage().getPalletById(potential.getDuty().getPalletId());
				if (pallet!=null) {
					potential.setRelatedEntity(pallet);
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
			final ShardEntity she = ShardEntity.create(it.next());
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
	 */
	public void registerCRUD(ShardEntity... dutiesFromAction) {
		for (final ShardEntity ent : dutiesFromAction) {
			final boolean typeDuty = ent.getType()==ShardEntity.Type.DUTY;
			final boolean found = (typeDuty && presentInPartition(ent)) || 
					(!typeDuty && partitionTable.getStage().getPallets().contains(ent));
			final EntityEvent event = ent.getDutyEvent();
			if (!event.isCrud()) {
				throw new RuntimeException("Bad call");
			}
			if ((!found && event == CREATE) || (found && (event == EntityEvent.REMOVE || event == FINALIZED))) {
				logger.info("{}: Registering Crud {}: {}", getClass().getSimpleName(), typeDuty ? "Duty": "Pallet", ent);
				if (typeDuty) {
					if (event == CREATE) {
						final ShardEntity pallet = partitionTable.getStage().getPalletById(ent.getDuty().getPalletId());
						if (pallet!=null) {
							ent.setRelatedEntity(pallet);
							partitionTable.getNextStage().addCrudDuty(ent);
						} else {
							logger.error("{}: Skipping Crud Event {}: Pallet ID :{} set not found or yet created", getClass().getSimpleName(),
									event, ent, ent.getDuty().getPalletId());
						}
					} else {
						partitionTable.getNextStage().addCrudDuty(ent);
					}
				} else {
					partitionTable.addCrudPallet(ent);
				}
			} else {
				logger.warn("{}: Skipping Crud Event {} {} in Partition Table: {}", 
						getClass().getSimpleName(), event, found ? "already found": " Not found", ent);
			}
		}
	}

}
