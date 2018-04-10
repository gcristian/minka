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

import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.BiConsumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.tilt.minka.api.ConsistencyException;
import io.tilt.minka.api.Duty;
import io.tilt.minka.api.Pallet;
import io.tilt.minka.core.leader.distributor.Delivery;
import io.tilt.minka.core.leader.distributor.Plan;
import io.tilt.minka.core.leader.distributor.SchemeWriter;
import io.tilt.minka.core.task.Scheduler;
import io.tilt.minka.domain.EntityEvent;
import io.tilt.minka.domain.Heartbeat;
import io.tilt.minka.domain.Shard;
import io.tilt.minka.domain.Shard.ShardState;
import io.tilt.minka.domain.ShardEntity;
import io.tilt.minka.domain.EntityState;
/**
 * Watches follower's heartbeats taking action on any difference.
 * 
 * Expected changes are delegated to {@linkplain SchemeWriter} 
 * Anomalies and CRUD ops. are recorded into {@linkplain Backstage}
 * 
 * @author Cristian Gonzalez
 * @since Jan 4, 2016
 */
public class SchemeSentry implements BiConsumer<Heartbeat, Shard> {

	private final Logger logger = LoggerFactory.getLogger(getClass());

	private final PartitionTable partitionTable;
	private final SchemeWriter schemeWriter;
	
	public SchemeSentry(final PartitionTable partitionTable, final Scheduler scheduler) {
		this.partitionTable = partitionTable;
		this.schemeWriter = new SchemeWriter(partitionTable);
	}

	/**
	 * this's raw from Broker's message reception
	 */
	@Override
	public void accept(final Heartbeat beat, final Shard shard) {
	    if (beat.getStateChange()!=null) {
	        shardStateChange(shard, shard.getState(), beat.getStateChange());
	        if (beat.getStateChange() == ShardState.QUITTED) {
	            return;
	        }
	    }
	    
		shard.addHeartbeat(beat);
		if (beat.getCapacities()!=null) {
			shard.setCapacities(beat.getCapacities());
		}
		
		final Plan plan = partitionTable.getCurrentPlan();
		if (plan!=null && !plan.getResult().isClosed()) {
			final Delivery delivery = plan.getDelivery(shard);
			if (delivery!=null) {
				schemeWriter.detectChanges(delivery, plan, beat, shard);
			} else if (logger.isInfoEnabled()){
				logger.info("{}: no pending Delivery for heartbeat's shard: {}", 
		            getClass().getSimpleName(), shard.getShardID().toString());
			}			
		} else if (plan == null && beat.reportsDuties()) {
			// there's been a change of leader: i'm initiating with older followers
			// TODO use DomainInfo to send a planid so the next leader can validate 
			// beated's plan is at least some range in between and so leader learns from beats 
			for (ShardEntity e: beat.getReportedCapturedDuties()) {
				partitionTable.getScheme().writeDuty(e, shard, EntityEvent.ATTACH);
			}
		}

		if ((beat.reportsDuties()) && shard.getState().isAlive()) {
			detectAndSaveAbsents(shard, beat.getReportedCapturedDuties());
			detectInvalidShards(shard, beat.getReportedCapturedDuties());
		}
	}
	
	/*this checks partition table looking for missing duties (not declared dangling, that's diff) */
	private void detectAndSaveAbsents(final Shard shard, final List<ShardEntity> reportedDuties) {
		Set<ShardEntity> sortedLog = null;
		for (final ShardEntity duty : partitionTable.getScheme().getDutiesByShard(shard)) {
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
								getClass().getSimpleName(), reportedDuty.getLastState(), duty.getLastState());
						found = true;
						break;
					}
					break;
				}
			}
						
			if (!found) {
				boolean done = false;
				if (foundAsDangling) {
					done = partitionTable.getBackstage().addDangling(duty);
				} else {
					done = partitionTable.getBackstage().addMissing(duty);
				}
				// copy the event so it's marked for later consideration
				if (done) {
					duty.getJournal().addEvent(
							duty.getLastEvent(), 
							foundAsDangling ? EntityState.DANGLING : EntityState.MISSING, 
							null, // the last shard id 
							duty.getJournal().getLast().getPlanId());
				    if (sortedLog==null) {
				        sortedLog = new TreeSet<>();
				    }
					sortedLog.add(duty);
				}
			}
		}
		if (sortedLog!=null) {
			logger.error("{}: ShardID: {}, absent duties in Heartbeat: {}, (backing up to backstage)",
					getClass().getSimpleName(), shard.getShardID(), ShardEntity.toStringIds(sortedLog));
		}
	}
	

    private void detectInvalidShards(final Shard sourceShard, final List<ShardEntity> reportedCapturedDuties) {
		for (final ShardEntity e : reportedCapturedDuties) {
			final Shard should = partitionTable.getScheme().getDutyLocation(e);
			if (should==null) {
				throw new ConsistencyException("unexisting duty: " + e.toBrief() + " reported by shard: " + sourceShard);
			} else if (!should.equals(sourceShard)) {
				throw new ConsistencyException("relocated duty: " + e.toBrief() + " being reported by shard: " + sourceShard);
			}
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
			logger.info("{}: Removing fallen Shard: {} from ptable. Saving: #{} duties: {}", getClass().getSimpleName(), shard,
				dangling.size(), ShardEntity.toStringIds(dangling));
		}
		partitionTable.getScheme().removeShard(shard);
		partitionTable.getBackstage().addDangling(dangling);
	}

	private boolean presentInPartition(final ShardEntity duty) {
		final Shard shardLocation = partitionTable.getScheme().getDutyLocation(duty.getDuty());
		return shardLocation != null && shardLocation.getState().isAlive();
	}

    private boolean presentInPartition(final Duty<?> duty) {
        final Shard shardLocation = partitionTable.getScheme().getDutyLocation(duty);
        return shardLocation != null && shardLocation.getState().isAlive();
    }

	public void enterDutiesFromSource(final List<Duty<?>> dutiesFromSource) {
		Set<Duty<?>> sortedLog = null;
		final Iterator<Duty<?>> it = dutiesFromSource.iterator();
		while (it.hasNext()) {
			final Duty<?> duty = it.next();
			final ShardEntity pallet = partitionTable.getScheme().getPalletById(duty.getPalletId());
			if (presentInPartition(duty)) {
			    if (sortedLog==null) {
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
					if (partitionTable.getBackstage().addCrudDuty(newone)) {
						if (logger.isInfoEnabled()) {
							logger.info("{}: Adding New Duty: {}", getClass().getSimpleName(), newone);
						}
					}
				} else {
					logger.error("{}: Skipping Duty CRUD {}: Pallet Not found (pallet id: {})", getClass().getSimpleName(),
							duty, duty.getPalletId());
				}
			}
		}
		if (sortedLog!=null && logger.isInfoEnabled()) {
			logger.info("{}: Skipping Duty CRUD already in PTable: {}", getClass().getSimpleName(), sortedLog);
		}
	}

	public void enterPalletsFromSource(final List<Pallet<?>> palletsFromSource) {
		final Set<ShardEntity> sortedLog = new TreeSet<>();
		final Iterator<Pallet<?>> it = palletsFromSource.iterator();
		while (it.hasNext()) {
			final ShardEntity she = ShardEntity.Builder.builder(it.next()).build();
			if (partitionTable.getScheme().getPallets().contains(she)) {
				sortedLog.add(she);
				it.remove();
			} else {
				if (logger.isInfoEnabled()) {
					logger.info("{}: Adding New Pallet: {} with Balancer: {}", getClass().getSimpleName(), she, 
						she.getPallet().getMetadata());
				}
				partitionTable.addCrudPallet(she);
			}
		}
		if (!sortedLog.isEmpty() && logger.isInfoEnabled()) {
			logger.info("{}: Skipping Pallet CRUD already in PTable: {}", getClass().getSimpleName(),
					ShardEntity.toStringIds(sortedLog));
		}
	}

	/**
	 * Check valid actions to client sent duties/pallets, 
	 * according their action and the current partition table
	 * @param 	dutiesFromAction entities to act on
	 */
	public void enterCRUD(ShardEntity... dutiesFromAction) {
		for (final ShardEntity entity : dutiesFromAction) {
			final boolean typeDuty = entity.getType()==ShardEntity.Type.DUTY;
			final boolean found = (typeDuty && presentInPartition(entity)) || 
					(!typeDuty && partitionTable.getScheme().getPallets().contains(entity));
			final EntityEvent event = entity.getLastEvent();
			if (!event.isCrud()) {
				throw new RuntimeException("Bad call");
			}
			if ((!found && event == CREATE) || (found && event == EntityEvent.REMOVE)) {
				if (logger.isInfoEnabled()) {
					logger.info("{}: Registering Crud {}: {}", getClass().getSimpleName(), entity.getType(), entity);
				}
				if (typeDuty) {
					if (event == CREATE) {
						final ShardEntity pallet = partitionTable.getScheme().getPalletById(entity.getDuty().getPalletId());
						if (pallet!=null) {
							partitionTable.getBackstage().addCrudDuty(
									ShardEntity.Builder.builder(entity.getEntity())
										.withRelatedEntity(pallet)
										.build());
						} else {
							logger.error("{}: Skipping Crud Event {}: Pallet ID :{} set not found or yet created", getClass().getSimpleName(),
									event, entity, entity.getDuty().getPalletId());
						}
					} else {
						partitionTable.getBackstage().addCrudDuty(entity);
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
