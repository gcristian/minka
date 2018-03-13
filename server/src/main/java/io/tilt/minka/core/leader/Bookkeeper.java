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

import java.time.Instant;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.tilt.minka.api.Duty;
import io.tilt.minka.api.Pallet;
import io.tilt.minka.core.leader.distributor.Delivery;
import io.tilt.minka.core.leader.distributor.Plan;
import io.tilt.minka.core.task.Scheduler;
import io.tilt.minka.core.task.Semaphore;
import io.tilt.minka.domain.EntityEvent;
import io.tilt.minka.domain.LogList.Log;
import io.tilt.minka.domain.Heartbeat;
import io.tilt.minka.domain.Shard;
import io.tilt.minka.domain.Shard.ShardState;
import io.tilt.minka.domain.ShardEntity;
import io.tilt.minka.domain.ShardIdentifier;
import io.tilt.minka.domain.EntityState;
/**
 * Maintainer and only writer of {@linkplain PartitionTable} 
 * Accounts coming heartbeats, detects anomalies
 * Only write-access to 
 * 
 * @author Cristian Gonzalez
 * @since Jan 4, 2016
 */
public class Bookkeeper implements BiConsumer<Heartbeat, Shard> {

	private final Logger logger = LoggerFactory.getLogger(getClass());

	private static final int MAX_EVENT_DATE_FOR_DIRTY = 10000;

	private final PartitionTable partitionTable;
	private final Scheduler scheduler;
	
	public Bookkeeper(final PartitionTable partitionTable, final Scheduler scheduler) {
		this.partitionTable = partitionTable;
		this.scheduler = java.util.Objects.requireNonNull(scheduler);
	}

	@Override
	public void accept(final Heartbeat beat, final Shard sourceShard) {
		if (beat.getStateChange() == ShardState.QUITTED) {
			checkShardChangingState(sourceShard);
			return;
		}
		sourceShard.addHeartbeat(beat);
		if (beat.getCapacities()!=null) {
			sourceShard.setCapacities(beat.getCapacities());
		}
			
		if (partitionTable.getCurrentPlan() == null || partitionTable.getCurrentPlan().getResult().isClosed()) {
			noticeDanglings(beat, sourceShard);
		} else {
			analyzeReportedDuties(sourceShard, beat.getReportedCapturedDuties());
		}
		// TODO perhaps in presence of Reallocation not ?
		if (sourceShard.getState().isAlive()) {
			declareHeartbeatAbsencesAsMissing(sourceShard, beat.getReportedCapturedDuties());
		}
	}

    private void noticeDanglings(final Heartbeat beat, final Shard sourceShard) {
        // believe only when online: to avoid Dirty efects after follower's hangs/stucks
        // so it clears itself before trusting their HBs
        for (final ShardEntity duty : beat.getReportedCapturedDuties()) {
        	if (duty.getLastState() == EntityState.DANGLING) {
        		logger.error("{}: Shard {} reported Dangling Duty (follower's unconfident: {}): {}",
        				getClass().getSimpleName(), sourceShard.getShardID(), 
        				duty.getLastEvent(), duty);
        	}
        }
    }

	private void analyzeReportedDuties(final Shard source, final List<ShardEntity> heartbeatDuties) {
		final Plan plan = partitionTable.getCurrentPlan();
		final Delivery delivery = plan.getDelivery(source);
		if (delivery!=null && delivery.getPlanId()==plan.getId()) {
		    boolean changed = false;
    		changed|=searchReallocations(source, heartbeatDuties, delivery);
    		changed|=searchAbsences(source, heartbeatDuties, delivery);
    		if (changed) {
    		    delivery.checkState();
    		}
    		if (!plan.getResult().isClosed() && plan.hasUnlatched()) {
    		    logger.info("{}: Plan unlatched, fwd >> distributor agent ", getClass().getSimpleName());
    		    scheduler.forward(scheduler.get(Semaphore.Action.DISTRIBUTOR));
    		}
		} else {
			logger.warn("{}: no pending Delivery for heartbeat's shard: {} - current/delivery plan id: {}/{}", 
	            getClass().getSimpleName(), source.getShardID().toString(), plan!=null ? plan.getId() : "unk", 
	            		delivery!=null ? delivery.getPlanId() : "unk");
		}
		if (plan.getResult().isClosed()) {
		    logger.info("{}: Plan finished ! (all changes in stage)", getClass().getSimpleName());
		}
	}
	
	 /*this checks partition table looking for missing duties (not declared dangling, that's diff) */
	private void declareHeartbeatAbsencesAsMissing(final Shard shard, final List<ShardEntity> hbDuties) {
		Set<ShardEntity> sortedLog = null;
		for (final ShardEntity duty : partitionTable.getStage().getDutiesByShard(shard)) {
			boolean found = false;
			for (ShardEntity hbDuty : hbDuties) {
				if (duty.equals(hbDuty)) {
					found = true;
					break;
				}
			}
			if (!found) {
			    if (sortedLog==null) {
			        sortedLog = new TreeSet<>();
			    }
				sortedLog.add(duty);
				partitionTable.getBackstage().addMissing(duty);
			}
		}
		if (sortedLog!=null) {
			logger.warn("{}: ShardID: {}, Missing Duties in heartbeat {}, Added to PartitionTable",
					getClass().getSimpleName(), shard.getShardID(), ShardEntity.toStringIds(sortedLog));
		}
	}

	/** 
	 * find the up-coming
	 * @return if there were changes 
	 */
	private boolean searchReallocations(final Shard shard, final List<ShardEntity> beatedDuties, final Delivery delivery) {
		Set<ShardEntity> sortedLogConfirmed = null;
		Set<ShardEntity> sortedLogDirty = null;
		boolean ret = false;
		for (final ShardEntity beated : beatedDuties) {
			for (ShardEntity prescripted : delivery.getDuties()) {
				if (prescripted.equals(beated)) {
					final Log expected = findConfirmationPair(beated, prescripted, shard.getShardID());
				    if (expected!=null) {
			    		if (sortedLogConfirmed==null) {
			    			sortedLogConfirmed = new TreeSet<>();
			    		}
		                sortedLogConfirmed.add(beated);
		                // remove the one holding older State
                        prescripted.getLog().addEvent(expected.getEvent(), 
                                EntityState.CONFIRMED, 
                                shard.getShardID(), 	                                
                                delivery.getPlanId());
                		if (partitionTable.getStage().writeDuty(prescripted, shard, expected.getEvent())) {
                		    ret = true;
                			// TODO warn: if a remove comes from client while a running plan, we might be avoidint it 
                			partitionTable.getBackstage().removeCrud(prescripted);
            			}
				    } else {
						final Date fact = prescripted.getLog().getLast().getHead();
						final long now = System.currentTimeMillis();
						if (now - fact.getTime() > MAX_EVENT_DATE_FOR_DIRTY) {
						    if (sortedLogDirty==null) {
						        sortedLogDirty = new TreeSet<>();
	                        }
							sortedLogDirty.add(beated);
						}
					}
				    break;
				}
			}
		}
		if (sortedLogConfirmed!=null) {
			logger.info("{}: ShardID: {}, Confirming partition event for Duties: {}", getClass().getSimpleName(),
					shard.getShardID(), ShardEntity.toStringIds(sortedLogConfirmed));
		}
		if (sortedLogDirty!=null) {
			logger.warn("{}: ShardID: {}, Reporting DIRTY partition event for Duties: {}", getClass().getSimpleName(),
					shard.getShardID(), ShardEntity.toStringIds(sortedLogDirty));
		}
		return ret;
	}

	/** @return the expected delivered event matching the beated logs */
    private Log findConfirmationPair(
            final ShardEntity beated, 
            final ShardEntity delivered,
            final ShardIdentifier shardid) {

        // beated duty must belong to current non-expired plan
        final long pid = partitionTable.getCurrentPlan().getId();
        final Log b = beated.getLog().find(pid, shardid, EntityEvent.ATTACH);
        if (b!=null) {
        	final EntityState es = b.getLastState();
        	if (es==EntityState.CONFIRMED || es == EntityState.RECEIVED) {
        		final Log d = delivered.getLog().find(pid, shardid, EntityEvent.ATTACH);
        		if (d!=null && d.getLastState()!=EntityState.CONFIRMED) {
        			return d;	
        		}
			} else {
				logger.error("{}: Reporting state: {} on Duty: {}", getClass().getSimpleName(), es);
        	}
        }
        return null;
    }
	
	/** treat un-coming as detaches
	/** @return whether or not there was an absence confirmed */
	private boolean searchAbsences(final Shard shard, final List<ShardEntity> beatedDuties, final Delivery delivery) {
	    boolean ret = false;
		Set<ShardEntity> sortedLog = null;
        final long pid = partitionTable.getCurrentPlan().getId();
        
		for (ShardEntity prescripted : delivery.getDuties()) {
			if (!beatedDuties.contains(prescripted)) {
				final Log found = prescripted.getLog().find(pid, shard.getShardID(), EntityEvent.DETACH, EntityEvent.REMOVE);
				if (found!=null && (found.getLastState()==EntityState.PENDING || found.getLastState()==EntityState.MISSING)) {
					if (sortedLog==null) {
                        sortedLog = new TreeSet<>();
                    }
                    sortedLog.add(prescripted);
                    prescripted.getLog().addEvent(found.getEvent(), 
                            EntityState.CONFIRMED,
                            shard.getShardID(),
                            pid);
                    boolean written = partitionTable.getStage().writeDuty(prescripted, shard, found.getEvent());
                    if (written && found.getEvent().isCrud()) {
                		kickFromNextStage(prescripted, found);
                		ret = true;
                    }
				}
			}
		}
		
		if (sortedLog!=null) {
			logger.info("{}: ShardID: {}, Confirming (by absence) partioning event for Duties: {}",
					getClass().getSimpleName(), shard.getShardID(), ShardEntity.toStringIds(sortedLog));
		}
		return ret;
	}

    private void kickFromNextStage(final ShardEntity prescripted, final Log log) {
        for (ShardEntity duty: partitionTable.getBackstage().getDutiesCrud()) {
        	if (duty.equals(prescripted)) {
        		final Instant lastEventOnCrud = duty.getLog().getLast().getHead().toInstant();
        		if (log.getHead().toInstant().isAfter(lastEventOnCrud)) {
        			partitionTable.getBackstage().removeCrud(prescripted);
        			break;
        		}
        	}
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
		partitionTable.getBackstage().addDangling(dangling);
	}

	private boolean presentInPartition(final ShardEntity duty) {
		final Shard shardLocation = partitionTable.getStage().getDutyLocation(duty.getDuty());
		return shardLocation != null && shardLocation.getState().isAlive();
	}

    private boolean presentInPartition(final Duty<?> duty) {
        final Shard shardLocation = partitionTable.getStage().getDutyLocation(duty);
        return shardLocation != null && shardLocation.getState().isAlive();
    }

	public void enterDutiesFromSource(final List<Duty<?>> dutiesFromSource) {
		Set<Duty<?>> sortedLog = null;
		final Iterator<Duty<?>> it = dutiesFromSource.iterator();
		while (it.hasNext()) {
			final Duty<?> duty = it.next();
			final ShardEntity pallet = partitionTable.getStage().getPalletById(duty.getPalletId());
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
					logger.info("{}: Adding New Duty: {}", getClass().getSimpleName(), newone);
					partitionTable.getBackstage().addCrudDuty(newone);
				} else {
					logger.error("{}: Skipping Duty CRUD {}: Pallet Not found (pallet id: {})", getClass().getSimpleName(),
							duty, duty.getPalletId());
				}
			}
		}
		if (sortedLog!=null) {
			logger.info("{}: Skipping Duty CRUD already in PTable: {}", getClass().getSimpleName(), sortedLog);
		}
	}

	public void enterPalletsFromSource(final List<Pallet<?>> palletsFromSource) {
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
	public void enterCRUD(ShardEntity... dutiesFromAction) {
		for (final ShardEntity entity : dutiesFromAction) {
			final boolean typeDuty = entity.getType()==ShardEntity.Type.DUTY;
			final boolean found = (typeDuty && presentInPartition(entity)) || 
					(!typeDuty && partitionTable.getStage().getPallets().contains(entity));
			final EntityEvent event = entity.getLastEvent();
			if (!event.isCrud()) {
				throw new RuntimeException("Bad call");
			}
			if ((!found && event == CREATE) || (found && event == EntityEvent.REMOVE)) {
				logger.info("{}: Registering Crud {}: {}", getClass().getSimpleName(), typeDuty ? "Duty": "Pallet", entity);
				if (typeDuty) {
					if (event == CREATE) {
						final ShardEntity pallet = partitionTable.getStage().getPalletById(entity.getDuty().getPalletId());
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
