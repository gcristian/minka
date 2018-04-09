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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.tilt.minka.api.ConsistencyException;
import io.tilt.minka.api.Duty;
import io.tilt.minka.api.Pallet;
import io.tilt.minka.core.leader.distributor.Delivery;
import io.tilt.minka.core.leader.distributor.Plan;
import io.tilt.minka.core.task.Scheduler;
import io.tilt.minka.domain.EntityEvent;
import io.tilt.minka.domain.EntityJournal.Log;
import io.tilt.minka.domain.Heartbeat;
import io.tilt.minka.domain.Shard;
import io.tilt.minka.domain.Shard.ShardState;
import io.tilt.minka.domain.ShardEntity;
import io.tilt.minka.domain.ShardIdentifier;
import io.tilt.minka.domain.EntityState;
/**
 * Maintainer and only writer of {@linkplain PartitionTable} 
 * Accounts coming heartbeats, detects anomalies
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
				detectAndWriteChanges(delivery, plan, beat, shard);
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

	private void detectAndWriteChanges(final Delivery delivery, final Plan plan, final Heartbeat beat, final Shard source) {
		long lattestPlanId = 0;
		boolean changed = false;
		if (beat.reportsDuties()) {
			lattestPlanId = latestPlan(beat);
			if (lattestPlanId==plan.getId()) {
				changed|=detectAndWriteAttachments(source, beat.getReportedCapturedDuties(), delivery);
			}
		}
		changed|=detectAndWriteDetachments(source, beat.getReportedCapturedDuties(), delivery);
		if (changed) {
		    delivery.checkState();
		} else if (lattestPlanId==plan.getId()) {
			// delivery found, no change but expected ? 
			logger.warn("{}: Ignoring shard's ({}) heartbeats with a previous Journal: {} (current: {})", 
		            getClass().getSimpleName(), source.getShardID().toString(), lattestPlanId, plan.getId());			
		}
		if (!plan.getResult().isClosed() && plan.hasUnlatched()) {
			if (logger.isInfoEnabled()) {
				logger.info("{}: Plan unlatched, fwd >> distributor agent ", getClass().getSimpleName());
			}
		    //scheduler.forward(scheduler.get(Semaphore.Action.DISTRIBUTOR));
		}
		
		if (plan.getResult().isClosed() && logger.isInfoEnabled()) {
			logger.info("{}: Plan finished ! (all changes in scheme)", getClass().getSimpleName());
		}
	}

	/* latest entity's journal plan ID among the beat */
	private long latestPlan(final Heartbeat beat) {
		long lattestPlanId = 0;
		for (ShardEntity e: beat.getReportedCapturedDuties()) {
			final long pid = e.getJournal().getLast().getPlanId();
			if (pid > lattestPlanId) {
				lattestPlanId = pid;
			}
		}
		return lattestPlanId;
	}
	
	
	/** 
	 * find the up-coming
	 * @return if there were changes 
	 */
	private boolean detectAndWriteAttachments(
			final Shard shard, 
			final List<ShardEntity> beatedDuties, 
			final Delivery delivery) {
		Set<ShardEntity> sortedLogConfirmed = null;
		Set<ShardEntity> sortedLogDirty = null;
		boolean ret = false;
		for (final ShardEntity beated : beatedDuties) {
			for (ShardEntity delivered : delivery.getDuties()) {
				if (delivered.equals(beated)) {
					final Log expected = findConfirmationPair(beated, delivered, shard.getShardID());
				    if (expected!=null) {
				    	if (logger.isInfoEnabled()) {
				    		if (sortedLogConfirmed==null) {
				    			sortedLogConfirmed = new TreeSet<>();
				    		}
			                sortedLogConfirmed.add(beated);
				    	}
                        delivered.getJournal().addEvent(expected.getEvent(), 
                                EntityState.CONFIRMED, 
                                shard.getShardID(),
                                delivery.getPlanId());
                		if (partitionTable.getScheme().writeDuty(delivered, shard, expected.getEvent())) {
                		    ret = true;
                			// TODO warn: if a remove comes from client while a running plan, we might be avoidint it 
                			partitionTable.getBackstage().removeCrud(delivered);
            			}
				    } else {
						final Date fact = delivered.getJournal().getLast().getHead();
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
    	Log ret = null;
    	// beated duty must belong to current non-expired plan
		final long pid = partitionTable.getCurrentPlan().getId();
		final Log beatedLog = beated.getJournal().find(pid, shardid, EntityEvent.ATTACH);
		if (beatedLog != null) {
			final EntityState beatedState = beatedLog.getLastState();
			if (beatedState == EntityState.CONFIRMED || beatedState == EntityState.RECEIVED) {
				final Log deliLog = delivered.getJournal().find(pid, shardid, EntityEvent.ATTACH);
				if (deliLog != null) {
					final EntityState deliState = deliLog.getLastState();
					if (deliState == EntityState.PENDING || deliState !=EntityState.CONFIRMED) {
						// expected normal situation
						ret = deliLog;
					} else if (deliState == EntityState.CONFIRMED) {
						// when cluster unstable: bad state but possible 
						final Shard location = partitionTable.getScheme().getDutyLocation(delivered.getDuty());
						if (location==null || !location.getShardID().equals(shardid)) {
							ret = deliLog;	
						}
					} else {
						logger.error("{}: confirmation pair found erroneous state: {} on duty: {}",
								getClass().getSimpleName(), deliState, delivered);
					}
				}
			} else {
				logger.error("{}: Reporting state: {} on Duty: {}", getClass().getSimpleName(), beatedState);
			}
		}
        return ret;
    }
	
	/** treat un-coming as detaches
	/** @return whether or not there was an absence confirmed */
	private boolean detectAndWriteDetachments(final Shard shard, final List<ShardEntity> beatedDuties, final Delivery delivery) {
	    boolean ret = false;
		Set<ShardEntity> sortedLog = null;
        final long pid = partitionTable.getCurrentPlan().getId();
        
		for (ShardEntity prescripted : delivery.getDuties()) {
			if (!beatedDuties.contains(prescripted)) {
				final Log found = prescripted.getJournal().find(pid, shard.getShardID(), EntityEvent.DETACH, EntityEvent.REMOVE);
				if (found!=null && (found.getLastState()==EntityState.PENDING || found.getLastState()==EntityState.MISSING)) {
					if (logger.isInfoEnabled()) {
						if (sortedLog==null) {
							sortedLog = new TreeSet<>();
						}
						sortedLog.add(prescripted);
					}
					prescripted.getJournal().addEvent(found.getEvent(), 
							EntityState.CONFIRMED,
							shard.getShardID(),
							pid);
					boolean written = partitionTable.getScheme().writeDuty(prescripted, shard, found.getEvent());
					if (written && found.getEvent().isCrud()) {
						// remove it from the backstage
						for (ShardEntity duty : partitionTable.getBackstage().getDutiesCrud()) {
							if (duty.equals(prescripted)) {
								final Instant lastEventOnCrud = duty.getJournal().getLast().getHead().toInstant();
								if (found.getHead().toInstant().isAfter(lastEventOnCrud)) {
									partitionTable.getBackstage().removeCrud(prescripted);
									break;
								}
							}
						}
	
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
