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
package io.tilt.minka.core.leader.distributor;

import static io.tilt.minka.core.leader.distributor.Distributor.PlanStatus.EXPIRED;
import static io.tilt.minka.core.leader.distributor.Distributor.PlanStatus.INVALID;
import static io.tilt.minka.core.leader.distributor.Distributor.PlanStatus.RETRY;
import static io.tilt.minka.core.leader.distributor.Distributor.PlanStatus.VALID;
import static io.tilt.minka.domain.ShardEntity.State.PREPARED;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

import org.apache.commons.lang.Validate;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

import io.tilt.minka.api.Config;
import io.tilt.minka.api.DependencyPlaceholder;
import io.tilt.minka.api.Duty;
import io.tilt.minka.api.DutyBuilder.Chore;
import io.tilt.minka.api.Pallet;
import io.tilt.minka.api.Pallet.Storage;
import io.tilt.minka.broker.EventBroker;
import io.tilt.minka.core.leader.Bookkeeper;
import io.tilt.minka.core.leader.EntityDao;
import io.tilt.minka.core.leader.PartitionTable;
import io.tilt.minka.core.leader.PartitionTable.ClusterHealth;
import io.tilt.minka.core.task.LeaderShardContainer;
import io.tilt.minka.core.task.Scheduler;
import io.tilt.minka.core.task.Scheduler.Agent;
import io.tilt.minka.core.task.Scheduler.Frequency;
import io.tilt.minka.core.task.Scheduler.PriorityLock;
import io.tilt.minka.core.task.Semaphore.Action;
import io.tilt.minka.core.task.impl.ServiceImpl;
import io.tilt.minka.domain.EntityEvent;
import io.tilt.minka.domain.Shard;
import io.tilt.minka.domain.Shard.ShardState;
import io.tilt.minka.domain.ShardEntity;
import io.tilt.minka.domain.ShardIdentifier;
import io.tilt.minka.utils.LogUtils;

/**
 * Periodically runs specified {@linkplain Balancer}'s over {@linkplain Pallet}'s
 * and drive the {@linkplain Plan} object if any, transporting duties.
 * 
 * @author Cristian Gonzalez
 * @since Nov 17, 2015
 */
public class Distributor extends ServiceImpl {

	private final Logger logger = LoggerFactory.getLogger(getClass());

	private final static int MAX_DRIVE_TRIES = 3;
	private final Config config;
	private final Scheduler scheduler;
	private final EventBroker eventBroker;
	private final PartitionTable partitionTable;
	private final Bookkeeper bookkeeper;
	private final ShardIdentifier shardId;
	private final EntityDao entityDao;
	private final DependencyPlaceholder dependencyPlaceholder;
	private final LeaderShardContainer leaderShardContainer;

	private int distributionCounter;
	private boolean initialAdding;
	private int counter;
	private Arranger arranger;

	private final Agent distributor;

	public Distributor(final Config config, final Scheduler scheduler, final EventBroker eventBroker,
			final PartitionTable partitionTable, final Bookkeeper bookkeeper, final ShardIdentifier shardId,
			final EntityDao dutyDao, final DependencyPlaceholder dependencyPlaceholder, 
			final LeaderShardContainer leaderShardContainer) {

		this.config = config;
		this.scheduler = scheduler;
		this.eventBroker = eventBroker;
		this.partitionTable = partitionTable;
		this.bookkeeper = bookkeeper;
		this.shardId = shardId;
		this.entityDao = dutyDao;
		this.leaderShardContainer = leaderShardContainer;

		if (config.getConsistency().getDutyStorage() == Storage.CLIENT_DEFINED) {
			Validate.notNull(dependencyPlaceholder, "When Minka not in Storage mode: a Partition Master is required");
		} else {
			Validate.isTrue(dependencyPlaceholder.getMaster() == null,
					"When Minka in Storage mode: a Partition Master must not be exposed");
		}

		this.dependencyPlaceholder = dependencyPlaceholder;
		this.initialAdding = true;

		this.distributor = scheduler.getAgentFactory()
				.create(Action.DISTRIBUTOR, PriorityLock.MEDIUM_BLOCKING, Frequency.PERIODIC, () -> distribute())
				.delayed(config.getDistributor().getStartDelayMs())
				.every(config.getDistributor().getDelayMs())
				.build();

		this.arranger = new Arranger(config);
	}

	@java.lang.Override
	public void start() {
		logger.info("{}: Starting. Scheduling constant periodic check", getName());
		scheduler.schedule(distributor);
	}

	@java.lang.Override
	public void stop() {
		logger.info("{}: Stopping", getName());
		this.scheduler.stop(distributor);
	}

	private void distribute() {
		try {
			// also if this's a de-frozening thread
			if (!leaderShardContainer.imLeader()) {
				logger.warn("{}: ({}) Posponing distribution: not leader anymore ! ", getName(), shardId);
				return;
			}
			// skip if unstable unless a plan in progress or expirations will occurr and dismiss
			Plan plan = partitionTable.getCurrentPlan();
			if ((plan==null) && // || currentPlan.isEmpty()) && 
			        partitionTable.getVisibilityHealth() == ClusterHealth.UNSTABLE) {
				logger.warn("{}: ({}) Posponing distribution until reaching cluster stability (", getName(), shardId);
				return;
			}
			logStatus();
			final int online = partitionTable.getStage().getShardsByState(ShardState.ONLINE).size();
			final int min = config.getProctor().getMinShardsOnlineBeforeSharding();
			if (online < min) {
			    logger.info("{}: balancing posponed: not enough online shards (min:{}, now:{})", getName(), min, online);
			    return;
			}
		    if (!loadFromClientWhenAllOnlines()) {
		        return;
		    }
		    
		    // distribution
            drive(plan);
			communicateUpdates();
		} catch (Exception e) {
			logger.error("{}: Unexpected ", getName(), e);
		} finally {
			logger.info(LogUtils.END_LINE);
		}
	}

    private void drive(Plan plan) {
        boolean build = !isDriveable(plan);
        PlanStatus res = null;
        int tries = 0;
        while ((res==RETRY && tries<=MAX_DRIVE_TRIES) || res!=VALID ) {
            if (build) {                    
                plan = build(res == null ? null : plan);
                res = VALID;
            }
            if (isDriveable(plan)) {
                res = pushAvailable(plan);
                if (res == VALID) {
                    res = checkExpiration(System.currentTimeMillis(), plan);
                }
                if (res == RETRY) {
                    res = repush(plan);
                }
            }
            if (res == EXPIRED || res == INVALID) {
                build = true;
            }
        }
    }

	private boolean isDriveable(final Plan plan) {
	    return plan!=null && !plan.isClosed();
	}

	private Plan build(final Plan previous) {
		final Plan plan = arranger.evaluate(partitionTable, previous);
		partitionTable.getNextStage().cleanAllocatedDanglings();
		if (!plan.areShippingsEmpty()) {
			partitionTable.addPlan(plan);
			this.partitionTable.setWorkingHealth(ClusterHealth.UNSTABLE);
			plan.prepare();
			logger.info("{}: Balancer generated issues on Plan: {}", getName(), plan.getId());
			return plan;
		} else {
			this.partitionTable.setWorkingHealth(ClusterHealth.STABLE);
			logger.info("{}: Distribution in Balance ", getName(), LogUtils.BALANCED_CHAR);
			return null;
		}
	}
	
    public enum PlanStatus {
        /* hay que regenerar uno nuevo */
        EXPIRED,
        /* hay que reintentar el drive */
        RETRY,
        /* no hay que hacer nada drive anduvo bien */
        VALID,
        /* hay cambios imposibles de hacer (ej: shard caido) */
        INVALID;
    }
	
	private PlanStatus repush(final Plan plan) {
	    for (Delivery delivery: plan.getAllPendings()) {
	        final PlanStatus res = push(delivery, true);
	        if (res == EXPIRED || res == INVALID) {
                return res;
            }
	    }
	    return VALID;
	}
	
	private PlanStatus pushAvailable(final Plan plan) {
	    logger.info("{}: Driving Plan: {}", getName(), plan.getId());
		while (plan.hasNextAvailable()) {
			final PlanStatus res = push(plan.next(), false);
			if (res == EXPIRED || res == INVALID) {
			    return res;
			}
		}
		return VALID;
	}

	/** @return if plan is still valid */
    private PlanStatus push(final Delivery delivery, final boolean retrying) {
        // check it's still in ptable
        if (partitionTable.getStage().getShardsByState(ShardState.ONLINE).contains(delivery.getShard())) {
        	final Collection<ShardEntity> duties = delivery.getByState(retrying ? ShardEntity.State.PENDING : null);
        	final Set<ShardEntity> sortedLog = new TreeSet<>(duties);
        	logger.info("{}: {} to Shard: {} Duties ({}): {}", getName(), delivery.getEvent().toVerb(),
        			delivery.getShard().getShardID(), duties.size(), ShardEntity.toStringIds(sortedLog));
        	delivery.markPending();
        	if (eventBroker.postEvents(delivery.getShard().getBrokerChannel(), new ArrayList<>(duties))) {
        		// dont mark to wait for those already confirmed (from fallen shards)
                for (ShardEntity duty: duties) {
                    if (duty.getState() == PREPARED || retrying) { 
                        duty.addEvent(ShardEntity.State.PENDING);
                    } else {
                        // is already confirmed 
                    }
                }                
        	} else {
        		logger.error("{}: Couldnt transport current issues !!!", getName());
        	}
        	return VALID;
        } else {
        	logger.error("{}: PartitionTable lost transport's target shard: {}", getName(), delivery.getShard());
        	return INVALID;
        }
    }

	private PlanStatus checkExpiration(final long now, final Plan currentPlan) {
		int maxSecs = config.getDistributor().getPlanExpirationSec();  
		final DateTime expiration = currentPlan.getCreation().plusSeconds(maxSecs);
		if (expiration.isBefore(now)) {
			if (currentPlan.getRetryCount() == config.getDistributor().getPlanMaxRetries()) {
				logger.info("{}: Abandoning Plan expired ! (max secs:{}) ", getName(), maxSecs);
				//buildAndDrivePlan(currentPlan);
				return EXPIRED;
			} else {
				currentPlan.incrementRetry();
				logger.info("{}: ReSending Plan expired: Retry {} (max secs:{}) ", getName(),
						currentPlan.getRetryCount(), maxSecs);
				//drive(currentPlan);
				return RETRY;
			}
		} else {
			logger.info("{}: Drive in progress waiting recent deliveries ({}'s to expire)", getName(),
					currentPlan.getId(), (expiration.getMillis() - now) / 1000);
			return VALID;
		}		
	}

	/** @return if distribution can continue, read from storage only first time */
	private boolean loadFromClientWhenAllOnlines() {
		if (initialAdding || (config.getDistributor().isReloadDutiesFromStorage()
				&& config.getDistributor().getReloadDutiesFromStorageEachPeriods() == counter++)) {
			logger.info("{}: reloading duties from storage", getName());
			counter = 0;
			final Set<Duty<?>> duties = reloadDutiesFromStorage();
			final Set<Pallet<?>> pallets = reloadPalletsFromStorage();
			if (duties == null || duties.isEmpty() || pallets == null || pallets.isEmpty()) {
				logger.warn("{}: distribution posponed: {} hasn't return any entities (pallets = {}, duties: {})",getName(),
					config.getConsistency().getDutyStorage() == Storage.MINKA_MANAGEMENT ? "Minka storage" : "PartitionMaster",
					duties, pallets);
				return false;
			} else {
				try {
					duties.forEach(d -> Chore.validateBuiltParams(d));
				} catch (Exception e) {
					logger.error("{}: Distribution suspended - Duty Built construction problem: ", getName(), e);
					return false;
				}
				logger.info("{}: {} reported {} entities for sharding...", getName(),
					config.getConsistency().getDutyStorage() == Storage.MINKA_MANAGEMENT ? "DutyDao" : "PartitionMaster",
					duties.size());
				final List<Pallet<?>> copyP = Lists.newArrayList(pallets);
				bookkeeper.enterPalletsFromSource(copyP);
				final List<Duty<?>> copy = Lists.newArrayList(duties);
				bookkeeper.enterDutiesFromSource(copy);
				initialAdding = false;
			}
			if (partitionTable.getNextStage().getDutiesCrud().isEmpty()) {
				logger.warn("{}: Aborting first distribution cause of no duties !", getName());
				return false;
			}
		} else {
			checkUnexistingDutiesFromStorage();
		}
		return true;
	}

	private void checkUnexistingDutiesFromStorage() {
		if (config.getDistributor().isRunConsistencyCheck() && partitionTable.getCurrentPlan().areShippingsEmpty()) {
			// only warn in case there's no reallocation ahead
			final Set<ShardEntity> currently = partitionTable.getStage().getDuties();
			final Set<ShardEntity> sorted = new TreeSet<>();
			for (Duty<?> duty: reloadDutiesFromStorage()) {
				final ShardEntity entity = ShardEntity.Builder.builder(duty).build();
				if (!currently.contains(entity)) {
					sorted.add(entity);
				}
			}
			if (!sorted.isEmpty()) {
				logger.error("{}: Consistency check: Absent duties going as Missing [ {}]", getName(),
						ShardEntity.toStringIds(sorted));
				partitionTable.getNextStage().addMissing(sorted);
			}
		}
	}

	@SuppressWarnings("unchecked")
	private Set<Duty<?>> reloadDutiesFromStorage() {
		Set<Duty<?>> duties = null;
		try {
			if (config.getConsistency().getDutyStorage() == Storage.MINKA_MANAGEMENT) {
				duties = entityDao.loadDutySnapshot();
			} else {
				duties = dependencyPlaceholder.getMaster().loadDuties();
			}
		} catch (Exception e) {
			logger.error("{}: {} throwed an Exception", getName(),
					config.getConsistency().getDutyStorage() == Storage.MINKA_MANAGEMENT ? "DutyDao" : "PartitionMaster", e);
		}
		return duties;
	}

	@SuppressWarnings("unchecked")
	private Set<Pallet<?>> reloadPalletsFromStorage() {
		Set<Pallet<?>> pallets = null;
		try {
			if (config.getConsistency().getDutyStorage() == Storage.MINKA_MANAGEMENT) {
				pallets = entityDao.loadPalletSnapshot();
			} else {
				pallets = dependencyPlaceholder.getMaster().loadPallets();
			}
		} catch (Exception e) {
			logger.error("{}: {} throwed an Exception", getName(),
					config.getConsistency().getDutyStorage()== Storage.MINKA_MANAGEMENT ? "DutyDao" : "PartitionMaster", e);
		}
		return pallets;
	}

	private void communicateUpdates() {
		final Set<ShardEntity> updates = partitionTable.getNextStage().getDutiesCrud().stream()
				.filter(i -> i.getDutyEvent() == EntityEvent.UPDATE && i.getState() == PREPARED)
				.collect(Collectors.toCollection(HashSet::new));
		if (!updates.isEmpty()) {
			for (ShardEntity updatedDuty : updates) {
				Shard location = partitionTable.getStage().getDutyLocation(updatedDuty);
				logger.info("{}: Transporting (update) Duty: {} to Shard: {}", getName(), updatedDuty.toString(), 
						location.getShardID());
				if (eventBroker.postEvent(location.getBrokerChannel(), updatedDuty)) {
					updatedDuty.addEvent(ShardEntity.State.PENDING);
				}
			}
		}
	}

    private void logStatus() {
        StringBuilder title = new StringBuilder("Distributor (i")
                .append(++distributionCounter).append(" by Leader: ").append(shardId.toString());
        logger.info(LogUtils.titleLine(title.toString()));
        partitionTable.logStatus();
    }
}
