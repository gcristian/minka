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

import static io.tilt.minka.core.leader.distributor.Plan.Result.CLOSED_EXPIRED;
import static io.tilt.minka.core.leader.distributor.Plan.Result.CLOSED_OBSOLETE;
import static io.tilt.minka.core.leader.distributor.Plan.Result.RETRYING;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

import org.apache.commons.lang.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.tilt.minka.api.Config;
import io.tilt.minka.api.DependencyPlaceholder;
import io.tilt.minka.api.Duty;
import io.tilt.minka.api.DutyBuilder.Task;
import io.tilt.minka.api.Pallet;
import io.tilt.minka.api.Pallet.Storage;
import io.tilt.minka.broker.EventBroker;
import io.tilt.minka.core.leader.SchemeSentry;
import io.tilt.minka.core.leader.EntityDao;
import io.tilt.minka.core.leader.PartitionTable;
import io.tilt.minka.core.leader.PartitionTable.ClusterHealth;
import io.tilt.minka.core.leader.balancer.Balancer;
import io.tilt.minka.core.leader.distributor.Plan.Result;
import io.tilt.minka.core.task.LeaderShardContainer;
import io.tilt.minka.core.task.Scheduler;
import io.tilt.minka.core.task.Scheduler.Agent;
import io.tilt.minka.core.task.Scheduler.Frequency;
import io.tilt.minka.core.task.Scheduler.PriorityLock;
import io.tilt.minka.core.task.Semaphore.Action;
import io.tilt.minka.core.task.Service;
import io.tilt.minka.domain.EntityEvent;
import io.tilt.minka.domain.EntityState;
import io.tilt.minka.domain.EntityJournal.Log;
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
public class Distributor implements Service {

	private final Logger logger = LoggerFactory.getLogger(getClass());

    private final Config config;
	private final Scheduler scheduler;
	private final EventBroker eventBroker;
	private final PartitionTable partitionTable;
	private final SchemeSentry bookkeeper;
	private final ShardIdentifier shardId;
	private final EntityDao entityDao;
	private final DependencyPlaceholder dependencyPlaceholder;
	private final LeaderShardContainer leaderShardContainer;
    private final Agent distributor;

	
	private boolean initialAdding;
	private int counterForReloads;
	private int counterForDistro;
	private PlanFactory planner;


	Distributor(
			final Config config, 
			final Scheduler scheduler, 
			final EventBroker eventBroker,
			final PartitionTable partitionTable, 
			final SchemeSentry bookkeeper, 
			final ShardIdentifier shardId,
			final EntityDao dutyDao, 
			final DependencyPlaceholder dependencyPlaceholder, 
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
				.create(Action.DISTRIBUTOR, 
						PriorityLock.MEDIUM_BLOCKING, 
						Frequency.PERIODIC, 
						() -> distribute())
				.delayed(config.beatToMs(config.getDistributor().getStartDelayBeats()))
				.every(config.beatToMs(config.getDistributor().getDelayBeats()))
				.build();

		this.planner = new PlanFactory(config);
	}

	@java.lang.Override
	public void start() {
		if (logger.isInfoEnabled()) {
			logger.info("{}: Starting. Scheduling constant periodic check", getName());
		}
		scheduler.schedule(distributor);
	}

	@java.lang.Override
	public void stop() {
		if (logger.isInfoEnabled()) {
			logger.info("{}: Stopping", getName());
		}
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
			final Plan currPlan = partitionTable.getCurrentPlan();
			if ((currPlan==null || currPlan.getResult().isClosed()) && 
			        partitionTable.getVisibilityHealth() == ClusterHealth.UNSTABLE) {
				logger.warn("{}: ({}) Posponing distribution until reaching cluster stability (", getName(), shardId);
				return;
			}
			logStatus();
			final int online = partitionTable.getScheme().getShardsByState(ShardState.ONLINE).size();
			final int min = config.getProctor().getMinShardsOnlineBeforeSharding();
			if (online < min) {
				logger.warn("{}: balancing posponed: not enough online shards (min:{}, now:{})", getName(), min, online);
				return;
			}
			if (!loadFromClientWhenAllOnlines()) {
				return;
			}

			// distribution
			drive(currPlan);
			communicateUpdates();
		} catch (Exception e) {
			logger.error("{}: Unexpected ", getName(), e);
		} finally {
			if (logger.isInfoEnabled()) {
				logger.info(LogUtils.END_LINE);
			}
		}
	}

	
	/**
	 * attempt to push deliveries ready until latch
	 * retry plan re-build and repush: 3 times, if it becomes obsolete or expired.
	 */
	private void drive(final Plan plan) {
		boolean rebuild = plan == null || plan.getResult().isClosed();
		boolean firstTime = true;
		Plan p = plan;
		Result r = null;
		while (firstTime || rebuild) {
			if (rebuild) {
				rebuild = false;
				p = buildPlan(p);
			}
			if (p != null && !p.getResult().isClosed()) {
				if (r == RETRYING) {
					repushPendings(p);
				} else {
					pushAvailable(p);
				}
				r = p.getResult();
			}
			if (r == CLOSED_EXPIRED || r == CLOSED_OBSOLETE) {
				rebuild = true;
			} else if (p != null) {
				p.computeState();
			}
			firstTime = false;
		}
	}


	/** @return a plan to drive built at balancer's request */
	private Plan buildPlan(final Plan previous) {
		final Plan plan = planner.create(partitionTable, previous);
		partitionTable.getBackstage().cleanAllocatedDanglings();
		if (null!=plan) {
			partitionTable.addPlan(plan);
			this.partitionTable.setWorkingHealth(ClusterHealth.UNSTABLE);
			plan.prepare();
			if (logger.isInfoEnabled()) {
				logger.info("{}: Balancer generated issues on Plan: {}", getName(), plan.getId());
			}
			return plan;
		} else {
			this.partitionTable.setWorkingHealth(ClusterHealth.STABLE);
			if (logger.isInfoEnabled()) {
				logger.info("{}: Distribution in Balance ", getName(), LogUtils.BALANCED_CHAR);
			}
			return null;
		}
	}
	
	/* retry already pushed deliveries with pending duties */
	private void repushPendings(final Plan plan) {
		for (Delivery delivery : plan.getAllPendings()) {
			push(plan, delivery, true);
			if (plan.getResult().isClosed()) {
				break;
			}
		}
	}
	
	/* push parallel deliveries */
	private void pushAvailable(final Plan plan) {
		if (logger.isInfoEnabled()) {
			logger.info("{}: Driving Plan: {}", getName(), plan.getId());
		}
		boolean deliveryValid = true;
		while (plan.hasNextParallel() && !plan.getResult().isClosed()) {
			deliveryValid &= push(plan, plan.next(), false);
		}
		if (deliveryValid) {
			checkAllDeliveriesValid(plan);
		}
	}

	private boolean checkAllDeliveriesValid(final Plan plan) {
		final List<Shard> onlineShards = partitionTable.getScheme().getShardsByState(ShardState.ONLINE);
		for (final Delivery d: plan.getAllPendings()) {
			if (!onlineShards.contains(d.getShard())) {
				logger.error("{}: Plan lost a target shard as online: {}", getName(), d.getShard());
				plan.obsolete();
				return false;
			}
		}
		return true;
	}

	/** @return if plan is still valid */
	private boolean push(final Plan plan, final Delivery delivery, final boolean retrying) {
		// check it's still in ptable
		if (!partitionTable.getScheme().getShardsByState(ShardState.ONLINE).contains(delivery.getShard())) {
			logger.error("{}: PartitionTable lost transport's target shard: {}", getName(), delivery.getShard());
			plan.obsolete();
			return false;
		} else {
			final Map<ShardEntity, Log> logByDuty = retrying ? delivery.getByState(EntityState.PENDING)
					: delivery.getByState();
			if (logByDuty.isEmpty()) {
				throw new IllegalStateException("delivery with no duties to send ?");
			}
			if (logger.isInfoEnabled()) {
				logger.info("{}: {} to Shard: {} Duties ({}): {}", getName(), delivery.getEvent().toVerb(),
						delivery.getShard().getShardID(), logByDuty.size(),
						ShardEntity.toStringIds(new TreeSet<>(logByDuty.keySet())));
			}
			delivery.checkState();
			if (eventBroker.send(delivery.getShard().getBrokerChannel(), (List)new ArrayList<>(logByDuty.keySet()))) {
				// dont mark to wait for those already confirmed (from fallen shards)
				for (ShardEntity duty : logByDuty.keySet()) {
					// PEND only that track of current delivery
					logByDuty.get(duty).addState(EntityState.PENDING);
				}
			} else {
				logger.error("{}: Couldnt transport current issues !!!", getName());
			}
			return true;
		}
	}

	/** @return if distribution can continue, read from storage only first time */
	private boolean loadFromClientWhenAllOnlines() {
	    final boolean reload = !initialAdding && (config.getDistributor().isReloadDutiesFromStorage()
                && config.getDistributor().getReloadDutiesFromStorageEachPeriods() == counterForReloads++);
	    
		if (initialAdding || reload) {
		    counterForReloads = 0;
			logger.info("{}: reloading duties from storage", getName());
			final Set<Duty<?>> duties = reloadDutiesFromStorage();
			final Set<Pallet<?>> pallets = reloadPalletsFromStorage();
			if (duties == null || duties.isEmpty() || pallets == null || pallets.isEmpty()) {
				logger.warn("{}: distribution posponed: {} hasn't return any entities (pallets = {}, duties: {})",getName(),
					config.getConsistency().getDutyStorage() == Storage.MINKA_MANAGEMENT ? "Minka storage" : "PartitionMaster",
					duties, pallets);
				return false;
			} else {
				try {
					duties.forEach(d -> Task.validateBuiltParams(d));
				} catch (Exception e) {
					logger.error("{}: Distribution suspended - Duty Built construction problem: ", getName(), e);
					return false;
				}
				logger.info("{}: {} reported {} entities for sharding...", getName(),
					config.getConsistency().getDutyStorage() == Storage.MINKA_MANAGEMENT ? "DutyDao" : "PartitionMaster",
					duties.size());
				bookkeeper.enterPalletsFromSource(new ArrayList<>(pallets));
				bookkeeper.enterDutiesFromSource(new ArrayList<>(duties));
				initialAdding = false;
			}
			if (partitionTable.getBackstage().getDutiesCrud().isEmpty()) {
				logger.warn("{}: Aborting first distribution cause of no duties !", getName());
				return false;
			}
		} else {
			checkUnexistingDutiesFromStorage();
		}
		return true;
	}

	/** feed missing duties with storage/scheme diff. */
	private void checkUnexistingDutiesFromStorage() {
		if (config.getDistributor().isRunConsistencyCheck() && partitionTable.getCurrentPlan().areShippingsEmpty()) {
			// only warn in case there's no reallocation ahead
			final Set<ShardEntity> currently = partitionTable.getScheme().getDuties();
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
				partitionTable.getBackstage().addMissing(sorted);
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
		final Set<ShardEntity> updates = partitionTable.getBackstage().getDutiesCrud().stream()
				.filter(i -> i.getJournal().getLast().getEvent() == EntityEvent.UPDATE 
					&& i.getJournal().getLast().getLastState() == EntityState.PREPARED)
				.collect(Collectors.toCollection(HashSet::new));
		if (!updates.isEmpty()) {
			for (ShardEntity updatedDuty : updates) {
				Shard location = partitionTable.getScheme().getDutyLocation(updatedDuty);
				logger.info("{}: Transporting (update) Duty: {} to Shard: {}", getName(), updatedDuty.toString(), 
						location.getShardID());
				if (eventBroker.send(location.getBrokerChannel(), updatedDuty)) {
					// not aware how we'll handle these 
					//updatedDuty.addState(EventTrack.State.PENDING);
				}
			}
		}
	}

	private void logStatus() {
		if (logger.isInfoEnabled()) {
			StringBuilder title = new StringBuilder("Distributor (i").append(++counterForDistro).append(" by Leader: ")
					.append(shardId.toString());
			logger.info(LogUtils.titleLine(title.toString()));
		}
		partitionTable.logStatus();
	}
}
