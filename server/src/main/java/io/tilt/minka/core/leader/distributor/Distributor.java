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


import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.tilt.minka.api.Config;
import io.tilt.minka.api.Duty;
import io.tilt.minka.api.DutyBuilder.Task;
import io.tilt.minka.api.Pallet;
import io.tilt.minka.api.Reply;
import io.tilt.minka.broker.EventBroker;
import io.tilt.minka.core.leader.balancer.Balancer;
import io.tilt.minka.core.leader.data.ShardingState;
import io.tilt.minka.core.leader.data.ShardingState.ClusterHealth;
import io.tilt.minka.core.leader.data.UncommitedRepository;
import io.tilt.minka.core.task.LeaderAware;
import io.tilt.minka.core.task.Scheduler;
import io.tilt.minka.core.task.Scheduler.Agent;
import io.tilt.minka.core.task.Scheduler.Frequency;
import io.tilt.minka.core.task.Scheduler.PriorityLock;
import io.tilt.minka.core.task.Semaphore.Action;
import io.tilt.minka.core.task.Service;
import io.tilt.minka.domain.DependencyPlaceholder;
import io.tilt.minka.domain.EntityEvent;
import io.tilt.minka.domain.EntityJournal.Log;
import io.tilt.minka.domain.EntityState;
import io.tilt.minka.domain.ShardEntity;
import io.tilt.minka.shard.Shard;
import io.tilt.minka.shard.ShardIdentifier;
import io.tilt.minka.shard.ShardState;
import io.tilt.minka.utils.LogUtils;

/**
 * Periodically runs specified {@linkplain Balancer}'s over {@linkplain Pallet}'s
 * and drive the {@linkplain ChangePlan} object if any, transporting duties.
 * 
 * @author Cristian Gonzalez
 * @since Nov 17, 2015
 */
public class Distributor implements Service {

	private final Logger logger = LoggerFactory.getLogger(getClass());

	private final Config config;
	private final Scheduler scheduler;
	private final EventBroker eventBroker;
	private final ShardingState shardingState;
	private final UncommitedRepository uncommitedRepository;
	private final ShardIdentifier shardId;
	private final DependencyPlaceholder dependencyPlaceholder;
	private final LeaderAware leaderAware;
    private final Agent distributor;
	private final EntityRepository entityRepo;
	
	private boolean initialAdding;
	private int counterForReloads;
	private int counterForDistro;
	private ChangePlanFactory factory;
	private Instant lastStealthBlocked;


	Distributor(
			final Config config, 
			final Scheduler scheduler, 
			final EventBroker eventBroker,
			final ShardingState shardingState, 
			final UncommitedRepository stageRepo,
			final ShardIdentifier shardId,
			final DependencyPlaceholder dependencyPlaceholder, 
			final LeaderAware leaderAware, 
			final EntityRepository entityRepo) {

		this.config = config;
		this.scheduler = scheduler;
		this.eventBroker = eventBroker;
		this.shardingState = shardingState;
		this.uncommitedRepository = stageRepo;
		this.shardId = shardId;
		this.leaderAware = leaderAware;
		this.entityRepo = entityRepo;

		this.dependencyPlaceholder = dependencyPlaceholder;
		this.initialAdding = true;

		this.distributor = scheduler.getAgentFactory()
				.create(Action.DISTRIBUTOR, 
						PriorityLock.MEDIUM_BLOCKING, 
						Frequency.PERIODIC, 
						() -> distribute())
				.delayed(config.beatToMs(config.getDistributor().getStartDelay()))
				.every(config.beatToMs(config.getDistributor().getPhaseFrequency()))
				.build();

		this.factory = new ChangePlanFactory(config);
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
			if (!leaderAware.imLeader()) {
				logger.warn("{}: ({}) Suspending distribution: not leader anymore ! ", getName(), shardId);
				return;
			}
			final boolean changes = config.getDistributor().isRunOnStealthMode() &&
					(shardingState.getCommitedState().isStealthChange() 
					|| shardingState.getUncommited().isStealthChange());
			// skip if unstable unless a plan in progress or expirations will occurr and dismiss
			final ChangePlan currPlan = shardingState.getCurrentPlan();
			
			final boolean noPlan = currPlan==null || currPlan.getResult().isClosed();
			
			if (noPlan && shardingState.getShardsHealth() == ClusterHealth.UNSTABLE) {
				logger.warn("{}: ({}) Suspending distribution until reaching cluster stability", getName(), shardId);
				return;
			} else if (!changes && noPlan) {
				return;
			} else if (changes && noPlan) {
				if (shardingState.getUncommited().isStealthChange()) {
					final long threshold = config.beatToMs(config.getDistributor().getStealthHoldThreshold());
					if (!shardingState.getUncommited().stealthOverThreshold(threshold)) {						
						if (lastStealthBlocked==null) {
							lastStealthBlocked = Instant.now();
						} else if (System.currentTimeMillis() - lastStealthBlocked.toEpochMilli() > 
							config.beatToMs(config.getDistributor().getPhaseFrequency() * 5)) {
							lastStealthBlocked = null;
							logger.warn("{}: Phase release: threshold ", getName());
						} else {
							logger.info("{}: Phase hold: stage's stealth-change over time distance threshold", getName());
							return;
						}
					}					
				}
			}
			
			final int online = shardingState.getCommitedState().shardsSize(ShardState.ONLINE.filter());
			final int min = config.getProctor().getMinShardsOnlineBeforeSharding();
			if (online < min) {
				logger.warn("{}: Suspending distribution: not enough online shards (min:{}, now:{})", getName(), min, online);
				return;
			}
			
			if (!loadFromClientWhenAllOnlines()) {
				return;
			}
			counterForDistro++;
			logStatus();
			
			// distribution
			drive(currPlan);
			communicateUpdates();
			logger.info(LogUtils.END_LINE);
		} catch (Exception e) {
			logger.error("{}: Unexpected ", getName(), e);
		}
	}

	
	/**
	 * attempt to push deliveries ready until latch
	 * retry plan re-build and repush: 3 times, if it becomes obsolete or expired.
	 */
	private void drive(final ChangePlan changePlan) {
		boolean rebuild = changePlan == null || changePlan.getResult().isClosed();
		boolean firstTime = true;
		ChangePlan p = changePlan;
		ChangePlanState r = null;
		while (firstTime || rebuild) {
			if (rebuild) {
				rebuild = false;
				p = buildPlan(p);
			}
			if (p != null && !p.getResult().isClosed()) {
				if (r == ChangePlanState.RETRYING) {
					repushPendings(p);
				} else {
					pushAvailable(p);
				}
				r = p.getResult();
			}
			if (r == ChangePlanState.CLOSED_EXPIRED || r == ChangePlanState.CLOSED_OBSOLETE) {
				rebuild = true;
			} else if (p != null) {
				p.calculateState();
			}
			firstTime = false;
		}
	}


	/** @return a plan to drive built at balancer's request */
	private ChangePlan buildPlan(final ChangePlan previous) {
		final ChangePlan changePlan = factory.create(shardingState, previous);
		if (null!=changePlan) {
			shardingState.setPlan(changePlan);
			this.shardingState.setDistributionHealth(ClusterHealth.UNSTABLE);
			changePlan.prepare();
			shardingState.getUncommited().dropSnapshot();
			if (logger.isInfoEnabled()) {
				logger.info("{}: Balancer generated issues on ChangePlan: {}", getName(), changePlan.getId());
			}
			return changePlan;
		} else {
			this.shardingState.setDistributionHealth(ClusterHealth.STABLE);
			if (logger.isInfoEnabled()) {
				logger.info("{}: Distribution in Balance ", getName(), LogUtils.BALANCED_CHAR);
			}			
			shardingState.getCommitedState().stealthChange(false);
			shardingState.getUncommited().setStealthChange(false);
			return null;
		}
	}
	
	/* retry already pushed deliveries with pending duties */
	private void repushPendings(final ChangePlan changePlan) {
		changePlan.onDeliveries(d->d.getStep() == Delivery.Step.PENDING, delivery-> {
			if (!changePlan.getResult().isClosed()) {
				push(changePlan, delivery, true);
			}
		});
	}
	
	/* push parallel deliveries */
	private void pushAvailable(final ChangePlan changePlan) {
		if (logger.isInfoEnabled()) {
			logger.info("{}: Driving ChangePlan: {}", getName(), changePlan.getId());
		}
		boolean deliveryValid = true;
		
		// lets log when it's about to expire
		while (changePlan.hasNextParallel(logger::info) && !changePlan.getResult().isClosed()) {
			deliveryValid &= push(changePlan, changePlan.next(), false);
		}	
		if (deliveryValid) {
			checkAllDeliveriesValid(changePlan);
		}
	}

	private boolean checkAllDeliveriesValid(final ChangePlan changePlan) {
		final boolean valid[] = new boolean[] {true};
		changePlan.onDeliveries(d->d.getStep() == Delivery.Step.PENDING, d-> {
			if (!deliveryShardValid(d)) {
				logger.error("{}: ChangePlan lost a target shard as online: {}", getName(), d.getShard());
				changePlan.obsolete();
				valid[0] = false;
			}
		});
		return valid[0];
	}

	private boolean deliveryShardValid(final Delivery d) {
		return shardingState.getCommitedState().filterShards(
				sh->sh.equals(d.getShard()) && sh.getState().isAlive());							
	}
	

	/** @return if plan is still valid */
	private boolean push(final ChangePlan changePlan, final Delivery delivery, final boolean retrying) {
		// check it's still in ptable
		if (!deliveryShardValid(delivery)) {
			logger.error("{}: ShardingState lost transport's target shard: {}", getName(), delivery.getShard());
			changePlan.obsolete();
			return false;
		} else {
			final Map<ShardEntity, Log> map = new HashMap<>();
			
			final List<ShardEntity> payload = new ArrayList<>();
			final List<Log> logs = new ArrayList<>();
			final BiConsumer<ShardEntity, Log> bc = (e, l)-> { payload.add(e); logs.add(l); map.put(e, l); };
			int deliCount = (retrying)  ? delivery.contentsByState(EntityState.PENDING, bc) :  delivery.contentsByState(bc);
			if (deliCount == 0) {
				throw new IllegalStateException("delivery with no duties to send ?");
			}
			if (logger.isInfoEnabled()) {
				logger.info("{}: {} to Shard: {} Duties ({}): {}", getName(), delivery.getEvent().toVerb(),
						delivery.getShard().getShardID(), deliCount,
						ShardEntity.toStringIds(payload));
			}
			
			boolean sent = true;
			for (Map.Entry<ShardEntity, Log> e: map.entrySet()) {
				e.getKey().clearPayload();
				e.getValue().addState(EntityState.PENDING);
				if (!eventBroker.send(
				        delivery.getShard().getBrokerChannel(), 
				        e.getKey(), 
				        entityRepo.upstream(e.getKey().getDuty()))) {
					sent = false;
					e.getValue().addState(EntityState.PREPARED);
					logger.error("{}: Couldnt transport current issues !!!", getName());
				}
			}
			if (sent) {
				delivery.markSent();
			}
			
			return true;
		}
	}

	/** @return if distribution can continue, read from storage only first time */
	private boolean loadFromClientWhenAllOnlines() {
	    final boolean reload = !initialAdding && (config.getDistributor().isReloadDutiesFromStorage()
                && config.getDistributor().getDutiesReloadFromStoragePhaseFrequency() == counterForReloads++);
	    
		if (initialAdding || reload) {
		    counterForReloads = 0;
			logger.info("{}: reloading duties from storage", getName());
			final Set<Duty> duties = reloadDutiesFromStorage();
			final Set<Pallet> pallets = reloadPalletsFromStorage();
						
			if (pallets == null || pallets.isEmpty()) {
				logger.warn("{}: EventMapper user's supplier hasn't return any pallets {}",getName(), pallets);
				initialAdding = false;
			} else {				
				uncommitedRepository.saveAllPalletsRaw(pallets, logger("Pallet"));
			}
			
			if (duties == null || duties.isEmpty()) {
				logger.warn("{}: EventMapper user's supplier hasn't return any duties: {}",getName(), duties);
				initialAdding = false;
			} else {
				try {
					duties.forEach(d -> Task.validateBuiltParams(d));
				} catch (Exception e) {
					logger.error("{}: Distribution suspended - Duty Built construction problem: ", getName(), e);
					return false;
				}
				uncommitedRepository.saveAllDutiesRaw(duties, logger("Duty"));
				initialAdding = false;
			}

			if (shardingState.getUncommited().getDutiesCrud().isEmpty()) {
				logger.warn("{}: Aborting first distribution (no CRUD duties)", getName());
				return false;
			} else {
				logger.info("{}: reported {} entities for sharding...", getName(), duties.size());
			}
		} else {
			checkUnexistingDutiesFromStorage();
		}
		return true;
	}

	private Consumer<Reply> logger(final String type) {
		return (reply)-> {
			if (!reply.isSuccess()) {
				logger.info("{}: Skipping {} CRUD {} cause: {}", getName(), type, reply.getEntity(), reply.getValue());
			}
		};
	}

	/** feed missing duties with storage/scheme diff. */
	private void checkUnexistingDutiesFromStorage() {
		if (config.getDistributor().isRunConsistencyCheck() && shardingState.getCurrentPlan().areShippingsEmpty()) {
			// only warn in case there's no reallocation ahead
			final Set<ShardEntity> sorted = new TreeSet<>();
			for (Duty duty: reloadDutiesFromStorage()) {
				final ShardEntity entity = ShardEntity.Builder.builder(duty).build();
				if (!shardingState.getCommitedState().dutyExists(entity)) {
					sorted.add(entity);
				}
			}
			if (!sorted.isEmpty()) {
				logger.error("{}: Consistency check: Absent duties going as Missing [ {}]", getName(),
						ShardEntity.toStringIds(sorted));
				shardingState.getUncommited().addMissing(sorted);
			}
		}
	}

	@SuppressWarnings("unchecked")
	private Set<Duty> reloadDutiesFromStorage() {
		Set<Duty> duties = null;
		try {
			duties = dependencyPlaceholder.getMaster().loadDuties();
		} catch (Exception e) {
			logger.error("{}: throwed an Exception", getName(), e);
		}
		return duties;
	}

	@SuppressWarnings("unchecked")
	private Set<Pallet> reloadPalletsFromStorage() {
		Set<Pallet> pallets = null;
		try {
			pallets = dependencyPlaceholder.getMaster().loadPallets();
		} catch (Exception e) {
			logger.error("{}: throwed an Exception", getName(), e);
		}
		return pallets;
	}

	private void communicateUpdates() {
		final Set<ShardEntity> updates = shardingState.getUncommited().getDutiesCrud().stream()
				.filter(i -> i.getJournal().getLast().getEvent() == EntityEvent.UPDATE 
					&& i.getJournal().getLast().getLastState() == EntityState.PREPARED)
				.collect(Collectors.toCollection(HashSet::new));
		if (!updates.isEmpty()) {
			for (ShardEntity updatedDuty : updates) {
				Shard location = shardingState.getCommitedState().findDutyLocation(updatedDuty);
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
			StringBuilder title = new StringBuilder("Distributor (i").append(counterForDistro)
					.append(" by Leader: ").append(shardId.toString());
			logger.info(LogUtils.titleLine(title.toString()));
		}
		shardingState.logStatus();
	}
}
