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

import static io.tilt.minka.core.leader.distributor.ChangePlan.Result.CLOSED_EXPIRED;
import static io.tilt.minka.core.leader.distributor.ChangePlan.Result.CLOSED_OBSOLETE;
import static io.tilt.minka.core.leader.distributor.ChangePlan.Result.RETRYING;

import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import org.apache.commons.lang.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.tilt.minka.api.Config;
import io.tilt.minka.api.Duty;
import io.tilt.minka.api.DutyBuilder.Task;
import io.tilt.minka.api.Pallet;
import io.tilt.minka.api.Pallet.Storage;
import io.tilt.minka.api.Reply;
import io.tilt.minka.broker.EventBroker;
import io.tilt.minka.core.leader.EntityDao;
import io.tilt.minka.core.leader.balancer.Balancer;
import io.tilt.minka.core.leader.data.SchemeRepository;
import io.tilt.minka.core.leader.data.ShardingScheme;
import io.tilt.minka.core.leader.data.ShardingScheme.ClusterHealth;
import io.tilt.minka.core.leader.distributor.ChangePlan.Result;
import io.tilt.minka.core.task.LeaderShardContainer;
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
import io.tilt.minka.domain.Shard;
import io.tilt.minka.domain.Shard.ShardState;
import io.tilt.minka.domain.ShardEntity;
import io.tilt.minka.domain.ShardIdentifier;
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
	private final ShardingScheme shardingScheme;
	private final SchemeRepository schemeRepository;
	private final ShardIdentifier shardId;
	private final EntityDao entityDao;
	private final DependencyPlaceholder dependencyPlaceholder;
	private final LeaderShardContainer leaderShardContainer;
    private final Agent distributor;

	private boolean initialAdding;
	private int counterForReloads;
	private int counterForDistro;
	private ChangePlanFactory factory;
	private Instant lastStealthBlocked;


	Distributor(
			final Config config, 
			final Scheduler scheduler, 
			final EventBroker eventBroker,
			final ShardingScheme shardingScheme, 
			final SchemeRepository schemeRepo,
			final ShardIdentifier shardId,
			final EntityDao dutyDao, 
			final DependencyPlaceholder dependencyPlaceholder, 
			final LeaderShardContainer leaderShardContainer) {

		this.config = config;
		this.scheduler = scheduler;
		this.eventBroker = eventBroker;
		this.shardingScheme = shardingScheme;
		this.schemeRepository = schemeRepo;
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
			if (!leaderShardContainer.imLeader()) {
				logger.warn("{}: ({}) Suspending distribution: not leader anymore ! ", getName(), shardId);
				return;
			}
			final boolean changes = config.getDistributor().isRunOnStealthMode() &&
					(shardingScheme.getScheme().isStealthChange() 
					|| shardingScheme.getBackstage().isStealthChange());
			// skip if unstable unless a plan in progress or expirations will occurr and dismiss
			final ChangePlan currPlan = shardingScheme.getCurrentPlan();
			
			final boolean noPlan = currPlan==null || currPlan.getResult().isClosed();
			
			if (noPlan && shardingScheme.getShardsHealth() == ClusterHealth.UNSTABLE) {
				logger.warn("{}: ({}) Suspending distribution until reaching cluster stability", getName(), shardId);
				return;
			} else if (!changes && noPlan) {
				return;
			} else if (changes && noPlan) {
				if (shardingScheme.getBackstage().isStealthChange()) {
					final long threshold = config.beatToMs(config.getDistributor().getStealthHoldThresholdBeats());
					if (!shardingScheme.getBackstage().stealthOverThreshold(threshold)) {						
						if (lastStealthBlocked==null) {
							lastStealthBlocked = Instant.now();
						} else if (System.currentTimeMillis() - lastStealthBlocked.toEpochMilli() > 
							config.beatToMs(config.getDistributor().getDelayBeats() * 5)) {
							lastStealthBlocked = null;
							logger.warn("{}: Phase release: threshold ", getName());
						} else {
							logger.info("{}: Phase hold: backstage's stealth-change over time distance threshold", getName());
							return;
						}
					}					
				}
			}
			
			final int online = shardingScheme.getScheme().shardsSize(ShardState.ONLINE.filter());
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
				p.calculateState();
			}
			firstTime = false;
		}
	}


	/** @return a plan to drive built at balancer's request */
	private ChangePlan buildPlan(final ChangePlan previous) {
		final ChangePlan changePlan = factory.create(shardingScheme, previous);
		if (null!=changePlan) {
			shardingScheme.setPlan(changePlan);
			this.shardingScheme.setDistributionHealth(ClusterHealth.UNSTABLE);
			changePlan.prepare();
			shardingScheme.getBackstage().dropSnapshot();
			if (logger.isInfoEnabled()) {
				logger.info("{}: Balancer generated issues on ChangePlan: {}", getName(), changePlan.getId());
			}
			return changePlan;
		} else {
			this.shardingScheme.setDistributionHealth(ClusterHealth.STABLE);
			if (logger.isInfoEnabled()) {
				logger.info("{}: Distribution in Balance ", getName(), LogUtils.BALANCED_CHAR);
			}			
			shardingScheme.getScheme().stealthChange(false);
			shardingScheme.getBackstage().setStealthChange(false);
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
		return shardingScheme.getScheme().filterShards(
				sh->sh.equals(d.getShard()) && sh.getState().isAlive());							
	}

	/** @return if plan is still valid */
	private boolean push(final ChangePlan changePlan, final Delivery delivery, final boolean retrying) {
		// check it's still in ptable
		if (!deliveryShardValid(delivery)) {
			logger.error("{}: ShardingScheme lost transport's target shard: {}", getName(), delivery.getShard());
			changePlan.obsolete();
			return false;
		} else {
			final List<ShardEntity> payload = new ArrayList<>();
			final List<Log> logs = new ArrayList<>();
			final BiConsumer<ShardEntity, Log> bc = (e, l)-> { payload.add(e); logs.add(l); };
			int deliCount = (retrying)  ? delivery.contentsByState(EntityState.PENDING, bc) :  delivery.contentsByState(bc);
			if (deliCount == 0) {
				throw new IllegalStateException("delivery with no duties to send ?");
			}
			if (logger.isInfoEnabled()) {
				logger.info("{}: {} to Shard: {} Duties ({}): {}", getName(), delivery.getEvent().toVerb(),
						delivery.getShard().getShardID(), deliCount,
						ShardEntity.toStringIds(payload));
			}	
			logs.forEach(l->l.addState(EntityState.PENDING));
			if (eventBroker.send(delivery.getShard().getBrokerChannel(), (List)payload)) {
				// dont mark to wait for those already confirmed (from fallen shards)
				delivery.markSent();
			} else {
				logs.forEach(l->l.addState(EntityState.PREPARED));
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
						
			final String master = config.getConsistency().getDutyStorage() == Storage.MINKA_MANAGEMENT ? 
					"Minka storage" : "PartitionMaster";
			if (pallets == null || pallets.isEmpty()) {
				logger.warn("{}: EventMapper user's supplier hasn't return any pallets {}",getName(), master,  pallets);
				initialAdding = false;
			} else {				
				schemeRepository.saveAllPalletsRaw(pallets, logger("Pallet"));
			}
			
			if (duties == null || duties.isEmpty()) {
				logger.warn("{}: EventMapper user's supplier hasn't return any duties: {}",getName(), master, duties);
				initialAdding = false;
			} else {
				try {
					duties.forEach(d -> Task.validateBuiltParams(d));
				} catch (Exception e) {
					logger.error("{}: Distribution suspended - Duty Built construction problem: ", getName(), e);
					return false;
				}
				schemeRepository.saveAllDutiesRaw(duties, logger("Duty"));
				initialAdding = false;
			}

			if (shardingScheme.getBackstage().getDutiesCrud().isEmpty()) {
				logger.warn("{}: Aborting first distribution (no CRUD duties)", getName());
				return false;
			} else {
				logger.info("{}: {} reported {} entities for sharding...", getName(), master, duties.size());
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
		if (config.getDistributor().isRunConsistencyCheck() && shardingScheme.getCurrentPlan().areShippingsEmpty()) {
			// only warn in case there's no reallocation ahead
			final Set<ShardEntity> sorted = new TreeSet<>();
			for (Duty<?> duty: reloadDutiesFromStorage()) {
				final ShardEntity entity = ShardEntity.Builder.builder(duty).build();
				if (!shardingScheme.getScheme().dutyExists(entity)) {
					sorted.add(entity);
				}
			}
			if (!sorted.isEmpty()) {
				logger.error("{}: Consistency check: Absent duties going as Missing [ {}]", getName(),
						ShardEntity.toStringIds(sorted));
				shardingScheme.getBackstage().addMissing(sorted);
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
		final Set<ShardEntity> updates = shardingScheme.getBackstage().getDutiesCrud().stream()
				.filter(i -> i.getJournal().getLast().getEvent() == EntityEvent.UPDATE 
					&& i.getJournal().getLast().getLastState() == EntityState.PREPARED)
				.collect(Collectors.toCollection(HashSet::new));
		if (!updates.isEmpty()) {
			for (ShardEntity updatedDuty : updates) {
				Shard location = shardingScheme.getScheme().findDutyLocation(updatedDuty);
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
		shardingScheme.logStatus();
	}
}
