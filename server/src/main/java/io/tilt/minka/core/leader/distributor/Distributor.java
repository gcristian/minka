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


import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.tilt.minka.api.Config;
import io.tilt.minka.api.Pallet;
import io.tilt.minka.broker.EventBroker;
import io.tilt.minka.core.leader.balancer.Balancer;
import io.tilt.minka.core.leader.data.Scheme;
import io.tilt.minka.core.leader.data.Scheme.ClusterHealth;
import io.tilt.minka.core.leader.data.UncommitedRepository;
import io.tilt.minka.core.task.LeaderAware;
import io.tilt.minka.core.task.Scheduler;
import io.tilt.minka.core.task.Scheduler.Agent;
import io.tilt.minka.core.task.Scheduler.Frequency;
import io.tilt.minka.core.task.Scheduler.PriorityLock;
import io.tilt.minka.core.task.Semaphore.Action;
import io.tilt.minka.core.task.Service;
import io.tilt.minka.domain.CommitTree.Log;
import io.tilt.minka.domain.DependencyPlaceholder;
import io.tilt.minka.domain.EntityEvent;
import io.tilt.minka.domain.EntityState;
import io.tilt.minka.domain.ShardEntity;
import io.tilt.minka.shard.Shard;
import io.tilt.minka.shard.ShardIdentifier;
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

	private final Scheduler scheduler;
	private final EventBroker eventBroker;
	private final Scheme scheme;
	private final ShardIdentifier shardId;
    private final Agent distributor;
	private final PhasePermission permission;
	private final PhaseLoader loader;
	private final ChangePlanFactory factory;

	private int counterForDistro;

	Distributor(
			final Config config, 
			final Scheduler scheduler, 
			final EventBroker eventBroker,
			final Scheme scheme, 
			final UncommitedRepository stageRepo,
			final ShardIdentifier shardId,
			final DependencyPlaceholder dependencyPlaceholder, 
			final LeaderAware leaderAware) {

		this.scheduler = scheduler;
		this.eventBroker = eventBroker;
		this.scheme = scheme;
		this.shardId = shardId;

		this.distributor = scheduler.getAgentFactory()
				.create(Action.DISTRIBUTOR, 
						PriorityLock.MEDIUM_BLOCKING, 
						Frequency.PERIODIC, 
						() -> distribute())
				.delayed(config.beatToMs(config.getDistributor().getStartDelay()))
				.every(config.beatToMs(config.getDistributor().getPhaseFrequency()))
				.build();

		this.factory = new ChangePlanFactory(config, leaderAware);
		this.permission = new PhasePermission(config, scheme, leaderAware, shardId.getId());
		this.loader = new PhaseLoader(stageRepo, dependencyPlaceholder, config, scheme);
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
			if (permission.authorize() && loader.loadDutiesOnClusterStable()) {
				counterForDistro++;
				logStatus();
				drive(scheme.getCurrentPlan());
				communicateUpdates();
				logger.info(LogUtils.END_LINE);
			}
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
		boolean justBuilt = false;
		while (firstTime || rebuild) {
			if (rebuild) {
				rebuild = false;
				p = buildPlan(p);
				justBuilt = true;
			}
			if (p != null && !p.getResult().isClosed()) {
				if (r == ChangePlanState.RETRYING) {
					repushPendings(p);
				} else {
					pushAvailable(p, justBuilt);
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
		final ChangePlan changePlan = factory.create(scheme, previous);
		if (null!=changePlan) {
			scheme.setPlan(changePlan);
			this.scheme.setDistributionHealth(ClusterHealth.UNSTABLE);			
			changePlan.prepare();
			scheme.getUncommited().dropSnapshot();
			if (logger.isInfoEnabled()) {
				logger.info("{}: Balancer generated issues on ChangePlan: {}", getName(), changePlan.getId());
			}
			return changePlan;
		} else {
			this.scheme.setDistributionHealth(ClusterHealth.STABLE);
			if (logger.isInfoEnabled()) {
				logger.info("{}: Distribution in Balance ", getName(), LogUtils.BALANCED_CHAR);
			}			
			scheme.getCommitedState().stealthChange(false);
			scheme.getUncommited().setStealthChange(false);
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
	private void pushAvailable(final ChangePlan changePlan, final boolean justBuilt) {
		if (logger.isInfoEnabled()) {
			logger.info("{}: Driving ChangePlan: {}", getName(), changePlan.getId());
		}
		boolean deliveryValid = true;
		
		// lets log when it's about to expire
		while (changePlan.hasNextParallel(justBuilt ? (msg)-> msg.length(): logger::info) 
				&& !changePlan.getResult().isClosed()) {
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
		return scheme.getCommitedState().filterShards(
				sh->sh.equals(d.getShard()) && sh.getState().isAlive());							
	}

	/** @return if plan is still valid */
	private boolean push(final ChangePlan changePlan, final Delivery delivery, final boolean retrying) {
		// check it's still in ptable
		if (!deliveryShardValid(delivery)) {
			logger.error("{}: Scheme lost transport's target shard: {}", getName(), delivery.getShard());
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

	private void communicateUpdates() {
		final Set<ShardEntity> updates = scheme.getUncommited().getDutiesCrud().stream()
				.filter(i -> i.getCommitTree().getLast().getEvent() == EntityEvent.UPDATE 
					&& i.getCommitTree().getLast().getLastState() == EntityState.PREPARED)
				.collect(Collectors.toCollection(HashSet::new));
		if (!updates.isEmpty()) {
			for (ShardEntity updatedDuty : updates) {
				Shard location = scheme.getCommitedState().findDutyLocation(updatedDuty);
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
		scheme.logStatus();
	}
}
