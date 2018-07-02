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

import static io.tilt.minka.core.leader.data.Scheme.ClusterHealth.STABLE;
import static io.tilt.minka.core.leader.data.Scheme.ClusterHealth.UNSTABLE;
import static java.time.Instant.now;
import static java.util.Objects.requireNonNull;

import java.time.Instant;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.tilt.minka.api.Config;
import io.tilt.minka.broker.EventBroker;
import io.tilt.minka.core.leader.data.Scheme;
import io.tilt.minka.core.leader.data.Scheme.ClusterHealth;
import io.tilt.minka.core.task.LeaderAware;
import io.tilt.minka.core.task.Scheduler;
import io.tilt.minka.core.task.Scheduler.Agent;
import io.tilt.minka.core.task.Scheduler.Frequency;
import io.tilt.minka.core.task.Scheduler.PriorityLock;
import io.tilt.minka.core.task.Semaphore.Action;
import io.tilt.minka.core.task.Service;
import io.tilt.minka.domain.ShardEntity;
import io.tilt.minka.shard.Clearance;
import io.tilt.minka.shard.DomainInfo;
import io.tilt.minka.shard.NetworkShardIdentifier;
import io.tilt.minka.shard.Shard;
import io.tilt.minka.shard.ShardState;
import io.tilt.minka.shard.Transition;
import io.tilt.minka.utils.CollectionUtils.SlidingSortedSet;
import io.tilt.minka.utils.LogUtils;

/**
 * Fixes Shards behaviour into state transitions.
 * Clears Shards so they belong to the cluster.
 * 
 * @author Cristian Gonzalez
 * @since Dec 2, 2015
 */
class ShardKeeper implements Service {

	private final Logger logger = LoggerFactory.getLogger(getClass());

	private final Config config;
	private final Scheme scheme;
	private final StateWriter writer;
	private final EventBroker eventBroker;
	private final Scheduler scheduler;
	private final NetworkShardIdentifier shardId;
	private final LeaderAware leaderAware;
	private final Agent analyzer;
	private final Transitioner transitioner;

	private int analysisCounter;
	private int lastUnstableAnalysisId = 1;
	private Instant lastAnalysys;
	

	ShardKeeper(
			final Config config, 
			final Scheme scheme, 
			final StateWriter writer, 
			final EventBroker eventBroker, 
			final Scheduler scheduler, 
			final NetworkShardIdentifier shardId, 
			final LeaderAware leaderAware) {

		this.config = requireNonNull(config);
		this.writer = requireNonNull(writer);
		this.scheme = requireNonNull(scheme);
		this.eventBroker = requireNonNull(eventBroker);
		this.scheduler = requireNonNull(scheduler);
		this.shardId = requireNonNull(shardId);
		this.leaderAware = requireNonNull(leaderAware);

		this.analyzer = scheduler.getAgentFactory()
				.create(Action.PROCTOR, 
						PriorityLock.MEDIUM_BLOCKING, 
						Frequency.PERIODIC, 
						() -> shepherShards())
				.delayed(config.beatToMs(config.getProctor().getStartDelay()))
				.every(config.beatToMs(config.getProctor().getPhaseFrequency()))
				.build();
		this.lastAnalysys = now();
		this.transitioner = new Transitioner(config, new ShardBeatsHealth(config));
	}

	@Override
	public void start() {
		logger.info("{}: Starting. Scheduling constant shepherding check", getName());
		scheduler.schedule(analyzer);
	}

	@Override
	public void stop() {
		logger.info("{}: Stopping", getName());
		this.scheduler.stop(analyzer);
	}

	private void shepherShards() {
		try {
			if (!leaderAware.imLeader()) {
				return;
			}
			final int size = scheme.getCommitedState().shardsSize();
			if (size==0) {
				logger.warn("{}: Partition queue empty: no shards emiting heartbeats ?", getName());
				return;
			}
			analysisCounter++;
			
			final int online = rankShards(size);
			calculateHealth(size, online);
			clearShards();
			lastAnalysys= now();
		} catch (Exception e) {
			logger.error("{}: Unexpected while shepherdizing", getName(), e);
		}
	}
	
	private int rankShards(final int size) {
		final int[] sizeOnline = new int[1];
		final List<Runnable> actions = new LinkedList<>();
		scheme.getCommitedState().findShards(null, shard-> {
			final Transition trans = transitioner.nextTransition(
					shard.getState(), 
					(SlidingSortedSet)shard.getTransitions(), 
					shard.getHeartbeats());  
			final ShardState priorState = shard.getState();
			
			if (trans.getState() != priorState) {
				explainToLog(trans, priorState, shard, actions.isEmpty(), size);
				lastUnstableAnalysisId = analysisCounter;
				actions.add(()->writer.shardStateTransition(shard, priorState, trans));
			} else if (lastAnalysys.isBefore(shard.getFirstTimeSeen())) {
				explainToLog(trans, priorState, shard, actions.isEmpty(), size);
			}

			sizeOnline[0] += trans.getState() == ShardState.ONLINE ? 1 : 0;
		});
		// avoid failfast iterator
		if (!actions.isEmpty()) {
			actions.forEach(Runnable::run);
		}
		return sizeOnline[0];
	}

	private void calculateHealth(final int size, final int sizeOnline) {
		final int threshold = config.getProctor().getClusterHealthStabilityDelayPhases();
		ClusterHealth health = UNSTABLE;
		if (sizeOnline == size && analysisCounter - lastUnstableAnalysisId >= threshold) {
			health = STABLE;
		}
		if (health != scheme.getShardsHealth()) {
			scheme.setShardsHealth(health);
			logger.warn("{}: Cluster back to: {} ({}, min unchanged analyses: {})", getName(),
					scheme.getShardsHealth(), lastUnstableAnalysisId, threshold);
		}
	}
	
	private void explainToLog(final Transition transition, final ShardState priorState,
			final Shard shard, final boolean atFirstChange, final int sizeShards) {
		try {		
			if (logger.isInfoEnabled()) {
				if (atFirstChange) {
					logger.info(LogUtils.titleLine(new StringBuilder("Analyzing Shards (i")
							.append(analysisCounter)
							.append(") by Leader: ")
							.append(shardId.toString())
							.toString()));
					final StringBuilder sb = new StringBuilder();
					scheme.getCommitedState().findShards(null, s -> sb.append(s).append(','));
					logger.info("{}: Health: {}, {} shard(s) going to be analyzed: {}", getName(), scheme
							.getShardsHealth(), sizeShards, sb.toString());
				}
				logger.info(String.format("%s: %s %s %s, %s, (%s/%s), Seq [%s] %s", 
						getClass().getSimpleName(), shard,
						transition.getState() == priorState ? "stays" : "goes", 
						transition.getState(), 
						transition.getCause(), 
						shard.getHeartbeats().size(),
						shard.getHeartbeats().last().getSequenceId(),
						shard.getHeartbeats().first().getSequenceId(), 
						shardId.equals(shard.getShardID()) ? LogUtils.SPECIAL : ""));
			}
		} catch (Exception e) {
			logger.error("", e);
		}
	}
	
	private void clearShards() {
		try {
			if (logger.isDebugEnabled()) {
				logger.debug("{}: Blessing {} shards", getName(), scheme.getCommitedState().shardsSize(null));
			}
			final DomainInfo dom = new DomainInfo();
			final Set<ShardEntity> allPallets = new HashSet<>();
			scheme.getCommitedState().findPallets(allPallets::add);
			dom.setDomainPallets(allPallets);
			
			scheme.getCommitedState().findShards(ShardState.GONE.negative(), 
					shard-> eventBroker.send(
							shard.getBrokerChannel(), 
							Clearance.create(shardId, dom)));
		} catch (Exception e) {
			logger.error("{}: Unexpected while blessing", getName(), e);
		}
	}
}
