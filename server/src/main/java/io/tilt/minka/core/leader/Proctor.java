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

import static io.tilt.minka.broker.EventBroker.ChannelHint.EVENT_SET;
import static io.tilt.minka.core.leader.data.ShardingScheme.ClusterHealth.STABLE;
import static io.tilt.minka.core.leader.data.ShardingScheme.ClusterHealth.UNSTABLE;
import static io.tilt.minka.domain.Shard.ShardState.GONE;
import static io.tilt.minka.domain.Shard.ShardState.JOINING;
import static io.tilt.minka.domain.Shard.ShardState.ONLINE;
import static io.tilt.minka.domain.Shard.ShardState.QUARANTINE;
import static io.tilt.minka.utils.LogUtils.HEALTH_DOWN;
import static io.tilt.minka.utils.LogUtils.HEALTH_UP;
import static java.time.Instant.now;
import static java.util.Objects.requireNonNull;

import java.time.Instant;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import org.apache.commons.math3.stat.descriptive.SummaryStatistics;
import org.apache.commons.math3.util.Precision;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.tilt.minka.api.Config;
import io.tilt.minka.broker.EventBroker;
import io.tilt.minka.core.leader.data.ShardingScheme;
import io.tilt.minka.core.leader.data.ShardingScheme.ClusterHealth;
import io.tilt.minka.core.task.LeaderShardContainer;
import io.tilt.minka.core.task.Scheduler;
import io.tilt.minka.core.task.Scheduler.Agent;
import io.tilt.minka.core.task.Scheduler.Frequency;
import io.tilt.minka.core.task.Scheduler.PriorityLock;
import io.tilt.minka.core.task.Semaphore.Action;
import io.tilt.minka.core.task.Service;
import io.tilt.minka.domain.Clearance;
import io.tilt.minka.domain.DomainInfo;
import io.tilt.minka.domain.Heartbeat;
import io.tilt.minka.domain.NetworkShardIdentifier;
import io.tilt.minka.domain.Shard;
import io.tilt.minka.domain.Shard.ShardState;
import io.tilt.minka.domain.ShardEntity;
import io.tilt.minka.utils.LogUtils;

/**
 * Observes shard's heartbeats lapses and ranks them in states.
 * 
 * @author Cristian Gonzalez
 * @since Dec 2, 2015
 */
public class Proctor implements Service {

	private final Logger logger = LoggerFactory.getLogger(getClass());

	private final Config config;
	private final ShardingScheme shardingScheme;
	private final SchemeSentry schemeSentry;
	private final EventBroker eventBroker;
	private final Scheduler scheduler;
	private final NetworkShardIdentifier shardId;
	private final LeaderShardContainer leaderShardContainer;
	private final Agent analyzer;

	private int analysisCounter;
	private int lastUnstableAnalysisId = 1;
	private Instant lastAnalysys;
	

	public Proctor(
			final Config config, 
			final ShardingScheme shardingScheme, 
			final SchemeSentry bookkeeper, 
			final EventBroker eventBroker, 
			final Scheduler scheduler, 
			final NetworkShardIdentifier shardId, 
			final LeaderShardContainer leaderShardContainer) {

		this.config = requireNonNull(config);
		this.shardingScheme = requireNonNull(shardingScheme);
		this.schemeSentry = requireNonNull(bookkeeper);
		this.eventBroker = requireNonNull(eventBroker);
		this.scheduler = requireNonNull(scheduler);
		this.shardId = requireNonNull(shardId);
		this.leaderShardContainer = requireNonNull(leaderShardContainer);

		this.analyzer = scheduler.getAgentFactory()
				.create(Action.PROCTOR, 
						PriorityLock.MEDIUM_BLOCKING, 
						Frequency.PERIODIC, 
						() -> shepherShards())
				.delayed(config.beatToMs(config.getProctor().getStartDelayBeats()))
				.every(config.beatToMs(config.getProctor().getDelayBeats()))
				.build();
		this.lastAnalysys = now();
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
			if (!leaderShardContainer.imLeader()) {
				return;
			}
			final int size = shardingScheme.getScheme().shardsSize();
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
		shardingScheme.getScheme().findShards(null, shard-> {
			final String[] ressume = new String[1];
			final Shard.Change change = evaluateStateThruHeartbeats(shard, info->ressume[0]=info);
			final ShardState priorState = shard.getState();
			if (change.getState() != priorState) {
				logChangeAndTitle(ressume, actions.isEmpty(), size);
				lastUnstableAnalysisId = analysisCounter;
				actions.add(()->schemeSentry.shardStateChange(shard, priorState, change));
			} else if (lastAnalysys.isBefore(shard.getFirstTimeSeen())) {
				logChangeAndTitle(ressume, actions.isEmpty(), size);
			}

			sizeOnline[0] += change.getState() == ONLINE ? 1 : 0;
		});
		// avoid failfast iterator
		if (!actions.isEmpty()) {
			actions.forEach(Runnable::run);
		}
		return sizeOnline[0];
	}

	private void calculateHealth(final int size, final int sizeOnline) {
		final int threshold = config.getProctor().getClusterHealthStabilityDelayPeriods();
		ClusterHealth health = UNSTABLE;
		if (sizeOnline == size && analysisCounter - lastUnstableAnalysisId >= threshold) {
			health = STABLE;
		}
		if (health != shardingScheme.getShardsHealth()) {
			shardingScheme.setShardsHealth(health);
			logger.warn("{}: Cluster back to: {} ({}, min unchanged analyses: {})", getName(),
					shardingScheme.getShardsHealth(), lastUnstableAnalysisId,
					threshold);
		}
	}
	
	private Shard.Change evaluateStateThruHeartbeats(final Shard shard, final Consumer<String> cons) {
		final long now = System.currentTimeMillis();
		final long normalDelay = config.beatToMs(config.getFollower().getHeartbeatDelayBeats());
		final long configuredLapse = config.beatToMs(config.getProctor().getHeartbeatLapseBeats());
		final long lapseStart = now - configuredLapse;
		//long minMandatoryHBs = configuredLapse / normalDelay;

		final ShardState currentState = shard.getState();
		ShardState newState = currentState;
		LinkedList<Heartbeat> pastLapse = null;
		Shard.Cause cause = shard.getChanges().iterator().next().getCause();

		final int minHealthlyToGoOnline = config.getProctor().getMinHealthlyHeartbeatsForShardOnline();
		final int minToBeGone = config.getProctor().getMinAbsentHeartbeatsBeforeShardGone();
		final int maxSickToGoQuarantine = config.getProctor().getMaxSickHeartbeatsBeforeShardQuarantine();

		if (shard.getHeartbeats().size() < minToBeGone) {
			final long max = config.beatToMs(config.getProctor().getMaxShardJoiningStateBeats());
			if (shard.getLastStatusChange().plusMillis(max).isBefore(Instant.now())) {
				cause = Shard.Cause.JOINING_STARVED;
				newState = GONE;
			} else {
				cause = Shard.Cause.FEW_HEARTBEATS;
				newState = JOINING;
			}
		} else {
			pastLapse = shard.getHeartbeats().values().stream().filter(i -> i.getCreation().isAfter(lapseStart))
					.collect(Collectors.toCollection(LinkedList::new));
			int pastLapseSize = pastLapse.size();
			if (pastLapseSize > 0 && checkHealth(now, normalDelay, pastLapse)) {
				if (pastLapseSize >= minHealthlyToGoOnline) {
					cause = Shard.Cause.HEALTHLY_THRESHOLD;
					newState = ONLINE;
				} else {
					cause = Shard.Cause.HEALTHLY_THRESHOLD;
					newState = QUARANTINE;
					// how many times should we support flapping before killing it
				}
			} else {
				if (pastLapseSize > maxSickToGoQuarantine) {
					if (pastLapseSize <= minToBeGone || pastLapseSize == 0) {
						cause = Shard.Cause.MIN_ABSENT;
						newState = GONE;
					} else {
						cause = Shard.Cause.MAX_SICK_FOR_ONLINE;
						newState = QUARANTINE;
					}
				} else if (pastLapseSize <= minToBeGone && currentState == QUARANTINE) {
					cause = Shard.Cause.MIN_ABSENT;
					newState = GONE;
				} else if (pastLapseSize > 0 && currentState == ONLINE) {
					cause = Shard.Cause.SWITCH_BACK;
					newState = QUARANTINE;
				} else if (pastLapseSize == 0 
						&& (currentState == QUARANTINE || currentState == ONLINE)) {
					cause = Shard.Cause.BECAME_ANCIENT;
					newState = GONE;
				}
			}
		}
		
		cons.accept(String.format("%s: %s %s %s, %s, (%s/%s), Seq [%s..%s] %s", getName(), shard,
			newState == currentState ? "stays" : "goes", 
			newState, 
			cause, 
			shard.getHeartbeats().size(),
			pastLapse != null ? pastLapse.size() : 0, 
			shard.getHeartbeats().last().getSequenceId(),
			shard.getHeartbeats().first().getSequenceId(), 
			shardId.equals(shard.getShardID()) ? LogUtils.SPECIAL : ""));

		return new Shard.Change(cause, newState);
	}

	private boolean checkHealth(final long now, final long normalDelay, final List<Heartbeat> onTime) {
		SummaryStatistics stat = new SummaryStatistics();
		// there's some hope
		long lastCreation = onTime.get(0).getCreation().getMillis();
		long biggestDistance = 0;
		for (Heartbeat hb : onTime) {
			long creation = hb.getCreation().getMillis();
			long arriveDelay = now - creation;
			long distance = creation - lastCreation;
			stat.addValue(distance);
			lastCreation = creation;
			biggestDistance = distance > biggestDistance ? distance : biggestDistance;
			if (logger.isDebugEnabled()) {
				logger.debug("{}: HB SeqID: {}, Arrive Delay: {}ms, Distance: {}ms, Creation: {}", getName(),
						hb.getSequenceId(), arriveDelay, distance, hb.getCreation());
			}
		}

		long stdDeviationDelay = (long) Precision.round(stat.getStandardDeviation(), 2);
		long permittedStdDeviationDistance = (normalDelay
				* (long) (config.getProctor().getHeartbeatMaxDistanceStandardDeviation() * 10d) / 100);
		/*
		 * long permittedBiggestDelay = (normalDelay *
		 * (long)(config.getProctorHeartbeatMaxBiggestDistanceFactor()*10d) /
		 * 100);
		 */
		final NetworkShardIdentifier shardId = onTime.get(0).getShardId();
		boolean healthly = stdDeviationDelay < permittedStdDeviationDistance;// && biggestDelay < permittedBiggestDelay;
		if (logger.isDebugEnabled()) {
			logger.debug("{}: Shard: {}, {} Standard deviation distance: {}/{}", getName(), shardId,
					healthly ? HEALTH_UP : HEALTH_DOWN, stdDeviationDelay, permittedStdDeviationDistance);
		}
		return healthly;
	}

	private void logChangeAndTitle(final String[] ressume, final boolean atFirstChange, final int sizeShards) {
		try {
			if (ressume[0] != null && logger.isInfoEnabled()) {
				if (atFirstChange) {
					logger.info(LogUtils.titleLine(new StringBuilder("Analyzing Shards (i")
							.append(analysisCounter)
							.append(") by Leader: ")
							.append(shardId.toString())
							.toString()));
					final StringBuilder sb = new StringBuilder();
					shardingScheme.getScheme().findShards(null, shard -> sb.append(shard).append(','));
					logger.info("{}: Health: {}, {} shard(s) going to be analyzed: {}", getName(), shardingScheme
							.getShardsHealth(), sizeShards, sb.toString());
				}
				logger.info(ressume[0]);
				ressume[0] = null;
			}
		} catch (Exception e) {
			logger.error("", e);
		}
	}
	
	private void clearShards() {
		try {
			if (logger.isDebugEnabled()) {
				logger.debug("{}: Blessing {} shards", getName(), shardingScheme.getScheme().shardsSize(null));
			}
			final DomainInfo dom = new DomainInfo();
			final Set<ShardEntity> allPallets = new HashSet<>();
			shardingScheme.getScheme().findPallets(allPallets::add);
			dom.setDomainPallets(allPallets);
			
			shardingScheme.getScheme().findShards(ShardState.GONE.negative(), 
					shard-> eventBroker.send(
							shard.getBrokerChannel(), 
							EVENT_SET, 
							Clearance.create(shardId, dom)));
		} catch (Exception e) {
			logger.error("{}: Unexpected while blessing", getName(), e);
		}
	}
}
