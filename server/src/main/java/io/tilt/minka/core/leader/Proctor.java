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
import static io.tilt.minka.core.leader.PartitionScheme.ClusterHealth.STABLE;
import static io.tilt.minka.core.leader.PartitionScheme.ClusterHealth.UNSTABLE;
import static io.tilt.minka.utils.LogUtils.HEALTH_DOWN;
import static io.tilt.minka.utils.LogUtils.HEALTH_UP;
import static java.time.Instant.now;
import static java.util.Objects.requireNonNull;

import java.time.Instant;
import java.util.ArrayList;
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
import io.tilt.minka.core.leader.PartitionScheme.ClusterHealth;
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
	private final PartitionScheme partitionScheme;
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
			final PartitionScheme partitionScheme, 
			final SchemeSentry bookkeeper, 
			final EventBroker eventBroker, 
			final Scheduler scheduler, 
			final NetworkShardIdentifier shardId, 
			final LeaderShardContainer leaderShardContainer) {

		this.config = requireNonNull(config);
		this.partitionScheme = requireNonNull(partitionScheme);
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

	private void clearShards() {
		try {
			if (logger.isDebugEnabled()) {
				logger.debug("{}: Blessing {} shards", getName(), partitionScheme.getScheme().shardsSize(null));
			}
			final DomainInfo dom = new DomainInfo();
			final Set<ShardEntity> allPallets = new HashSet<>();
			partitionScheme.getScheme().onPallets(allPallets::add);
			dom.setDomainPallets(allPallets);
			
			partitionScheme.getScheme().onShards(ShardState.GONE.negative(), 
					shard-> eventBroker.send(
							shard.getBrokerChannel(), 
							EVENT_SET, 
							Clearance.create(shardId, dom)));
		} catch (Exception e) {
			logger.error("{}: Unexpected while blessing", getName(), e);
		}
	}
	
	private void shepherShards() {
		try {
			if (!leaderShardContainer.imLeader()) {
				return;
			}
			final int size = partitionScheme.getScheme().shardsSize();
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
		final List<Runnable> actions = new ArrayList<>(size);
		partitionScheme.getScheme().onShards(null, shard-> {
			final String[] ressume = new String[1];
			final ShardState newState = evaluateStateThruHeartbeats(shard, s->ressume[0]=s);
			final ShardState priorState = shard.getState();
			if (newState != priorState) {
				logChangeAndTitle(ressume, actions.isEmpty(), size);
				lastUnstableAnalysisId = analysisCounter;
				actions.add(()->schemeSentry.shardStateChange(shard, priorState, newState));
			} else if (lastAnalysys.isBefore(shard.getFirstTimeSeen())) {
				logChangeAndTitle(ressume, actions.isEmpty(), size);
			}

			sizeOnline[0] += priorState == ShardState.ONLINE || newState == ShardState.ONLINE ? 1 : 0;
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
		if (health != partitionScheme.getShardsHealth()) {
			partitionScheme.setShardsHealth(health);
			logger.warn("{}: Cluster back to: {} ({}, min unchanged analyses: {})", getName(),
					partitionScheme.getShardsHealth(), lastUnstableAnalysisId,
					threshold);
		}
	}
	
	private ShardState evaluateStateThruHeartbeats(final Shard shard, final Consumer<String> cons) {
		final long now = System.currentTimeMillis();
		final long normalDelay = config.beatToMs(config.getFollower().getHeartbeatDelayBeats());
		final long configuredLapse = config.beatToMs(config.getProctor().getHeartbeatLapseBeats());
		final long lapseStart = now - configuredLapse;
		//long minMandatoryHBs = configuredLapse / normalDelay;

		final ShardState currentState = shard.getState();
		ShardState newState = currentState;
		LinkedList<Heartbeat> pastLapse = null;
		String msg = "";

		final int minHealthlyToGoOnline = config.getProctor().getMinHealthlyHeartbeatsForShardOnline();
		final int minToBeGone = config.getProctor().getMinAbsentHeartbeatsBeforeShardGone();
		final int maxSickToGoQuarantine = config.getProctor().getMaxSickHeartbeatsBeforeShardQuarantine();

		if (shard.getHeartbeats().size() < minToBeGone) {
			final long max = config.beatToMs(config.getProctor().getMaxShardJoiningStateBeats());
			if (shard.getLastStatusChange().plus(max).isBeforeNow()) {
				msg = "try joining expired";
				newState = ShardState.GONE;
			} else {
				msg = "no enough heartbeats in lapse";
				newState = ShardState.JOINING;
			}
		} else {
			pastLapse = shard.getHeartbeats().values().stream().filter(i -> i.getCreation().isAfter(lapseStart))
					.collect(Collectors.toCollection(LinkedList::new));
			int pastLapseSize = pastLapse.size();
			if (pastLapseSize > 0 && checkHealth(now, normalDelay, pastLapse)) {
				if (pastLapseSize >= minHealthlyToGoOnline) {
					msg = "healthy lapse > = min. healthly for online";
					newState = ShardState.ONLINE;
				} else {
					msg = "healthly lapse < min. healthly for online";
					newState = ShardState.QUARANTINE;
					// how many times should we support flapping before killing it
				}
			} else {
				if (pastLapseSize > maxSickToGoQuarantine) {
					if (pastLapseSize <= minToBeGone || pastLapseSize == 0) {
						msg = "sick lapse < min to gone";
						newState = ShardState.GONE;
					} else {
						msg = "sick lapse > max. sick to stay online";
						newState = ShardState.QUARANTINE;
					}
				} else if (pastLapseSize <= minToBeGone && currentState == ShardState.QUARANTINE) {
					msg = "sick lapse < min to gone";
					newState = ShardState.GONE;
				} else if (pastLapseSize > 0 && currentState == ShardState.ONLINE) {
					msg = "sick lapse > 0 (" + pastLapseSize + ")";
					newState = ShardState.QUARANTINE;
				} else if (pastLapseSize == 0 
						&& (currentState == ShardState.QUARANTINE || currentState == ShardState.ONLINE)) {
					msg = "sick lapse (0) became ancient";
					newState = ShardState.GONE;
				} else {
					msg = "past lapse is " + pastLapseSize;
				}
			}
		}
		
		cons.accept(String.format("%s: %s %s %s, %s, (%s/%s), Seq [%s..%s] %s", getName(), shard,
			newState == currentState ? "staying" : "going", 
			newState, 
			msg, 
			shard.getHeartbeats().size(),
			pastLapse != null ? pastLapse.size() : 0, 
			shard.getHeartbeats().last().getSequenceId(),
			shard.getHeartbeats().first().getSequenceId(), 
			shardId.equals(shard.getShardID()) ? LogUtils.SPECIAL : ""));

		return newState;
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
					partitionScheme.getScheme().onShards(null, shard -> sb.append(shard).append(','));
					logger.info("{}: Health: {}, {} shard(s) going to be analyzed: {}", getName(), partitionScheme
							.getShardsHealth(), sizeShards, sb.toString());
				}
				logger.info(ressume[0]);
				ressume[0] = null;
			}
		} catch (Exception e) {
			logger.error("", e);
		}
	}
}
