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
import static io.tilt.minka.core.leader.PartitionTable.ClusterHealth.STABLE;
import static io.tilt.minka.core.leader.PartitionTable.ClusterHealth.UNSTABLE;
import static io.tilt.minka.utils.LogUtils.HEALTH_DOWN;
import static io.tilt.minka.utils.LogUtils.HEALTH_UP;
import static java.util.Objects.requireNonNull;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.commons.math3.stat.descriptive.SummaryStatistics;
import org.apache.commons.math3.util.Precision;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.tilt.minka.api.Config;
import io.tilt.minka.broker.EventBroker;
import io.tilt.minka.core.leader.PartitionTable.ClusterHealth;
import io.tilt.minka.core.task.LeaderShardContainer;
import io.tilt.minka.core.task.Scheduler;
import io.tilt.minka.core.task.Scheduler.Agent;
import io.tilt.minka.core.task.Scheduler.Frequency;
import io.tilt.minka.core.task.Scheduler.PriorityLock;
import io.tilt.minka.core.task.Semaphore.Action;
import io.tilt.minka.core.task.impl.ServiceImpl;
import io.tilt.minka.domain.Clearance;
import io.tilt.minka.domain.DomainInfo;
import io.tilt.minka.domain.Heartbeat;
import io.tilt.minka.domain.NetworkShardIdentifier;
import io.tilt.minka.domain.Shard;
import io.tilt.minka.domain.Shard.ShardState;
import io.tilt.minka.utils.LogUtils;

/**
 * Basically observes shards behaviour and classifies in States that enables distribution
 * 
 * Analyze the {@linkplain PartitionTable} defining a shard's {@linkplain ShardState}
 * which in turn feeds from the {@linkplain Bookkeeper} receiving {@linkplain Heartbeat}s
 * Also sends {@linkplain Clearance} messages to authorized {@linkplain Shard}s
 * and sends {@linkplain DomainInfo} messages to all shards.
 * 
 * @author Cristian Gonzalez
 * @since Dec 2, 2015
 */
public class Proctor extends ServiceImpl {

	private final Logger logger = LoggerFactory.getLogger(getClass());

	public static final String LEADER_SHARD_PATH = "leader-shard";

	private final Config config;
	private final PartitionTable partitionTable;
	private final Bookkeeper bokkeeper;
	private final EventBroker eventBroker;
	private final Scheduler scheduler;
	private final NetworkShardIdentifier shardId;
	private final LeaderShardContainer leaderShardContainer;

	private int analysisCounter;
	private int blessCounter;
	private int lastUnstableAnalysisId;

	private final Agent analyzer;

	/*
	 * 10 mins min: previous time-window lapse to look for events to rebuild
	 * Partition Table
	 */

	public Proctor(
			final Config config, 
			final PartitionTable partitionTable, 
			final Bookkeeper bookkeeper, 
			final EventBroker eventBroker, 
			final Scheduler scheduler, 
			final NetworkShardIdentifier shardId, 
			final LeaderShardContainer leaderShardContainer) {

		this.config = requireNonNull(config);
		this.partitionTable = requireNonNull(partitionTable);
		this.bokkeeper = requireNonNull(bookkeeper);
		this.eventBroker = requireNonNull(eventBroker);
		this.scheduler = requireNonNull(scheduler);
		this.shardId = requireNonNull(shardId);
		this.leaderShardContainer = requireNonNull(leaderShardContainer);

		this.analyzer = scheduler.getAgentFactory()
				.create(Action.PROCTOR, 
						PriorityLock.MEDIUM_BLOCKING, 
						Frequency.PERIODIC, 
						() -> analyzeShards())
				.delayed(config.beatToMs(config.getProctor().getStartDelayBeats()))
				.every(config.beatToMs(config.getProctor().getDelayBeats()))
				.build();
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

	private void blessShards() {
		try {
			final List<Shard> shards = partitionTable.getStage().getShardsByState(null);
			logger.info("{}: Blessing {} shards {}", getName(), shards.size(), shards);
			shards.forEach(i -> eventBroker.postEvent(i.getBrokerChannel(), EVENT_SET, Clearance.create(shardId)));
		} catch (Exception e) {
			logger.error("{}: Unexpected while blessing", getName(), e);
		} finally {
			logger.info(LogUtils.END_LINE);
		}
	}
	
	private void sendDomainInfo() {
		for (final Shard shard: partitionTable.getStage().getShardsByState(null)) {
			if (!shard.getState().equals(ShardState.GONE)) {
				final DomainInfo dom = new DomainInfo();
				dom.setDomainPallets(partitionTable.getStage().getPallets());
				eventBroker.postEvent(shard.getBrokerChannel(), EVENT_SET, dom);
			}
		}
	}

	private void analyzeShards() {
		try {
			if (!leaderShardContainer.imLeader()) {
				return;
			}
			logger.info(LogUtils
					.titleLine("Analyzing Shards (i" + analysisCounter++ + ") by Leader: " + shardId.toString()));
			final List<Shard> shards = partitionTable.getStage().getShards();
			if (shards.isEmpty()) {
				logger.warn("{}: Partition queue empty: no shards emiting heartbeats ?", getName());
				return;
			}
			lastUnstableAnalysisId = analysisCounter == 1 ? 1 : lastUnstableAnalysisId;
			logger.info("{}: Health: {}, {} shard(s) going to be analyzed: {}", getName(),
					partitionTable.getVisibilityHealth(), shards.size(), shards);
			int sizeOnline = 0;
			for (Shard shard : shards) {
				ShardState concludedState = evaluateStateThruHeartbeats(shard);
				if (concludedState != shard.getState()) {
					lastUnstableAnalysisId = analysisCounter;
					shard.setState(concludedState);
					bokkeeper.checkShardChangingState(shard);
				}
				sizeOnline += shard.getState() == ShardState.ONLINE ? 1 : 0;
			}
			final ClusterHealth health = sizeOnline == shards.size()
					&& analysisCounter - lastUnstableAnalysisId >= config.getProctor().getClusterHealthStabilityDelayPeriods()
							? STABLE : UNSTABLE;
			if (health != partitionTable.getVisibilityHealth()) {
				partitionTable.setVisibilityHealth(health);
				logger.warn("{}: Cluster back to: {} ({}, min unchanged analyses: {})", getName(),
						partitionTable.getVisibilityHealth(), lastUnstableAnalysisId,
						config.getProctor().getClusterHealthStabilityDelayPeriods());
			}
			sendDomainInfo();
			//if (blessCounter++ > 1) {
				blessCounter = 0;
				blessShards();
			//}
		} catch (Exception e) {
			logger.error("{}: Unexpected while shepherdizing", getName(), e);
		} finally {
			logger.info(LogUtils.END_LINE);
		}
	}
	
	private ShardState evaluateStateThruHeartbeats2(final Shard shard) {
		ShardState ret = null;
		
		final List<Heartbeat> allbeats = shard.getHeartbeats();
		
		if (shard.getState()==null) {
			ret = ShardState.JOINING;
		} else {
			switch (shard.getState()) {
				case JOINING:
					// -> to any state
					break;
				case ONLINE:
					// -> to: quitted, quarantine, gone, 
					break;
				case QUARANTINE:
					// -> to: online, quitted
					break;
			}
		}
		
		return ret;
	}

	private ShardState evaluateStateThruHeartbeats(final Shard shard) {
		final long now = System.currentTimeMillis();
		final long normalDelay = config.beatToMs(config.getFollower().getHeartbeatDelayBeats());
		final long configuredLapse = config.beatToMs(config.getProctor().getHeartbeatLapseBeats());
		final long lapseStart = now - configuredLapse;
		//long minMandatoryHBs = configuredLapse / normalDelay;

		final ShardState currentState = shard.getState();
		ShardState newState = currentState;
		final List<Heartbeat> all = shard.getHeartbeats();
		LinkedList<Heartbeat> pastLapse = null;
		String msg = "";

		final int minHealthlyToGoOnline = config.getProctor().getMinHealthlyHeartbeatsForShardOnline();
		final int minToBeGone = config.getProctor().getMaxAbsentHeartbeatsBeforeShardGone();
		final int maxSickToGoQuarantine = config.getProctor().getMaxSickHeartbeatsBeforeShardQuarantine();

		if (all.size() < minToBeGone) {
			final long max = config.beatToMs(config.getProctor().getMaxShardJoiningStateBeats());
			if (shard.getLastStatusChange().plus(max).isBeforeNow()) {
				msg = "try joining expired";
				newState = ShardState.GONE;
			} else {
				msg = "no enough heartbeats in lapse";
				newState = ShardState.JOINING;
			}
		} else {
			pastLapse = all.stream().filter(i -> i.getCreation().isAfter(lapseStart))
					.collect(Collectors.toCollection(LinkedList::new));
			int pastLapseSize = pastLapse.size();
			if (pastLapseSize > 0 && checkHealth(now, normalDelay, pastLapse)) {
				if (pastLapseSize >= minHealthlyToGoOnline) {
					msg = "healthy lapse > = min. healthly for online";
					newState = ShardState.ONLINE;
				} else {
					msg = "healthly lapse < min. healthly for online";
					newState = ShardState.QUARANTINE;
					// TODO cuantas veces soporto que flapee o que este Quarantine antes de matarlo x forro ?
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
				} else if (pastLapseSize == 0 && currentState == ShardState.QUARANTINE) {
					msg = "sick lapse = 0 ";
					newState = ShardState.GONE;
				} else {
					msg = "past lapse is " + pastLapseSize;
				}
			}
		}

		logger.info("{}: {} {} {}, {}, ({}/{}), Seq [{}..{}] {}", getName(), shard,
				newState == currentState ? "stays in" : "changing to", 
				newState, 
				msg, 
				all.size(),
				pastLapse != null ? pastLapse.size() : 0, 
				all.get(all.size() - 1).getSequenceId(),
				all.get(0).getSequenceId(), 
				shardId.equals(shard.getShardID()) ? LogUtils.SPECIAL : "");

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
		if (logger.isInfoEnabled()) {
			logger.debug("{}: Shard: {}, {} Standard deviation distance: {}/{}", getName(), shardId,
					healthly ? HEALTH_UP : HEALTH_DOWN, stdDeviationDelay, permittedStdDeviationDistance);
		}
		return healthly;
	}

}
