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
package io.tilt.minka.core.task;

import static io.tilt.minka.api.config.BootstrapConfiguration.NAMESPACE_MASK_LEADER_LATCH;
import static java.util.Objects.requireNonNull;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.time.Instant;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;

import io.tilt.minka.api.Config;
import io.tilt.minka.api.ConfigValidator;
import io.tilt.minka.broker.EventBroker;
import io.tilt.minka.core.follower.Follower;
import io.tilt.minka.core.leader.Leader;
import io.tilt.minka.core.monitor.SystemStateMonitor;
import io.tilt.minka.core.task.Scheduler.Agent;
import io.tilt.minka.core.task.Scheduler.Frequency;
import io.tilt.minka.core.task.Scheduler.PriorityLock;
import io.tilt.minka.core.task.Semaphore.Action;
import io.tilt.minka.core.task.impl.SpectatorSupplier;
import io.tilt.minka.domain.DependencyPlaceholder;
import io.tilt.minka.shard.ShardIdentifier;
import io.tilt.minka.spectator.Locks;
import io.tilt.minka.spectator.ServerCandidate;

/**
 * Last singleton loaded by the spring context, starter of all instances of {@linkplain Service}
 * 
 * @author Cristian Gonzalez
 * @since Nov 5, 2015
 */
public class Bootstrap implements Service {

	
	public Logger logger = Leader.logger;
	
	private final Config config;
	private final ConfigValidator validator;
	private final Leader leader;
	private final Follower follower;
	private final DependencyPlaceholder dependencyPlaceholder;
	private final Scheduler scheduler;
	private final LeaderAware leaderAware;
	private final ShardIdentifier shardId;
	private final EventBroker eventBroker;
	private final SpectatorSupplier spectatorSupplier;
	private final String leaderLatchPath;
	
	private final Agent bootLeadershipCandidate;
	private final Agent readyAwareBooting;
	private final Agent unconfidentLeader;
	
	private Date start;
	private Locks locks;
	private int repostulationCounter;

	private ServerCandidate serverCallbacks;


	/* starts a new shard */
	Bootstrap(
			final Config config, 
			final ConfigValidator validator, 
			final SpectatorSupplier spectatorSupplier,
			final boolean autoStart, 
			final Leader leader, 
			final Follower follower,
			final DependencyPlaceholder dependencyPlaceholder, 
			final Scheduler scheduler,
			final LeaderAware leaderAware, 
			final ShardIdentifier shardId, 
			final EventBroker eventBroker) {

		this.config = requireNonNull(config, "a unique service name is required (within the ZK ensemble)");
		this.validator = requireNonNull(validator);
		this.spectatorSupplier = requireNonNull(spectatorSupplier);
		this.leader = requireNonNull(leader);
		this.follower = requireNonNull(follower);
		this.dependencyPlaceholder = requireNonNull(dependencyPlaceholder);
		this.scheduler = requireNonNull(scheduler);
		this.leaderAware = requireNonNull(leaderAware);
		this.shardId = requireNonNull(shardId);
		this.eventBroker = requireNonNull(eventBroker);

		this.repostulationCounter = 0;

		this.bootLeadershipCandidate = createCandidate();
		this.readyAwareBooting = createBootup();
		this.unconfidentLeader = createUnconfident();
		
		if (autoStart) {
			start();
		}
		this.leaderLatchPath = String.format(NAMESPACE_MASK_LEADER_LATCH, config.getBootstrap().getNamespace());
	}

	private Agent createBootup() {
		return scheduler
				.getAgentFactory().create(
						Action.BOOTSTRAP_BULLETPROOF_START, 
						PriorityLock.HIGH_ISOLATED,
						Frequency.ONCE_DELAYED, 
						() -> readyAwareBooting())
				.delayed(config.beatToMs(config.getBootstrap().getReadynessRetryFrequency()))
				.build();
	}

	private Agent createCandidate() {
		return scheduler
				.getAgentFactory().create(
						Action.BOOTSTRAP_LEADERSHIP_CANDIDATURE, 
						PriorityLock.HIGH_ISOLATED,
						Frequency.ONCE_DELAYED, 
						() -> bootLeadershipCandidate())
				.delayed(config.beatToMs(config.getBootstrap().getRepublishLeaderCandidateAfterLost()))
				.build();
	}
	
	private Agent createUnconfident() {
		return scheduler
				.getAgentFactory().create(
						Action.ANY, 
						PriorityLock.HIGH_ISOLATED,
						Frequency.PERIODIC, 
						() -> checkReceivingBeats())
				.every(config.beatToMs(config.getBootstrap().getLeaderUnconfidentStartDelay()))
				.delayed(config.beatToMs(config.getBootstrap().getLeaderUnconfidentFrequency()))
				.build();
	}
	
	@Override
	public void start() {
		
	    this.start = new Date();
		// check configuration is valid and not unstable-prone
		validator.validate(config, dependencyPlaceholder.getMaster());
		scheduler.start();
		leaderAware.start(); // all latter services will use it
		
		locks = new Locks(spectatorSupplier.get());
		readyAwareBooting(); // start the real thing
		// after booting to avoid booting's failure race condition with restart()
		locks.setConnectionLostCallback(() -> restart());
		eventBroker.start(); // enable the principal service
	}

	@Override
	/* this's the system shutdown */
	public void stop() {
		if (inService()) {
		    this.start=null;
			logger.info("{}: ({}) Destroying context..", getName(), shardId);
			if (config.getBootstrap().isLeaderShardAlsoFollows()) {
				follower.stop();
			}
			if (config.getBootstrap().isPublishLeaderCandidature() && leader.inService()) {
				leader.stop();
			}
			leaderAware.stop();
			eventBroker.stop();

			// close the task controller at last
			scheduler.stop();
			locks.close();
		} else {
			logger.warn("{}: ({}) Stopping ? I'm not in service", getName(), shardId);
		}
	}

	/**
	 * when ZK connection's lost:
	 * kill the leader and reboot
	 * let the follower keep working as it's not compromised 
	 */
	private void restart() {
		logger.warn("{}: ({}) ZK Connection's lost fallback: restarting leader and leader's shard-container", getName(),
				shardId);

		// stop current leader if on service and start the highlander booting all over again
		if (config.getBootstrap().isPublishLeaderCandidature() && leader.inService()) {
			leader.stop();
		}
		// ignore container's current held refs. and start it over
		leaderAware.stop();
		spectatorSupplier.renew();
		leaderAware.start();
		bootLeadershipCandidate();
	}

	// check if PartitionDelegate is ready and if it must: start follower and candidate leader
	private void readyAwareBooting() {
		if (dependencyPlaceholder.getDelegate().isReady()) {
			logger.info("{}: ({}) Starting...", getName(), shardId);
			if (config.getBootstrap().isLeaderShardAlsoFollows()) {
				follower.start();
			}
			bootLeadershipCandidate();
		} else {
			logger.warn("{}: ({}) Not ready ? check method Server.load() (PartitionDelegate returned not ready!. "
					+ "Bootstrap will retry endlessly)", getName(), shardId);
			scheduler.schedule(readyAwareBooting);
		}
	}
	
	/**
	 * if config allows leadership candidate: 
	 * postulate and keep retrying until postulation is accepted 
	 * even in case of losing leadership   
	 */
	private void bootLeadershipCandidate() {
		if (config.getBootstrap().isPublishLeaderCandidature()) {
			logger.info("{}: ({}) Candidating Leader (lap: {})", getName(), shardId, repostulationCounter);
			this.serverCallbacks = new ServerCandidate() {
				@Override
				public void start() {
					logger.info("Bootstrap: {} Elected as Leader", leader.getShardId());
					leader.start();
					//scheduler.schedule(unconfidentLeader);
					//for testing only: hoppingLeader(latchName);
				}
				@Override
				public void stop() {
					leader.stop();
					//scheduler.stop(unconfidentLeader);
					if (inService()) {
						logger.info("Bootstrap: {} Stopping Leader: now Candidate", leader.getShardId());
						scheduler.schedule(bootLeadershipCandidate);
					} else {
						logger.info("Bootstrap: {} Stopping Leader at shutdown", leader.getShardId());
					}
				}
			};
			final boolean promoted = locks.runWhenLeader(leaderLatchPath, serverCallbacks); 
			if (!promoted) {
				// may be ZK's down: so retry
				logger.error("{}: ({}) Leader membership rejected, retrying", getName(), shardId);
				scheduler.schedule(bootLeadershipCandidate);
			} else {
				repostulationCounter++;
			}
		} else {
			logger.info("{}: ({}) Avoiding leader candidate according Config", getName(), shardId);
		}
	}

	/** programatically make the leader serve among short lapses before hopping again */
	private void hoppingLeader() {
		scheduler.schedule(
			scheduler.getAgentFactory().create(
				Action.DISTRIBUTE_ENTITY, 
				PriorityLock.HIGH_ISOLATED,
				Frequency.ONCE_DELAYED, 
				()-> locks.stopCandidateOrLeader(leaderLatchPath, true))
			.delayed(30000)
			.build());
	}

	/* if no beats received then no true leader: stop candidate  **/ 
	private void checkReceivingBeats() {
		if (!leader.inService()) {
			return;
		}
		final Instant now = Instant.now();
		final Instant last = leader.getFollowerEventsHandler().getLastBeat();
		if (last!=null && serverCallbacks!=null) {
			if (now.minusMillis(config.beatToMs(5)).isAfter(last)) {
				logger.error("{}: ({}) No beats received recently. Cancelling leadership and candidate. (last: {})", getName(), shardId, last);
				locks.stopCandidateOrLeader(leaderLatchPath, true);
				// if ZK lost us we may be in a network partitioning: dont expect to be stopped thru callback
				serverCallbacks.stop();
			}
		}
	}

	@Override
	public boolean inService() {
		return this.start != null;
	}

}