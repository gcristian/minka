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

import static java.util.Objects.requireNonNull;

import java.time.Instant;
import java.util.Date;

import org.apache.commons.lang.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.tilt.minka.api.Config;
import io.tilt.minka.api.ConfigValidator;
import io.tilt.minka.broker.EventBroker;
import io.tilt.minka.core.follower.Follower;
import io.tilt.minka.core.leader.Leader;
import io.tilt.minka.core.task.Scheduler.Agent;
import io.tilt.minka.core.task.Scheduler.Frequency;
import io.tilt.minka.core.task.Scheduler.PriorityLock;
import io.tilt.minka.core.task.Semaphore.Action;
import io.tilt.minka.core.task.impl.SpectatorSupplier;
import io.tilt.minka.domain.DependencyPlaceholder;
import io.tilt.minka.domain.ShardIdentifier;
import io.tilt.minka.spectator.Locks;
import io.tilt.minka.spectator.ServerCandidate;
import io.tilt.minka.utils.LogUtils;

/**
 * Last singleton loaded by the spring context, starter of all instances of {@linkplain Service}
 * 
 * @author Cristian Gonzalez
 * @since Nov 5, 2015
 */
public class Bootstrap implements Service {

	private static final long REPUBLISH_LEADER_CANDIDATE_AFTER_LOST_MS = 1000l;

	private static final Logger logger = LoggerFactory.getLogger(Bootstrap.class);

	private final Config config;
	private final ConfigValidator validator;
	private final Leader leader;
	private final Follower follower;
	private final DependencyPlaceholder dependencyPlaceholder;
	private final Scheduler scheduler;
	private final LeaderShardContainer leaderShardContainer;
	private final ShardIdentifier shardId;
	private final EventBroker eventBroker;
	private final SpectatorSupplier spectatorSupplier;
	private final Agent bootLeadershipCandidate;
	private final Agent readyAwareBooting;
	
	private Date start;
	private Locks locks;
	private int repostulationCounter;

	/* starts a new shard */
	public Bootstrap(
			final Config config, 
			final ConfigValidator validator, 
			final SpectatorSupplier spectatorSupplier,
			final boolean autoStart, 
			final Leader leader, 
			final Follower follower,
			final DependencyPlaceholder dependencyPlaceholder, 
			final Scheduler scheduler,
			final LeaderShardContainer leaderShardContainer, 
			final ShardIdentifier shardId, 
			final EventBroker eventBroker) {

		Validate.notNull(config, "a unique service name is required (within the ZK ensemble)");
		this.config = config;
		this.validator = requireNonNull(validator);
		this.spectatorSupplier = requireNonNull(spectatorSupplier);
		this.leader = requireNonNull(leader);
		this.follower = requireNonNull(follower);
		this.dependencyPlaceholder = requireNonNull(dependencyPlaceholder);
		this.scheduler = requireNonNull(scheduler);
		this.leaderShardContainer = requireNonNull(leaderShardContainer);
		this.shardId = requireNonNull(shardId);
		this.eventBroker = requireNonNull(eventBroker);

		this.repostulationCounter = 0;

		this.bootLeadershipCandidate = scheduler
				.getAgentFactory().create(
						Action.BOOTSTRAP_LEADERSHIP_CANDIDATURE, 
						PriorityLock.HIGH_ISOLATED,
						Frequency.ONCE_DELAYED, 
						() -> bootLeadershipCandidate())
				.delayed(REPUBLISH_LEADER_CANDIDATE_AFTER_LOST_MS)
				.build();

		this.readyAwareBooting = scheduler
				.getAgentFactory().create(
						Action.BOOTSTRAP_BULLETPROOF_START, 
						PriorityLock.HIGH_ISOLATED,
						Frequency.ONCE_DELAYED, 
						() -> readyAwareBooting())
				.delayed(config.beatToMs(config.getBootstrap().getReadynessRetryDelayBeats()))
				.build();

		if (autoStart) {
			start();
		}
	}


	private Runnable iHaveFollowers() {
		return ()-> {
		final Instant now = Instant.now();
		if (now.minusMillis(config.beatToMs(5)).isAfter(leader.getFollowerEventsHandler().getLastBeat())) {
			// TODO release le
		}};
	}

	
	@Override
	public void start() {
	    this.start = new Date();
		logger.info(LogUtils.getGreetings(leader.getShardId(), config.getBootstrap().getServiceName(), 
				config.getBootstrap().getWebServerHostPort()));
		// check configuration is valid and not unstable-prone
		validator.validate(config, dependencyPlaceholder.getMaster());
		scheduler.start();
		leaderShardContainer.start(); // all latter services will use it
		eventBroker.start(); // enable the principal service
		locks = new Locks(spectatorSupplier.get());
		readyAwareBooting(); // start the real thing
		// after booting to avoid booting's failure race condition with restart()
		locks.setConnectionLostCallback(() -> restart());
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
			leaderShardContainer.stop();
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
		leaderShardContainer.stop();
		spectatorSupplier.renew();
		leaderShardContainer.start();
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
			logger.warn("{}: ({}) PartitionDelegate returned not ready!. Bootstrap will retry endlessly. Check: {}",
					getName(), shardId, dependencyPlaceholder.getDelegate().getClass().getName());
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
			if (!locks.runWhenLeader(getElectionName(), new ServerCandidate() {
				@Override
				public void start() {
					Bootstrap.logger.info("Bootstrap: {} Elected as Leader", leader.getShardId());
					leader.start();
					// let's getting hoppin
					/*
					 * scheduler.run(SynchronizedAgentFactory.build(Action.
					 * DISTRIBUTE_ENTITY, PriorityLock.HIGH_ISOLATED,
					 * Frequency.ONCE_DELAYED, ()-> { logger.info(
					 * "---->TESTING THE HOP HOP !");
					 * locks.stopCandidateOrLeader(getElectionName(), true);
					 * }).delayed(30000));
					 */
				}

				@Override
				public void stop() {
					leader.stop();
					if (inService()) {
						Bootstrap.logger.info("Bootstrap: {} Stopping Leader: now Candidate", leader.getShardId());
						scheduler.schedule(bootLeadershipCandidate);
					} else {
						Bootstrap.logger.info("Bootstrap: {} Stopping Leader at shutdown", leader.getShardId());
					}
				}
			})) {
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

	private String getElectionName() {
		final String electionName = "minka/" + config.getBootstrap().getServiceName() + "/leader-latch";
		return electionName;
	}

	@Override
	public boolean inService() {
		return this.start != null;
	}

}