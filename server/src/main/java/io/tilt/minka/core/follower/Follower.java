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
package io.tilt.minka.core.follower;

import static java.util.Objects.requireNonNull;

import java.time.Instant;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.tilt.minka.api.Config;
import io.tilt.minka.broker.EventBroker;
import io.tilt.minka.core.task.LeaderShardContainer;
import io.tilt.minka.core.task.Scheduler;
import io.tilt.minka.core.task.Scheduler.Agent;
import io.tilt.minka.core.task.Scheduler.Frequency;
import io.tilt.minka.core.task.Scheduler.PriorityLock;
import io.tilt.minka.core.task.Semaphore.Action;
import io.tilt.minka.core.task.Service;
import io.tilt.minka.domain.Clearance;
import io.tilt.minka.domain.Heartbeat;
import io.tilt.minka.domain.Shard;
import io.tilt.minka.domain.Shard.ShardState;
import io.tilt.minka.domain.ShardedPartition;

/**
 * An agnostic slave service with the following responsibilities:
 * 
 * 1) send heartbeats to the master
 * 2) translate to the partition commander the received sharded-entity dis/assignments from the master
 *   
 * @author Cristian Gonzalez
 * @since Nov 7, 2015
 *
 */
public class Follower implements Service {

	private final Logger logger = LoggerFactory.getLogger(getClass());
	private final String classname = getClass().getSimpleName();
	
	private boolean alive;

	private final DateTime creation;

	private final LeaderEventsHandler leaderEventsHandler;
	private final Config config;
	private final EventBroker eventBroker;

	private final Scheduler scheduler;
	private final Heartpump heartpump;
	private final HeartbeatFactory heartbeatFactory;
	private final Agent follow;
	private final ShardedPartition partition;
	private final LeaderShardContainer leaderShardContainer;
	
	public Follower(
			final Config config, 
			final Heartpump heartpump, 
			final LeaderEventsHandler leaderConsumer,
			final EventBroker eventBroker, 
			final Scheduler scheduler, 
			final HeartbeatFactory heartbeatFactory,
			final ShardedPartition partition,
			final LeaderShardContainer leaderShardContainer) {
		super();

		this.alive = true;
		this.heartpump = requireNonNull(heartpump);
		this.heartbeatFactory = requireNonNull(heartbeatFactory);
		this.leaderEventsHandler = requireNonNull(leaderConsumer);
		this.config = requireNonNull(config);
		this.eventBroker = requireNonNull(eventBroker);
		this.creation = new DateTime(DateTimeZone.UTC);
		this.scheduler = requireNonNull(scheduler);
		this.partition = requireNonNull(partition);
		this.leaderShardContainer = requireNonNull(leaderShardContainer);
		
		this.follow = scheduler.getAgentFactory()
				.create(Action.HEARTBEAT_REPORT, 
						PriorityLock.MEDIUM_BLOCKING, 
						Frequency.PERIODIC,
						() -> follow())
				.delayed(config.beatToMs(config.getFollower().getHeartbeatStartDelayBeats()))
				.every(config.beatToMs(config.getFollower().getHeartbeatDelayBeats()))
				.build();
	}

	@Override
	public void start() {
		if (logger.isInfoEnabled()) {
			logger.info("{}: ({}) Starting services", classname, config.getLoggingShardId());
		}
		this.leaderEventsHandler.start();
		alive = true;
		// after partition manager initialized: set emergency shutdown
		//eventBroker.setBrokerShutdownCallback(() -> stop());
		scheduler.schedule(follow);
	}

	@Override
	public void stop() {
		if (logger.isInfoEnabled()) {
			logger.info("{}: ({}) Stopping services, on duty since: {}", classname,
				config.getLoggingShardId(), creation);
		}
			
		if (inService()) {
			final Heartbeat bye = heartbeatFactory.create(true);
			bye.setShardChange(new Shard.Change(Shard.Cause.FOLLOWER_BREAKUP, ShardState.QUITTED));
			if (!heartpump.emit(bye)) {
				logger.error("{}: ({}) Unable to send quitting heartbeat", classname, config.getLoggingShardId());
			} else {
				logger.info("{}: ({}) Emitted quitting heartbeat: {}", classname, config.getLoggingShardId(), bye);
			}
			if (logger.isInfoEnabled()) {
				logger.info("{}: ({}) Stopping timer", classname, config.getLoggingShardId());
			}
			scheduler.stop(follow);
			alive = false;
			this.leaderEventsHandler.stop();
		} else {
			if (logger.isInfoEnabled()) {
				logger.info("{}: ({}) Follower was not longer in service", classname, config.getLoggingShardId());
			}
		}
	}
	
	private boolean lastSuccess;
	
	private void follow() {
		checkClearanceOrDrop();		
		checkBeatsOrDrop(); // avoid release twice
		
		// prevent factory of a non full detail heartbeat
		lastSuccess = heartpump.emit(heartbeatFactory.create(!lastSuccess));		
	}

	/** 
	 * Release all Duties from partition if it must.
	 * @return whether or not the clearance is valid 
	 */
	private boolean checkClearanceOrDrop() {
		boolean lost = false;
		final Clearance clear = leaderEventsHandler.getLastClearance();
		long delta = 0;
		final long hbdelay = config.beatToMs(config.getFollower().getHeartbeatDelayBeats());
		int maxAbsenceMs = (int)hbdelay * config.getProctor().getMinAbsentHeartbeatsBeforeShardGone();
		maxAbsenceMs*=eventualMultiplier();
		final int minToJoinMs = (int)hbdelay * (int)config.beatToMs(config.getProctor().getMaxShardJoiningStateBeats());
		
		final DateTime now = new DateTime(DateTimeZone.UTC);
		// it's OK if there's no clearance because just entering.. 
		if (clear != null) {
			final DateTime lastClearanceDate = clear.getCreation();
			lost = lastClearanceDate.plusMillis(maxAbsenceMs).isBefore(now);
			delta = now.getMillis() - lastClearanceDate.getMillis();
			if (lost && !partition.getDuties().isEmpty()) {
				logger.error("{}: ({}) Executing Clearance policy, last: {} too old (Max: {}, Past: {} msecs)",
					getClass().getSimpleName(), config.getLoggingShardId(),
					clear != null ? clear.getCreation() : "null", maxAbsenceMs, delta);
				leaderEventsHandler.getPartitionManager().releaseAllOnPolicies();
			} else if (!lost && logger.isDebugEnabled()) {
				logger.debug("{}: ({}) Clearence certified #{} from Leader: {}", classname,
						config.getLoggingShardId(), clear.getSequenceId(), clear.getLeaderShardId());
			}
		}
		return !lost;
	}

	/** @return give an aditional breath when leader has recently changed */
	private int eventualMultiplier() {
		int breath = 1;
		final Instant leaderChanged = leaderShardContainer.getLastLeaderChange();
		if (leaderChanged!=null && leaderChanged.isAfter(Instant.now().minusMillis(1000l))) {
			breath = 2;
		}
		return breath;
	}

	/** 
	 * Release all Duties from partition if it must.
	 * @return whether or not the pump's last beat is within a healthy time window 
	 */
	private boolean checkBeatsOrDrop() {
		if (heartpump.getLastBeat() != null) {
			final DateTime expiracy = heartpump.getLastBeat().plus(
					config.beatToMs(config.getFollower().getMaxHeartbeatAbsenceForReleaseBeats()));
			if (expiracy.isBefore(new DateTime(DateTimeZone.UTC))) {
				if (!partition.getDuties().isEmpty()) {
					logger.warn("{}: ({}) Executing Heartattack policy (last HB: {}): releasing delegate's held duties",
							getClass().getSimpleName(), config.getLoggingShardId(), expiracy);
					leaderEventsHandler.getPartitionManager().releaseAllOnPolicies();
				}
				return false;
			}
		}
		return true;
	}

	@Override
	public boolean inService() {
		return alive;
	}
}
