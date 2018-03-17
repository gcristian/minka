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

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.tilt.minka.api.Config;
import io.tilt.minka.broker.EventBroker;
import io.tilt.minka.core.task.Scheduler;
import io.tilt.minka.core.task.Scheduler.Agent;
import io.tilt.minka.core.task.Scheduler.Frequency;
import io.tilt.minka.core.task.Scheduler.PriorityLock;
import io.tilt.minka.core.task.Semaphore.Action;
import io.tilt.minka.core.task.impl.ServiceImpl;
import io.tilt.minka.domain.Clearance;
import io.tilt.minka.domain.Heartbeat;
import io.tilt.minka.domain.Shard.ShardState;

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
public class Follower extends ServiceImpl {

	private final Logger logger = LoggerFactory.getLogger(getClass());

	private boolean alive;

	private final DateTime creation;

	private final LeaderEventsHandler leaderConsumer;
	private final Config config;
	private final EventBroker eventBroker;

	private final Scheduler scheduler;
	private final Heartpump heartpump;
	private final HeartbeatFactory heartbeatFactory;
	private final Agent follow;


	public Follower(
			final Config config, 
			final Heartpump heartpump, 
			final LeaderEventsHandler leaderConsumer,
			final EventBroker eventBroker, 
			final Scheduler scheduler, 
			final HeartbeatFactory heartbeatFactory) {
		super();

		this.alive = true;
		this.heartpump = heartpump;
		this.heartbeatFactory = heartbeatFactory;
		this.leaderConsumer = leaderConsumer;
		this.config = config;
		this.eventBroker = eventBroker;
		this.creation = new DateTime(DateTimeZone.UTC);
		this.scheduler = scheduler;

		this.follow = scheduler.getAgentFactory()
				.create(Action.HEARTBEAT_REPORT, 
						PriorityLock.MEDIUM_BLOCKING, 
						Frequency.PERIODIC,
						() -> follow())
				.delayed(config.getFollower().getHeartbeatStartDelayMs())
				.every(config.getFollower().getHeartbeatDelayMs())
				.build();
	}

	@Override
	public void start() {
		logger.info("{}: ({}) Starting services", getClass().getSimpleName(), config.getLoggingShardId());
		this.heartpump.init();
		this.leaderConsumer.init();
		alive = true;
		// after partition manager initialized: set emergency shutdown
		eventBroker.setBrokerShutdownCallback(() -> stop());
		scheduler.schedule(follow);
	}

	@Override
	public void stop() {
		logger.info("{}: ({}) Stopping services, on duty since: {}", getClass().getSimpleName(),
				config.getLoggingShardId(), creation);
		if (inService()) {
			final Heartbeat bye = heartbeatFactory.create();
			bye.setStateChange(ShardState.QUITTED);
			heartpump.emit(bye);
			logger.info("{}: ({}) Stopping timer", getClass().getSimpleName(), config.getLoggingShardId());
			scheduler.stop(follow);
			alive = false;
			this.heartpump.stop();
			this.leaderConsumer.stop();
		} else {
			logger.info("{}: ({}) Follower was not longer in service", getClass().getSimpleName(),
					config.getLoggingShardId());
		}
	}
	
	private boolean firstBeat = true;
	
	private void follow() {
		boolean valid = checkClearanceOrFall();		
		boolean healthy = valid && checkBeatsOrFall(); // avoid release twice
		if (valid && (healthy || firstBeat)) {
			heartpump.emit(heartbeatFactory.create());
		}
		firstBeat = false;
	}

	/** @return whether or not the clearance is valid */
	private boolean checkClearanceOrFall() {
		boolean lost = false;
		final Clearance clear = leaderConsumer.getLastClearance();
		long delta = 0;
		//int maxAbsence = config.getFollower().getClearanceMaxAbsenceMs();
		final long hbdelay = config.getFollower().getHeartbeatDelayMs();
		final int maxAbsenceMs = (int)hbdelay * config.getProctor().getMaxAbsentHeartbeatsBeforeShardGone();
		final int minToJoinMs = (int)hbdelay * config.getProctor().getMaxShardJoiningStateMs();
		
		final DateTime now = new DateTime(DateTimeZone.UTC);
		// it's OK if there's no clearance because just entering.. 
		if (clear != null) {
			
			final DateTime lastClearanceDate = clear.getCreation();
			lost = lastClearanceDate.plusMillis(maxAbsenceMs).isBefore(now);
			delta = now.getMillis() - lastClearanceDate.getMillis();
			if (lost) {
				logger.error("{}: ({}) Executing Clearance policy, last: {} too old (Max: {}, Past: {} msecs)",
						getClass().getSimpleName(), config.getLoggingShardId(),
						clear != null ? clear.getCreation() : "null", maxAbsenceMs, delta);
				leaderConsumer.getPartitionManager().releaseAllOnPolicies();
			} else {
				logger.debug("{}: ({}) Clearence certified #{} from Leader: {}", getClass().getSimpleName(),
						config.getLoggingShardId(), clear.getSequenceId(), clear.getLeaderShardId());
			}
		}
		return !lost;
	}

	/** @return whether or not the pump's last beat is within a healthy time window */
	private boolean checkBeatsOrFall() {
		if (heartpump.getLastBeat() != null) {
			final DateTime expiracy = heartpump.getLastBeat().plus(config.getFollower().getMaxHeartbeatAbsenceForReleaseMs());
			if (expiracy.isBefore(new DateTime(DateTimeZone.UTC))) {
				logger.warn("{}: ({}) Executing Heartattack policy (last HB: {}): releasing delegate's held duties",
						getClass().getSimpleName(), config.getLoggingShardId(), expiracy);
				leaderConsumer.getPartitionManager().releaseAllOnPolicies();
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
