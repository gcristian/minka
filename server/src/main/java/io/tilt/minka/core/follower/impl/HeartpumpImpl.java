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
package io.tilt.minka.core.follower.impl;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.tilt.minka.api.Config;
import io.tilt.minka.broker.EventBroker;
import io.tilt.minka.broker.EventBroker.Channel;
import io.tilt.minka.core.follower.Heartpump;
import io.tilt.minka.core.task.LeaderAware;
import io.tilt.minka.domain.Heartbeat;
import io.tilt.minka.domain.ShardedPartition;
import io.tilt.minka.shard.NetworkShardIdentifier;

class HeartpumpImpl implements Heartpump {

	private final Logger logger = LoggerFactory.getLogger(getClass());

	private final Config config;
	private final EventBroker eventBroker;
	private final ShardedPartition partition;
	private final LeaderAware leaderAware;
	private final String classname = getClass().getSimpleName();
	
	private DateTime lastHeartbeatTimestamp;

	HeartpumpImpl(
			final Config config, 
			final EventBroker eventBroker, 
			final ShardedPartition partition,
			final LeaderAware leaderAware) {

		super();
		this.config = config;
		this.eventBroker = eventBroker;
		this.partition = partition;
		this.leaderAware = leaderAware;
	}

	@Override
	public DateTime getLastBeat() {
		return this.lastHeartbeatTimestamp;
	}

	@Override
	public boolean emit(final Heartbeat arg) {
		try {
			if (leaderAware.getLeaderShardId() == null) {
				logger.warn("{}: Still without an acknowledged Leader shard !", classname,
						config.getLoggingShardId());
				return false;
			}
			if (eventBroker.send(
					eventBroker.buildToTarget(
							config, 
							Channel.FOLLTOLEAD,
							leaderAware.getLeaderShardId()), 
					arg)) {
				this.lastHeartbeatTimestamp = new DateTime(DateTimeZone.UTC);
				return true;
			} else {
				logger.error("{}: ({}) Broker did not sent Heartbeat !", classname,
						config.getLoggingShardId());
			}
		} catch (Exception e) {
			logger.error("{}: ({}) Broker with exception", classname, config.getLoggingShardId(), e);
		}
		return false;
	}

	NetworkShardIdentifier getID() {
		return partition.getId();
	}

}