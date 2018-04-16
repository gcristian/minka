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
import io.tilt.minka.core.task.LeaderShardContainer;
import io.tilt.minka.domain.Heartbeat;
import io.tilt.minka.domain.NetworkShardIdentifier;
import io.tilt.minka.domain.ShardedPartition;

public class HeartpumpImpl implements Heartpump {

	private final Logger logger = LoggerFactory.getLogger(getClass());

	private final Config config;
	private final EventBroker eventBroker;
	private final ShardedPartition partition;
	private final LeaderShardContainer leaderShardContainer;
	private final String classname = getClass().getSimpleName();
	
	private DateTime lastHeartbeatTimestamp;

	public HeartpumpImpl(
			final Config config, 
			final EventBroker eventBroker, 
			final ShardedPartition partition,
			final LeaderShardContainer leaderShardContainer) {

		super();
		this.config = config;
		this.eventBroker = eventBroker;
		this.partition = partition;
		this.leaderShardContainer = leaderShardContainer;
	}

	public DateTime getLastBeat() {
		return this.lastHeartbeatTimestamp;
	}

	public boolean emit(final Heartbeat arg) {
		try {
			if (leaderShardContainer.getLeaderShardId() == null) {
				logger.warn("{}: Still without an acknowledged Leader shard !", classname,
						config.getLoggingShardId());
				//return false;
			}
			/* final Heartbeat thisHb = arg;
			 * 
			 * Heartbeat hbTraveling = thisHb; if (lastHeartbeat!=null &&
			 * thisHb.equalsInContent(lastHeartbeat)) { hbTraveling =
			 * Heartbeat.copy(thisHb); hbTraveling.cleanDuties(); } */

			if (eventBroker.send(
					eventBroker.buildToTarget(
							config, 
							Channel.HEARTBEATS,
							leaderShardContainer.getLeaderShardId()), 
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

	protected NetworkShardIdentifier getID() {
		return partition.getId();
	}

}