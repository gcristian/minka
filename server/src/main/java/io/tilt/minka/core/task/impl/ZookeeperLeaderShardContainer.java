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
package io.tilt.minka.core.task.impl;

import java.util.function.Consumer;
import java.util.function.Supplier;

import org.apache.commons.lang.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.tilt.minka.api.NewConfig;
import io.tilt.minka.domain.NetworkShardID;
import io.tilt.minka.domain.ShardID;
import io.tilt.minka.spectator.MessageMetadata;
import io.tilt.minka.spectator.Spectator;
import io.tilt.minka.spectator.Wells;

/**
 * In charge of knowing how and who is: the leader of the current service ensemble  
 * @author Cristian Gonzalez
 * @since Feb 2, 2016
 *
 */
public class ZookeeperLeaderShardContainer extends TransportlessLeaderShardContainer {

		private final ZookeeperLeaderShardContainer instance;
		
		private final Logger logger = LoggerFactory.getLogger(getClass());
		private static final String LEADER_SHARD_RECORD_PATH = "leader-shard-record";

		private Wells wells;
		private final Supplier<Spectator> supplier;
		private final NewConfig config;
		private final Consumer<MessageMetadata> callbackConsumer;

		public ZookeeperLeaderShardContainer(final NewConfig config, final ShardID myShardId,
				final Supplier<Spectator> supplier) {
			super(myShardId);
			Validate.notNull(config);
			this.config = config;
			this.instance = this;
			this.supplier = supplier;

			// define it once to avoid varying hashCode as inline argument
			this.callbackConsumer = (meta) -> {
				try {
						super.setNewLeader((NetworkShardID) meta.getPayload());
				} catch (Exception e) {
						logger.error("{}: ({}) Failing to read leader's shard-container event's payload",
									getClass().getSimpleName(), getMyShardId(), config.getLoggingShardId(), e);
				}
			};
		}
		
		public ZookeeperLeaderShardContainer getInstance() {
			return instance;
		}

		@Override
		public void setNewLeader(NetworkShardID newLeader) {
			wells.updateWell(getServiceZKPath(), newLeader);
		}

		@Override
		public void start() {
			this.wells = new Wells(supplier.get());
			logger.info("{}: ({}) Listening Leader change", getClass().getSimpleName(), getMyShardId());
			wells.runOnUpdate(getServiceZKPath(), callbackConsumer);
		}

		@Override
		public void stop() {
			logger.info("{}: ({}) Closing well on leader change awareness", getClass().getSimpleName(), getMyShardId());
			wells.closeWell(getServiceZKPath());
			wells.close();
		}
		
		private String getServiceZKPath() {
			return  LEADER_SHARD_RECORD_PATH;
		}

}
