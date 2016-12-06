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

import java.util.HashSet;
import java.util.List;
import java.util.Queue;
import java.util.Set;
import java.util.function.Consumer;

import org.apache.commons.lang.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.tilt.minka.core.task.LeaderShardContainer;
import io.tilt.minka.domain.NetworkShardID;
import io.tilt.minka.domain.ShardID;
import io.tilt.minka.utils.SynchronizedSlidingQueue;

/**
 * @author Cristian Gonzalez
 * @since Feb 2, 2016
 *
 */
public class TransportlessLeaderShardContainer extends ServiceImpl implements LeaderShardContainer {

		private final Logger logger = LoggerFactory.getLogger(getClass());

		private final ShardID myShardId;
		private NetworkShardID leaderShardId;
		private NetworkShardID lastLeaderShardId;
		private Queue<NetworkShardID> previousLeaders;
		private Set<Consumer<NetworkShardID>> observers;

		public TransportlessLeaderShardContainer(final ShardID myShardId) {
			this.previousLeaders = new SynchronizedSlidingQueue<NetworkShardID>(10);
			this.observers = new HashSet<>();
			this.myShardId = myShardId;
		}

		public ShardID getMyShardId() {
			return this.myShardId;
		}

		public final void observeForChange(Consumer<NetworkShardID> consumer) {
			logger.info("{}: ({}) Adding to observation group: {} (hash {})", getClass().getSimpleName(), myShardId,
						consumer, consumer.hashCode());
			this.observers.add(consumer);

			// already elected then tell him 
			if (leaderShardId != null) {
				logger.info("{}: ({}) Leader election already happened !: calling {} for consumption (hash {})",
							getClass().getSimpleName(), myShardId, consumer, consumer.hashCode());
				consumer.accept(leaderShardId);
			}
		}

		@Override
		public void setNewLeader(final NetworkShardID newLeader) {
			Validate.notNull(newLeader, "Cannot set a Null leader !");
			try {
				boolean firstLeader = lastLeaderShardId == null;
				if (!firstLeader && lastLeaderShardId.equals(newLeader)) {
						logger.info("{}: ({}) same Leader {} reelected, skipping observer notification",
								getClass().getSimpleName(), myShardId, this.leaderShardId.getStringIdentity());
						previousLeaders.add(leaderShardId);
				} else {
						logger.info("{}: ({}) Updating new Leader elected: {}", getClass().getSimpleName(), myShardId,
								newLeader);
						if (!firstLeader) {
							previousLeaders.add(leaderShardId);
						}
						leaderShardId = newLeader;
						for (Consumer<NetworkShardID> o : this.observers) {
							logger.info("{}: ({}) Notifying observer: {}", getClass().getSimpleName(), myShardId,
										o.getClass().getSimpleName());
							o.accept(this.leaderShardId);
						}
						lastLeaderShardId = newLeader;
				}
			} catch (Exception e) {
				logger.error("{}: ({}) LeaderShardContainer: unexpected error", getClass().getSimpleName(), myShardId, e);
			}
		}

		public final NetworkShardID getPreviousLeaderShardId() {
			return previousLeaders.peek();
		}

		public final NetworkShardID getLeaderShardId() {
			return this.leaderShardId;
		}

		@Override
		public final List<NetworkShardID> getAllPreviousLeaders() {
			// TODO
			return null;
		}

		@Override
		public boolean imLeader() {
			return this.myShardId.equals(leaderShardId);
		}

}
