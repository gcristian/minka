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

import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Queue;
import java.util.Set;
import java.util.function.Consumer;

import org.apache.commons.lang.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.tilt.minka.core.task.LeaderShardContainer;
import io.tilt.minka.domain.NetworkShardIdentifier;
import io.tilt.minka.domain.ShardIdentifier;
import io.tilt.minka.utils.CollectionUtils;

/**
 * @author Cristian Gonzalez
 * @since Feb 2, 2016
 *
 */
public class TransportlessLeaderShardContainer implements LeaderShardContainer {

	private final Logger logger = LoggerFactory.getLogger(getClass());

	private final ShardIdentifier myShardId;

	private NetworkShardIdentifier leaderShardId;
	private NetworkShardIdentifier lastLeaderShardId;
	private Instant lastLeaderChange;
	private Queue<NetworkShardIdentifier> previousLeaders;
	private Set<Consumer<NetworkShardIdentifier>> observers;

	public TransportlessLeaderShardContainer(final ShardIdentifier myShardId) {
        this.myShardId = myShardId;
		this.previousLeaders = new CollectionUtils.SynchronizedSlidingQueue<NetworkShardIdentifier>(10);
		this.observers = new HashSet<>();
	}

	public ShardIdentifier getMyShardId() {
		return this.myShardId;
	}

	public final void observeForChange(final Consumer<NetworkShardIdentifier> consumer) {
		if (logger.isInfoEnabled()) {
			logger.info("{}: ({}) Adding to observation group: {} (hash {})", getName(), myShardId, consumer, consumer
					.hashCode());
		}
		this.observers.add(consumer);

		// already elected then tell him 
		if (leaderShardId != null) {
			if (logger.isInfoEnabled()) {
				logger.info("{}: ({}) Leader election already happened !: calling {} for consumption (hash {})", 
						getName(), myShardId, consumer, consumer.hashCode());
			}
			consumer.accept(leaderShardId);
		}
	}

	@Override
	public void setNewLeader(final NetworkShardIdentifier newLeader) {
		Validate.notNull(newLeader, "Cannot set a Null leader !");
		this.lastLeaderChange = Instant.now();
		
		try {
			boolean firstLeader = lastLeaderShardId == null;
			if (!firstLeader && lastLeaderShardId.equals(newLeader)) {
				if (logger.isInfoEnabled()) {
					logger.info("{}: ({}) same Leader {} reelected, skipping observer notification",
							getName(), myShardId, this.leaderShardId.getId());
			    }
				previousLeaders.add(leaderShardId);
			} else {
				if (logger.isInfoEnabled()) {
					logger.info("{}: ({}) Updating new Leader elected: {}", getName(), myShardId, newLeader);
				}
				if (!firstLeader) {
					previousLeaders.add(leaderShardId);
				}
				leaderShardId = newLeader;
				for (Consumer<NetworkShardIdentifier> o : this.observers) {
				    if (logger.isInfoEnabled()) {
				        logger.info("{}: ({}) Notifying observer: {}", getName(), myShardId,
								o.getClass().getSimpleName());
				    }
					o.accept(this.leaderShardId);
				}
				lastLeaderShardId = newLeader;
			}
		} catch (Exception e) {
			logger.error("{}: ({}) LeaderShardContainer: unexpected error", getName(), myShardId, e);
		}
	}

	public final NetworkShardIdentifier getPreviousLeaderShardId() {
		return previousLeaders.peek();
	}

	public final NetworkShardIdentifier getLeaderShardId() {
		return this.leaderShardId;
	}

	@Override
	public final List<NetworkShardIdentifier> getAllPreviousLeaders() {
		return new ArrayList<>(previousLeaders);
	}
	
	@Override
	public Instant getLastLeaderChange() {
		return lastLeaderChange;
	}

	@Override
	public boolean imLeader() {
		return this.myShardId.equals(leaderShardId);
	}

	@Override
	public void start() {
		// TODO Auto-generated method stub
	}

	@Override
	public void stop() {
		// TODO Auto-generated method stub
	}

}
