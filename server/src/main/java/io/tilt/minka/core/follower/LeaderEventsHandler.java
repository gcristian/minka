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

import java.io.InputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.function.BiConsumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.tilt.minka.api.Config;
import io.tilt.minka.api.EntityPayload;
import io.tilt.minka.broker.EventBroker;
import io.tilt.minka.broker.EventBroker.BrokerChannel;
import io.tilt.minka.broker.EventBroker.Channel;
import io.tilt.minka.core.task.LeaderAware;
import io.tilt.minka.core.task.Scheduler;
import io.tilt.minka.core.task.Scheduler.PriorityLock;
import io.tilt.minka.core.task.Scheduler.Synchronized;
import io.tilt.minka.core.task.Semaphore.Action;
import io.tilt.minka.core.task.Service;
import io.tilt.minka.domain.DependencyPlaceholder;
import io.tilt.minka.domain.EntityState;
import io.tilt.minka.domain.ShardEntity;
import io.tilt.minka.domain.ShardedPartition;
import io.tilt.minka.shard.Clearance;

/**
 * Leader event handler.
 * Subscribes to broker events for follower reactions
 * 
 * @author Cristian Gonzalez
 * @since Aug 6, 2016
 */
public class LeaderEventsHandler implements Service, BiConsumer<Serializable, InputStream> {

	private final Logger logger = LoggerFactory.getLogger(getClass());

	private final Config config;
	private final DependencyPlaceholder dependencyPlaceholder;
	private final ShardedPartition partition;
	private final PartitionManager partitionManager;
	private final EventBroker eventBroker;
	private final Scheduler scheduler;
	private final LeaderAware leaderContainer;
	
	private Clearance lastClearance;
	private BrokerChannel channel;
	
	public LeaderEventsHandler(
			final Config config, 
			final DependencyPlaceholder dependencyPlaceholder,
			final ShardedPartition partition, 
			final PartitionManager partitionManager, 
			final EventBroker eventBroker,
			final Scheduler scheduler, 
			final LeaderAware leaderContainer) {

		super();
		this.config = config;
		this.dependencyPlaceholder = dependencyPlaceholder;
		this.partition = partition;
		this.partitionManager = partitionManager;
		this.eventBroker = eventBroker;
		this.scheduler = scheduler;
		this.leaderContainer = leaderContainer;
	}

	@SuppressWarnings("unchecked")
	@Override
	public void start() {
		this.dependencyPlaceholder.getDelegate().activate();
		logger.info("{}: ({}) Preparing for leader events", getName(), config.getLoggingShardId());
		final long sinceNow = System.currentTimeMillis();
		this.channel = eventBroker.buildToTarget(config, Channel.LEADTOFOLL, partition.getId());
		eventBroker.subscribe(channel,this, sinceNow, ShardEntity.class, Clearance.class, ArrayList.class);
	}

	public Clearance getLastClearance() {
		return this.lastClearance;
	}

	@Override
	public void stop() {
		logger.info("{}: ({}) Stopping", getName(), config.getLoggingShardId());
		partitionManager.releaseAll();
		this.dependencyPlaceholder.getDelegate().deactivate();
		eventBroker.unsubscribe(channel, EntityPayload.class, this);
	}

	@Override
	public void accept(final Serializable event, final InputStream stream) {
		if (event instanceof ShardEntity) {
			((ShardEntity)event).putPayload(stream);
			onSingle(event);
		} else if (event instanceof Clearance) {
			onClearance(event);
		} else {
			logger.error("{}: ({}) Unknown event!: {} ", getName(), config.getLoggingShardId(), 
					event.getClass().getSimpleName());
		}
	}

	private void onClearance(final Serializable event) {
		final Clearance clear = ((Clearance) event);
		if (clear.getLeaderShardId().equals(leaderContainer.getLeaderShardId())) {
			if (logger.isDebugEnabled()) {
				logger.debug("{}: ({}) Accepting clearance from: {} (id:{})", getName(), config.getLoggingShardId(),
						clear.getLeaderShardId(), clear.getSequenceId());
			}
			this.lastClearance = clear;
			partitionManager.acknowledge(clear.getInfo());
		} else if (clear.getLeaderShardId().equals(leaderContainer.getPreviousLeaderShardId())) {
			logger.warn("{}: ({}) Ignoring remaining clearance from previous leader: {} (current is: {})",
					getName(), config.getLoggingShardId(), clear.getLeaderShardId(),
					leaderContainer.getLeaderShardId());
		} else {
			logger.warn("{}: ({}) Ignoring clearance from unacknowledged leader: {} (my container says: {})",
					getName(), config.getLoggingShardId(), clear.getLeaderShardId(),
					leaderContainer.getLeaderShardId());
		}
	}
	
	@SuppressWarnings("unchecked")
	private void onSingle(final Serializable event) {
		if (logger.isInfoEnabled()) {
			logger.info("{}: ({}) Receiving {}: {}", getName(), config.getLoggingShardId(), 
					(ShardEntity) event, event);
		}
		final ShardEntity single = (ShardEntity) event;
		final Synchronized handler = scheduler.getFactory().build(
				Action.INSTRUCT_DELEGATE,
				PriorityLock.MEDIUM_BLOCKING, 
				() -> handleDuty(single));
		scheduler.run(handler);
	}

	private void handleDuty(final ShardEntity duty) {
		try {
			switch (duty.getLastEvent()) {
			case CREATE:
				break;
			case REMOVE:
				//partitionManager.finalized(e.getValue());
				break;
			case ATTACH:
				if (partitionManager.attach(Collections.singletonList(duty))) {
					received(duty);
				}
				break;
			case DETACH:
				if (partitionManager.dettach(Collections.singletonList(duty))) {
					received(duty);
				}
				break;
			case TRANSFER:
			case UPDATE:
				partitionManager.update(Collections.singletonList(duty));
				break;
			default:
				logger.error("{}: ({}) Not allowed: {}", duty, config.getLoggingShardId());
			}
		} catch (Exception e) {
			logger.error("{}: ({}) Unexpected while handling Duty:{}", getName(), config.getLoggingShardId(), duty, e);
		}
	}

	private void received(final ShardEntity e) {
		e.getJournal().addEvent(
				e.getLastEvent(), 
				EntityState.RECEIVED, 
				partition.getId(), 
				e.getJournal().getLast().getPlanId());
	}
	
	public PartitionManager getPartitionManager() {
		return this.partitionManager;
	}

}
