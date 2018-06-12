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

import static java.util.stream.Collectors.groupingBy;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map.Entry;
import java.util.function.Consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.tilt.minka.api.Config;
import io.tilt.minka.api.EntityPayload;
import io.tilt.minka.broker.EventBroker;
import io.tilt.minka.broker.EventBroker.BrokerChannel;
import io.tilt.minka.broker.EventBroker.Channel;
import io.tilt.minka.core.task.LeaderShardContainer;
import io.tilt.minka.core.task.Scheduler;
import io.tilt.minka.core.task.Scheduler.PriorityLock;
import io.tilt.minka.core.task.Scheduler.Synchronized;
import io.tilt.minka.core.task.Semaphore.Action;
import io.tilt.minka.core.task.Service;
import io.tilt.minka.domain.Clearance;
import io.tilt.minka.domain.DependencyPlaceholder;
import io.tilt.minka.domain.EntityEvent;
import io.tilt.minka.domain.EntityState;
import io.tilt.minka.domain.ShardCommand;
import io.tilt.minka.domain.ShardEntity;
import io.tilt.minka.domain.ShardedPartition;

/**
 * Leader event handler.
 * Subscribes to broker events for follower reactions
 * 
 * @author Cristian Gonzalez
 * @since Aug 6, 2016
 */
public class LeaderEventsHandler implements Service, Consumer<Serializable> {

	private final Logger logger = LoggerFactory.getLogger(getClass());

	private final Config config;
	private final DependencyPlaceholder dependencyPlaceholder;
	private final ShardedPartition partition;
	private final PartitionManager partitionManager;
	private final EventBroker eventBroker;
	private final Scheduler scheduler;
	private final LeaderShardContainer leaderContainer;
	
	private Clearance lastClearance;
	private BrokerChannel channel;
	
	public LeaderEventsHandler(
			final Config config, 
			final DependencyPlaceholder dependencyPlaceholder,
			final ShardedPartition partition, 
			final PartitionManager partitionManager, 
			final EventBroker eventBroker,
			final Scheduler scheduler, 
			final LeaderShardContainer leaderContainer) {

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
	@SuppressWarnings("unchecked")
	public void accept(final Serializable event) {
		if (event instanceof ShardCommand) {
			logger.debug("{}: ({}) Receiving: {}", getName(), config.getLoggingShardId(), event);
		} else if (event instanceof ShardEntity) {
			if (logger.isDebugEnabled()) {
				logger.debug("{}: ({}) Receiving # 1: {}", getName(), config.getLoggingShardId(), event);
			}
			scheduler.run(
			        scheduler.getFactory().build(
			                Action.INSTRUCT_DELEGATE, 
			                PriorityLock.MEDIUM_BLOCKING,
			                () -> handleDuty(Collections.singletonList((ShardEntity) event)))
			        );
		} else if (event instanceof ArrayList) {
			if (logger.isInfoEnabled()) {
				logger.info("{}: ({}) Receiving {}: {}", getName(), config.getLoggingShardId(), 
						((ArrayList<ShardEntity>) event).size(), event);
			}
			final List<ShardEntity> list = (ArrayList<ShardEntity>) event;
			if (list.isEmpty()) {
				throw new IllegalStateException("leader is sending an empty duty list");
			}
			final Synchronized handler = scheduler.getFactory().build(
					Action.INSTRUCT_DELEGATE,
					PriorityLock.MEDIUM_BLOCKING, 
					() -> handleDuty(list));
			scheduler.run(handler);
		} else if (event instanceof Clearance) {
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
		} else {
			logger.error("{}: ({}) Unknown event!: {} ", getName(), config.getLoggingShardId(), 
					event.getClass().getSimpleName());
		}
	}

	private void handleDuty(final List<ShardEntity> duties) {
		try {
			for (final Entry<EntityEvent, List<ShardEntity>> e : duties.stream()
					.collect(groupingBy(d -> d.getJournal().find(partition.getId()).getEvent())).entrySet()) {
				switch (e.getKey()) {
				case ATTACH:
					if (partitionManager.attach(e.getValue())) {
						received(e);
					}
					break;
				case DETACH:
					if (partitionManager.dettach(e.getValue())) {
						received(e);
					}
					break;
				case TRANSFER:
				case UPDATE:
					partitionManager.update(e.getValue());
					break;
				case REMOVE:
					partitionManager.finalized(e.getValue());
					break;
				default:
					logger.error("{}: ({}) Not allowed: {}", e.getKey(), config.getLoggingShardId());
				}
			}
		} catch (Exception e) {
			logger.error("{}: ({}) Unexpected while handling Duty:{}", getName(), config.getLoggingShardId(), duties, e);
		}
	}

	private void received(final Entry<EntityEvent, List<ShardEntity>> e) {
		e.getValue().forEach(d->d.getJournal().addEvent(
				e.getKey(), 
				EntityState.RECEIVED, 
				partition.getId(), 
				d.getJournal().getLast().getPlanId()));
	}
	
	public PartitionManager getPartitionManager() {
		return this.partitionManager;
	}

}
