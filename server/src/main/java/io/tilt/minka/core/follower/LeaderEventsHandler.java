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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

import io.tilt.minka.api.Config;
import io.tilt.minka.api.DependencyPlaceholder;
import io.tilt.minka.api.EntityPayload;
import io.tilt.minka.broker.EventBroker;
import io.tilt.minka.broker.EventBroker.Channel;
import io.tilt.minka.core.task.LeaderShardContainer;
import io.tilt.minka.core.task.Scheduler;
import io.tilt.minka.core.task.Scheduler.PriorityLock;
import io.tilt.minka.core.task.Scheduler.Synchronized;
import io.tilt.minka.core.task.Semaphore.Action;
import io.tilt.minka.core.task.Service;
import io.tilt.minka.core.task.impl.ServiceImpl;
import io.tilt.minka.domain.Clearance;
import io.tilt.minka.domain.DomainInfo;
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
public class LeaderEventsHandler extends ServiceImpl implements Service, Consumer<Serializable> {

	private final Logger logger = LoggerFactory.getLogger(getClass());

	private final DependencyPlaceholder dependencyPlaceholder;
	private final PartitionManager partitionManager;
	private final ShardedPartition partition;
	private final EventBroker eventBroker;
	private final Scheduler scheduler;
	private final Config config;
	private Clearance lastClearance;
	private final LeaderShardContainer leaderContainer;

	public LeaderEventsHandler(final Config config, final DependencyPlaceholder dependencyPlaceholder,
			final ShardedPartition partition, final PartitionManager partitionManager, final EventBroker eventBroker,
			final Scheduler scheduler, final LeaderShardContainer leaderContainer) {

		super();
		this.partition = partition;
		this.partitionManager = partitionManager;
		this.eventBroker = eventBroker;
		this.scheduler = scheduler;
		this.config = config;
		this.leaderContainer = leaderContainer;
		this.dependencyPlaceholder = dependencyPlaceholder;
	}

	@SuppressWarnings("unchecked")
	@Override
	public void start() {
		this.dependencyPlaceholder.getDelegate().activate();
		logger.info("{}: ({}) Preparing for leader events", getName(), config.getLoggingShardId());
		final long sinceNow = System.currentTimeMillis();
		eventBroker.subscribeEvents(eventBroker.buildToTarget(config, Channel.INSTRUCTIONS, partition.getId()),
				this, sinceNow, ShardEntity.class, Clearance.class, ArrayList.class, DomainInfo.class);
		/*eventBroker.subscribe(eventBroker.buildToTarget(config, Channel.INSTRUCTIONS_TO_FOLLOWER, partition.getId()),
				Clearance.class, this, sinceNow);
		eventBroker.subscribeEvents(
				eventBroker.buildToTarget(config, Channel.INSTRUCTIONS_TO_FOLLOWER, partition.getId()), ArrayList.class,
				this, sinceNow);
				*/

	}

	public Clearance getLastClearance() {
		return this.lastClearance;
	}

	@Override
	public void stop() {
		logger.info("{}: ({}) Stopping", getName(), config.getLoggingShardId());
		partitionManager.releaseAll();
		this.dependencyPlaceholder.getDelegate().deactivate();
		eventBroker.unsubscribe(eventBroker.buildToTarget(config, Channel.INSTRUCTIONS, partition.getId()),
				EntityPayload.class, this);
	}

	@Override
	@SuppressWarnings("unchecked")
	public void accept(final Serializable event) {
		if (event instanceof DomainInfo) {
			partitionManager.acknowledge((DomainInfo)event);
		} else if (event instanceof ShardCommand) {
			logger.info("{}: ({}) Receiving: {}", getName(), config.getLoggingShardId(), event);
			//partitionManager.handleClusterOperation((ShardCommand) event);
		} else if (event instanceof ShardEntity) {
			logger.info("{}: ({}) Receiving 1: {}", getName(), config.getLoggingShardId(), event);
			Synchronized handler = scheduler.getFactory().build(Action.INSTRUCT_DELEGATE, PriorityLock.MEDIUM_BLOCKING,
					() -> handleDuty((ShardEntity) event));
			scheduler.run(handler);
		} else if (event instanceof ArrayList) {
			logger.info("{}: ({}) Receiving {}: {}", getName(), config.getLoggingShardId(), ((ArrayList<ShardEntity>) event).size(), event);
			final List<ShardEntity> list = (ArrayList<ShardEntity>) event;
			final Synchronized handler = scheduler.getFactory().build(Action.INSTRUCT_DELEGATE,
					PriorityLock.MEDIUM_BLOCKING, () -> {
						if (list.stream().collect(Collectors.groupingBy(ShardEntity::getDutyEvent)).size() > 1) {
							list.forEach(d -> handleDuty(d));
						} else {
							handleDuty(list.toArray(new ShardEntity[list.size()]));
						}
					});
			scheduler.run(handler);
		} else if (event instanceof Clearance) {
			final Clearance clear = ((Clearance) event);
			if (clear.getLeaderShardId().equals(leaderContainer.getLeaderShardId())) {
				if (logger.isDebugEnabled()) {
					logger.debug("{}: ({}) Accepting clearance from: {} (id:{})", getName(), config.getLoggingShardId(),
							clear.getLeaderShardId(), clear.getSequenceId());
				}
				this.lastClearance = (Clearance) event;
			} else if (clear.getLeaderShardId().equals(leaderContainer.getPreviousLeaderShardId())) {
				logger.info("{}: ({}) Ignoring remaining clearance from previous leader: {} (current is: {})",
						getName(), config.getLoggingShardId(), clear.getLeaderShardId(),
						leaderContainer.getLeaderShardId());
			} else {
				logger.warn("{}: ({}) Ignoring clearance from unacknowledged leader: {} (my container says: {})",
						getName(), config.getLoggingShardId(), clear.getLeaderShardId(),
						leaderContainer.getLeaderShardId());
			}
		} else {
			logger.error("{}: ({}) Unknown event!: " + event.getClass().getSimpleName(), config.getLoggingShardId());
		}
	}

	private void handleDuty(final ShardEntity... duties) {
		try {
			switch (duties[0].getDutyEvent()) {
			case ATTACH:
				partitionManager.attach(Lists.newArrayList(duties));
				break;
			case DETACH:
				partitionManager.dettach(Lists.newArrayList(duties));
				break;
			case TRANSFER:
			case UPDATE:
				partitionManager.update(Lists.newArrayList(duties));
				break;
			case REMOVE:
				partitionManager.finalized(Lists.newArrayList(duties));
				break;
			default:
				logger.error("{}: ({}) Not allowed: {}", duties[0].getDutyEvent(), config.getLoggingShardId());
			}
			;
		} catch (Exception e) {
			logger.error("{}: ({}) Unexpected while handling Duty:{}", getName(), config.getLoggingShardId(), duties,
					e);
		}
	}

	public PartitionManager getPartitionManager() {
		return this.partitionManager;
	}

}
