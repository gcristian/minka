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
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.function.Consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.tilt.minka.api.CommitBatch;
import io.tilt.minka.api.CommitBatch.CommitBatchResponse;
import io.tilt.minka.api.Config;
import io.tilt.minka.api.EntityPayload;
import io.tilt.minka.api.ParkingThreads;
import io.tilt.minka.broker.EventBroker;
import io.tilt.minka.broker.EventBroker.BrokerChannel;
import io.tilt.minka.broker.EventBroker.Channel;
import io.tilt.minka.core.leader.data.CommitRequest;
import io.tilt.minka.core.task.LeaderAware;
import io.tilt.minka.core.task.Scheduler;
import io.tilt.minka.core.task.Scheduler.PriorityLock;
import io.tilt.minka.core.task.Scheduler.Synchronized;
import io.tilt.minka.core.task.Semaphore.Action;
import io.tilt.minka.core.task.Service;
import io.tilt.minka.domain.CommitTree;
import io.tilt.minka.domain.CommitTree.Log;
import io.tilt.minka.domain.DependencyPlaceholder;
import io.tilt.minka.domain.EntityEvent;
import io.tilt.minka.domain.EntityState;
import io.tilt.minka.domain.ShardEntity;
import io.tilt.minka.domain.ShardedPartition;
import io.tilt.minka.shard.Clearance;

/**
 * LeaderBootstrap event handler.
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
	private final LeaderAware leaderContainer;
	private final ParkingThreads parkingThreads;
	
	private Clearance lastClearance;
	private BrokerChannel channel;
	
	private final static Class[] subscriptions = new Class[] {
			ShardEntity.class, 
			ArrayList.class, 
			CommitBatch.CommitBatchResponse.class,
			Clearance.class,
			EntityPayload.class
		};

	
	LeaderEventsHandler(
			final Config config, 
			final DependencyPlaceholder dependencyPlaceholder,
			final ShardedPartition partition, 
			final PartitionManager partitionManager, 
			final EventBroker eventBroker,
			final Scheduler scheduler, 
			final LeaderAware leaderContainer,
			final ParkingThreads parkingThreads) {

		super();
		this.config = config;
		this.dependencyPlaceholder = dependencyPlaceholder;
		this.partition = partition;
		this.partitionManager = partitionManager;
		this.eventBroker = eventBroker;
		this.scheduler = scheduler;
		this.leaderContainer = leaderContainer;
		this.parkingThreads = parkingThreads;
	}

	@SuppressWarnings("unchecked")
	@Override
	public void start() {
		this.dependencyPlaceholder.getDelegate().activate();
		logger.info("{}: ({}) Preparing for leader events", getName(), config.getLoggingShardId());
		final long sinceNow = System.currentTimeMillis();
		this.channel = eventBroker.buildToTarget(config, Channel.LEADTOFOLL, partition.getId());
		for (Class c: subscriptions) {
			eventBroker.subscribe(channel,this, sinceNow, c);	
		}
		
	}

	public Clearance getLastClearance() {
		return this.lastClearance;
	}

	@Override
	public void stop() {
		logger.info("{}: ({}) Stopping", getName(), config.getLoggingShardId());
		partitionManager.releaseAll();
		this.dependencyPlaceholder.getDelegate().deactivate();
		for (Class c: subscriptions) {
			eventBroker.unsubscribe(channel, c, this);	
		}
	}

	@Override
	public void accept(final Serializable event) {
		if (!(event instanceof Clearance)) {
			if (logger.isInfoEnabled()) {
				logger.info("{}: ({}) Receiving {}", getName(), config.getLoggingShardId(), 
						event.getClass().getSimpleName());
			}
		}
		if (event instanceof ArrayList) {
			final ArrayList al = (ArrayList)event;
			final Object any = al.get(0);
			if (any instanceof ShardEntity) {
				onCollection(al);				
			} else if (any instanceof CommitRequest) {
				for (final CommitRequest sr: (List<CommitRequest>)event) {
					parkingThreads.resolve(sr.getEntity().getDuty(), sr.getState());
				}
			}
		} else if (event instanceof CommitBatchResponse) {
			final CommitBatchResponse cr = (CommitBatchResponse)event;
			parkingThreads.resolve(cr.getId(), cr);
		} else if (event instanceof Clearance) {
			onClearance((Clearance) event);
		} else {
			logger.error("{}: ({}) Unknown event!: {} ", getName(), config.getLoggingShardId(), 
					event.getClass().getSimpleName());
		}
	}

	private void onClearance(final Clearance clear) {
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
	
	private void onCollection(final List<ShardEntity> list) {
		if (list.isEmpty()) {
			throw new IllegalStateException("leader is sending an empty duty list");
		}
		final Synchronized handler = scheduler.getFactory().build(
				Action.INSTRUCT_DELEGATE,
				PriorityLock.MEDIUM_BLOCKING, 
				() -> handleDuty(list));
		scheduler.run(handler);
	}
	
	private void handleDuty(final List<ShardEntity> duties) {
		try {
			for (final Entry<EntityEvent, List<ShardEntity>> e : groupByFoundEvents(duties).entrySet()) {
				switch (e.getKey()) {
				case ATTACH:
					if (partitionManager.attach(e.getValue())) {
						acknowledge(e);
					}
					break;
				case DETACH:
					if (partitionManager.dettach(e.getValue())) {
						acknowledge(e);
					}
					break;
				case TRANSFER:
				case UPDATE:
					partitionManager.update(e.getValue());
					break;
				case DROP:
					if (partitionManager.drop(e.getValue())) {
						acknowledge(e);
					}
					break;
				case STOCK:
					if (partitionManager.stock(e.getValue())) {
						acknowledge(e);
					}
					break;
				default:
					logger.error("{}: ({}) Not allowed: {}", e.getKey(), config.getLoggingShardId());
				}
			}
		} catch (Exception e) {
			logger.error("{}: ({}) Unexpected while handling Duty:{}", getName(), config.getLoggingShardId(), 
					ShardEntity.toStringIds(duties), e);
		}
	}

	/** no special event sorting */
	private Map<EntityEvent, List<ShardEntity>> groupByFoundEvents(final List<ShardEntity> duties) {
		final Map<EntityEvent, List<ShardEntity>> map = new HashMap<>(EntityEvent.values().length);
		for (ShardEntity d: duties) {
			for (Log log: d.getCommitTree().findAll(partition.getId())) {
				// we recognize only allocations and replicas
				if (EntityEvent.Type.ALLOC == log.getEvent().getType() 
						|| EntityEvent.Type.REPLICA == log.getEvent().getType()
						|| log.getEvent()==EntityEvent.UPDATE 
						|| log.getEvent()==EntityEvent.TRANSFER) {
					List<ShardEntity> list = map.get(log.getEvent());
					if (list == null) {
						map.put(log.getEvent(), list = new LinkedList<>());
					}
					list.add(d);
				}
			}
		}
		return map;
	}

	private void acknowledge(final Entry<EntityEvent, List<ShardEntity>> e) {
		for (ShardEntity duty: e.getValue()) {
			final Log last = duty.getCommitTree().findOne(CommitTree.PLAN_LAST, partition.getId(), e.getKey());
			if (last!=null) {
				final EntityState es = last.getLastState();
				if (es==EntityState.PENDING) {
					duty.getCommitTree().addEvent(
						e.getKey(), 
						EntityState.ACK, 
						partition.getId(), 
						last.getPlanId());
				} else if (es!=EntityState.PREPARED){
					logger.warn("{}: ({}) Repeating {} Ack for {} (now {})", getName(), 
							config.getLoggingShardId(), last.getEvent(), duty, es);
				}
			}
		}
	}
	
	PartitionManager getPartitionManager() {
		return this.partitionManager;
	}

}
