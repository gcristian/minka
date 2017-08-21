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
package io.tilt.minka.core.leader;

import java.io.Serializable;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.tilt.minka.api.Config;
import io.tilt.minka.api.MinkaClient;
import io.tilt.minka.broker.EventBroker;
import io.tilt.minka.broker.EventBroker.Channel;
import io.tilt.minka.core.task.Scheduler;
import io.tilt.minka.core.task.Scheduler.PriorityLock;
import io.tilt.minka.core.task.impl.ServiceImpl;
import io.tilt.minka.domain.EntityEvent;
import io.tilt.minka.domain.NetworkShardIdentifier;
import io.tilt.minka.domain.Shard;
import io.tilt.minka.domain.Shard.ShardState;
import io.tilt.minka.domain.ShardCommand;
import io.tilt.minka.domain.ShardEntity;

/**
 * Drives events generated thru the {@linkplain MinkaClient} by the client
 * when the {@linkplain Leader} is not activated in that Shard
 *  
 * @author Cristian Gonzalez
 * @since Dec 2, 2015
 *
 */
public class ClientEventsHandler extends ServiceImpl implements Consumer<Serializable> {

	private final Logger logger = LoggerFactory.getLogger(getClass());

	private final Config config;
	private final PartitionTable partitionTable;
	private final Scheduler scheduler;
	private final Bookkeeper bookkeeper;
	private final EventBroker eventBroker;
	private final NetworkShardIdentifier shardId;

	public ClientEventsHandler(Config config, PartitionTable partitionTable, Scheduler scheduler,
			EventBroker eventBroker, Bookkeeper bookkeeper, NetworkShardIdentifier shardId) {

		this.config = config;
		this.partitionTable = partitionTable;
		this.scheduler = scheduler;
		this.eventBroker = eventBroker;
		this.bookkeeper = bookkeeper;
		this.shardId = shardId;
	}

	public boolean clusterOperation(final ShardCommand op) {
		boolean done = false;
		Runnable lambda = null;
		if (op.getOperation() == io.tilt.minka.domain.ShardCommand.Command.CLUSTER_CLEAN_SHUTDOWN) {
			lambda = () -> cleanShutdown(op);
		}
		scheduler.run(
				scheduler.getFactory().build(op.getOperation().getAction(), PriorityLock.LOW_ON_PERMISSION, lambda));
		//throw new IllegalStateException("Cannot perform cluster operation because scheduler disallowed so");
		//}
		return done;
	}

	private void cleanShutdown(final ShardCommand op) {
		//Locks.stopCandidate(Names.getLeaderName(config.getServiceName()), false);		
		for (Shard slave : partitionTable.getStage().getShardsByState(ShardState.ONLINE)) {
			eventBroker.postEvent(slave.getBrokerChannel(), op);
		}
		boolean offline = false;
		while (!offline && !Thread.interrupted()) {
			LockSupport.parkUntil(5000l);
			offline = partitionTable.getStage().getShardsByState(ShardState.ONLINE).size() == 0;
		}
		// only then close subscriptions
		stop();
	}

	private void listenUserEvents() {
		eventBroker.subscribe(eventBroker.buildToTarget(config, Channel.FROM_CLIENT, shardId), ShardEntity.class,
				this, 0);
		eventBroker.subscribe(eventBroker.buildToTarget(config, Channel.FROM_CLIENT, shardId), ShardCommand.class,
				this, 0);
	}

	@Override
	public void start() {
		logger.info("{}: Starting", getClass().getSimpleName());
		listenUserEvents();
	}

	@Override
	public void stop() {
		logger.info("{}: Stopping", getClass().getSimpleName());
		eventBroker.unsubscribe(eventBroker.build(config, Channel.FROM_CLIENT), ShardEntity.class, this);
	}

	@Override
	public void accept(Serializable event) {
		if (inService()) {
			if (event instanceof ShardEntity) {
				final ShardEntity entity = (ShardEntity) event;
				mediateOnEntity(entity);
			} else if (event instanceof ShardCommand) {
				clusterOperation((ShardCommand) event);
			}
		} else {
			logger.error("{}: User events came but this master is no longer in service: {}", getClass().getSimpleName(),
					event.getClass().getSimpleName());
		}
	}

	public void mediateOnEntity(final ShardEntity entity) {
		if (entity.is(EntityEvent.UPDATE) || entity.is(EntityEvent.TRANSFER)) {
			// TODO chekear las cuestiones de disponibilidad de esto
			if (entity.getType()==ShardEntity.Type.DUTY) {
				final Shard location = partitionTable.getStage().getDutyLocation(entity);
				if (location != null && location.getState().isAlive()) {
					final Serializable payloadType = entity.getUserPayload() != null
							? entity.getUserPayload().getClass().getSimpleName() : "[empty]";
					logger.info("{}: Routing event with Payload: {} on {} to Shard: {}", getClass().getSimpleName(),
							payloadType, entity, location);
				} else {
					logger.error("{}: Cannot route event to Duty:{} as Shard:{} is no longer functional",
							getClass().getSimpleName(), entity.toBrief(), location);
				}
				eventBroker.postEvent(location.getBrokerChannel(), entity);
			} else if (entity.getType()==ShardEntity.Type.PALLET) {
				for (Shard location: partitionTable.getStage().getPalletLocations(entity)) {
					if (location.getState().isAlive()) {
						final Serializable payloadType = entity.getUserPayload() != null
								? entity.getUserPayload().getClass().getSimpleName() : "[empty]";
						logger.info("{}: Routing event with Payload: {} on {} to Shard: {}", getClass().getSimpleName(),
								payloadType, entity, location);
						eventBroker.postEvent(location.getBrokerChannel(), entity);
					}
				}
			}
		} else {
			bookkeeper.enterCRUD(entity);
		}
	}

}
