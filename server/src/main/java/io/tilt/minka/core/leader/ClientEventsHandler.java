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
import java.util.Date;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.tilt.minka.api.Client;
import io.tilt.minka.api.Config;
import io.tilt.minka.api.Reply;
import io.tilt.minka.api.ReplyResult;
import io.tilt.minka.broker.EventBroker;
import io.tilt.minka.broker.EventBroker.Channel;
import io.tilt.minka.core.task.Scheduler;
import io.tilt.minka.core.task.Scheduler.PriorityLock;
import io.tilt.minka.core.task.Service;
import io.tilt.minka.domain.EntityEvent;
import io.tilt.minka.domain.NetworkShardIdentifier;
import io.tilt.minka.domain.Shard;
import io.tilt.minka.domain.Shard.ShardState;
import io.tilt.minka.domain.ShardCommand;
import io.tilt.minka.domain.ShardEntity;

/**
 * Drives events generated thru the {@linkplain Client} by the client
 * when the {@linkplain Leader} is not activated in that Shard
 *  
 * @author Cristian Gonzalez
 * @since Dec 2, 2015
 *
 */
public class ClientEventsHandler implements Service, Consumer<Serializable> {

	private final Logger logger = LoggerFactory.getLogger(getClass());

	private final Config config;
	private final PartitionScheme partitionScheme;
	private final Scheduler scheduler;
	private final SchemeSentry sentry;
	private final EventBroker eventBroker;
	private final NetworkShardIdentifier shardId;

	private Date start;

	public ClientEventsHandler(
			final Config config, 
			final PartitionScheme partitionScheme, 
			final Scheduler scheduler,
			final SchemeSentry bookkeeper,
			final EventBroker eventBroker, 
			final NetworkShardIdentifier shardId) {

		this.config = config;
		this.partitionScheme = partitionScheme;
		this.scheduler = scheduler;
		this.sentry = bookkeeper;
		this.eventBroker = eventBroker;
		this.shardId = shardId;
	}

	public boolean clusterOperation(final ShardCommand op) {
		boolean done = false;
		Runnable lambda = null;
		if (op.getOperation() == io.tilt.minka.domain.ShardCommand.Command.CLUSTER_CLEAN_SHUTDOWN) {
			lambda = () -> cleanShutdown(op);
		}
		scheduler.run(scheduler.getFactory().build(
						op.getOperation().getAction(), 
						PriorityLock.LOW_ON_PERMISSION, 
						lambda));
		return done;
	}

	private void cleanShutdown(final ShardCommand op) {
		//Locks.stopCandidate(Names.getLeaderName(config.getServiceName()), false);		
		partitionScheme.getScheme().onShards(ShardState.ONLINE.filter(), shard-> {
			eventBroker.send(shard.getBrokerChannel(), op);
		});
		boolean offline = false;
		while (!offline && !Thread.interrupted()) {
			LockSupport.parkUntil(5000l);
			offline = partitionScheme.getScheme().shardsSize(ShardState.ONLINE.filter()) == 0;
		}
		// only then close subscriptions
		stop();
	}

	private void listenUserEvents() {
		eventBroker.subscribe(
				eventBroker.buildToTarget(
						config, 
						Channel.FROM_CLIENT, 
						shardId), 
				ShardEntity.class,
				this, 0);
		eventBroker.subscribe(
				eventBroker.buildToTarget(
						config, 
						Channel.FROM_CLIENT, 
						shardId), 
				ShardCommand.class,
				this, 0);
	}

	@Override
	public void start() {
		this.start = new Date();
		logger.info("{}: Starting", getClass().getSimpleName());
		listenUserEvents();
	}

	@Override
	public void stop() {
		logger.info("{}: Stopping", getClass().getSimpleName());
		eventBroker.unsubscribe(eventBroker.build(config, Channel.FROM_CLIENT), ShardEntity.class, this);
	}

	@Override
	public boolean inService() {
		return this.start!=null;
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

	public Reply mediateOnEntity(final ShardEntity entity) {
		if (entity.is(EntityEvent.UPDATE) || entity.is(EntityEvent.TRANSFER)) {
			boolean sent[] = {false};
			if (entity.getType()==ShardEntity.Type.DUTY) {
				final Shard location = partitionScheme.getScheme().getDutyLocation(entity);
				if (location != null && location.getState().isAlive()) {
					final Serializable payloadType = entity.getUserPayload() != null
							? entity.getUserPayload().getClass().getSimpleName() : "[empty]";
					logger.info("{}: Routing event with Payload: {} on {} to Shard: {}", getClass().getSimpleName(),
							payloadType, entity, location);
				} else {
					logger.error("{}: Cannot route event to Duty:{} as Shard:{} is no longer functional",
							getClass().getSimpleName(), entity.toBrief(), location);
				}
				sent[0] = eventBroker.send(location.getBrokerChannel(), entity);
			} else if (entity.getType()==ShardEntity.Type.PALLET) {
				partitionScheme.getScheme().filterPalletLocations(entity, shard-> {
					if (shard.getState().isAlive()) {
						final Serializable payloadType = entity.getUserPayload() != null
								? entity.getUserPayload().getClass().getSimpleName() : "[empty]";
						logger.info("{}: Routing event with Payload: {} on {} to Shard: {}", getClass().getSimpleName(),
								payloadType, entity, shard);
						sent[0] = eventBroker.send(shard.getBrokerChannel(), entity);
					}
				});
			}
			return new Reply(sent[0] ? ReplyResult.SUCCESS_SENT: ReplyResult.FAILURE_NOT_SENT, 
					entity.getEntity(), null, null, null);
		} else {
			return sentry.enterCRUD(entity);
		}
	}

}
