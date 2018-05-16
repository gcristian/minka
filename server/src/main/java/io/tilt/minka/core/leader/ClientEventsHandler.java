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

import static java.util.Collections.singletonList;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.tilt.minka.api.Client;
import io.tilt.minka.api.Config;
import io.tilt.minka.api.Reply;
import io.tilt.minka.api.ReplyResult;
import io.tilt.minka.broker.EventBroker;
import io.tilt.minka.broker.EventBroker.BrokerChannel;
import io.tilt.minka.broker.EventBroker.Channel;
import io.tilt.minka.core.leader.data.SchemeRepository;
import io.tilt.minka.core.leader.data.ShardingScheme;
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
	private final ShardingScheme shardingScheme;
	private final Scheduler scheduler;
	private final SchemeRepository repo;
	private final EventBroker eventBroker;
	private final NetworkShardIdentifier shardId;

	private Date start;

	public ClientEventsHandler(
			final Config config, 
			final ShardingScheme shardingScheme, 
			final Scheduler scheduler,
			final SchemeRepository repo,
			final EventBroker eventBroker, 
			final NetworkShardIdentifier shardId) {

		this.config = config;
		this.shardingScheme = shardingScheme;
		this.scheduler = scheduler;
		this.repo = repo;
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
		shardingScheme.getScheme().findShards(ShardState.ONLINE.filter(), shard-> {
			eventBroker.send(shard.getBrokerChannel(), op);
		});
		boolean offline = false;
		while (!offline && !Thread.interrupted()) {
			LockSupport.parkUntil(5000l);
			offline = shardingScheme.getScheme().shardsSize(ShardState.ONLINE.filter()) == 0;
		}
		// only then close subscriptions
		stop();
	}

	private void listenUserEvents() {
		final BrokerChannel channel = eventBroker.buildToTarget(
				config, 
				Channel.FROM_CLIENT, 
				shardId);
		eventBroker.subscribe(channel, ShardEntity.class,this, 0);
		eventBroker.subscribe(channel, ShardCommand.class,this, 0);
		eventBroker.subscribe(channel, ArrayList.class,this, 0);
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
		eventBroker.unsubscribe(eventBroker.build(config, Channel.FROM_CLIENT), ShardCommand.class, this);
		eventBroker.unsubscribe(eventBroker.build(config, Channel.FROM_CLIENT), ArrayList.class, this);
	}

	@Override
	public boolean inService() {
		return this.start!=null;
	}
	
	@Override
	public void accept(final Serializable event) {
		if (inService()) {
			if (event instanceof ShardEntity) {
				final ShardEntity entity = (ShardEntity) event;
				mediateOnEntity(singletonList(entity), (r)->{});
			} else if (event instanceof List) {
				mediateOnEntity((List)event, (r)->{});
			} else if (event instanceof ShardCommand) {
				clusterOperation((ShardCommand) event);
			}
		} else {
			logger.error("{}: User events came but this master is no longer in service: {}", getClass().getSimpleName(),
					event.getClass().getSimpleName());
		}
	}

	public void mediateOnEntity(final Collection<ShardEntity> entities, final Consumer<Reply> callback) {
	    final ShardEntity first = entities.iterator().next();
    	if (first.is(EntityEvent.UPDATE) || first.is(EntityEvent.TRANSFER)) {
			updateOrTransfer(callback, first);
		} else {
		    if (first.getType()==ShardEntity.Type.DUTY) {
				if (first.is(EntityEvent.CREATE)) {
					repo.saveAllDuty(entities, callback);
				} else {
				    repo.removeAllDuty(entities, callback);
				}
			} else {
				if (first.is(EntityEvent.CREATE)) {
					repo.saveAllPallet(entities, callback);
				} else {
					repo.removeAllPallet(entities, callback);
				}				
			}
		}
	}

	private void updateOrTransfer(final Consumer<Reply> callback, final ShardEntity entity) {
		boolean sent[] = { false };
		if (entity.getType() == ShardEntity.Type.DUTY) {
			final Shard location = shardingScheme.getScheme().findDutyLocation(entity);
			if (location != null && location.getState().isAlive()) {
				final Serializable payloadType = entity.getUserPayload() != null ? 
						entity.getUserPayload().getClass().getSimpleName() : "[empty]";
				logger.info("{}: Routing event with Payload: {} on {} to Shard: {}", 
						getClass().getSimpleName(), payloadType, entity, location);
			} else {
				logger.error("{}: Cannot route event to Duty:{} as Shard:{} is no longer functional", 
						getClass().getSimpleName(), entity.toBrief(), location);
			}
			sent[0] = eventBroker.send(location.getBrokerChannel(), entity);
		} else if (entity.getType() == ShardEntity.Type.PALLET) {
			shardingScheme.getScheme().filterPalletLocations(entity, shard -> {
				if (shard.getState().isAlive()) {
					final Serializable payloadType = entity.getUserPayload() != null ? 
							entity.getUserPayload().getClass().getSimpleName() : "[empty]";
					logger.info("{}: Routing event with Payload: {} on {} to Shard: {}", 
							getClass().getSimpleName(), payloadType, entity, shard);
					sent[0] = eventBroker.send(shard.getBrokerChannel(), entity);
				}
			});
		}
		callback.accept(new Reply(sent[0] ? ReplyResult.SUCCESS_SENT : ReplyResult.FAILURE_NOT_SENT,
				entity.getEntity(), null, null, null));
	}

}
