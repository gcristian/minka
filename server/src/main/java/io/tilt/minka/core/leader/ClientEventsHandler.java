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
import java.util.Date;
import java.util.List;
import java.util.function.Consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.tilt.minka.api.Client;
import io.tilt.minka.api.Config;
import io.tilt.minka.api.Reply;
import io.tilt.minka.broker.EventBroker;
import io.tilt.minka.broker.EventBroker.BrokerChannel;
import io.tilt.minka.broker.EventBroker.Channel;
import io.tilt.minka.core.leader.data.DirtyFacade;
import io.tilt.minka.core.leader.data.Scheme;
import io.tilt.minka.core.task.Scheduler;
import io.tilt.minka.core.task.Service;
import io.tilt.minka.domain.EntityEvent;
import io.tilt.minka.domain.ShardEntity;
import io.tilt.minka.shard.NetworkShardIdentifier;
import io.tilt.minka.shard.Shard;

/**
 * Drives events generated thru the {@linkplain Client} by the client
 * when the {@linkplain LeaderBootstrap} is not activated in that Shard
 *  
 * @author Cristian Gonzalez
 * @since Dec 2, 2015
 *
 */
public class ClientEventsHandler implements Service, Consumer<Serializable> {

	private final Logger logger = LoggerFactory.getLogger(getClass());

	private final Config config;
	private final Scheme scheme;
	private final Scheduler scheduler;
	private final DirtyFacade stageRepo;
	private final EventBroker eventBroker;
	private final NetworkShardIdentifier shardId;

	private Date start;

	ClientEventsHandler(
			final Config config, 
			final Scheme scheme, 
			final Scheduler scheduler,
			final DirtyFacade stageRepo,
			final EventBroker eventBroker, 
			final NetworkShardIdentifier shardId) {

		this.config = config;
		this.scheme = scheme;
		this.scheduler = scheduler;
		this.stageRepo = stageRepo;
		this.eventBroker = eventBroker;
		this.shardId = shardId;
	}

	private void listenUserEvents() {
		final BrokerChannel channel = eventBroker.buildToTarget(
				config, 
				Channel.CLITOLEAD, 
				shardId);
		eventBroker.subscribe(channel, ShardEntity.class,this, 0);
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
		eventBroker.unsubscribe(eventBroker.build(config, Channel.CLITOLEAD), ShardEntity.class, this);
		eventBroker.unsubscribe(eventBroker.build(config, Channel.CLITOLEAD), ArrayList.class, this);
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
			}
		} else {
			logger.error("{}: User events came but this master is no longer in service: {}", getClass().getSimpleName(),
					event.getClass().getSimpleName());
		}
	}
	
	public enum Consistency {
		LEADER,
		
		
	}

	public synchronized void mediateOnEntity(final Collection<ShardEntity> entities, final Consumer<Reply> callback) {
	    final ShardEntity first = entities.iterator().next();
    	if (first.is(EntityEvent.UPDATE) || first.is(EntityEvent.TRANSFER)) {
			updateOrTransfer(callback, first);
		} else {
		    if (first.getType()==ShardEntity.Type.DUTY) {
				if (first.is(EntityEvent.CREATE)) {
					stageRepo.saveAllDuties(entities, callback);
				} else {
				    stageRepo.removeAllDuties(entities, callback);
				}
			} else {
				if (first.is(EntityEvent.CREATE)) {
					stageRepo.saveAllPallets(entities, callback);
				} else {
					stageRepo.removeAllPallet(entities, callback);
				}				
			}
		}
	}

	private void updateOrTransfer(final Consumer<Reply> callback, final ShardEntity entity) {
		boolean sent[] = { false };
		if (entity.getType() == ShardEntity.Type.DUTY) {
			final Shard location = scheme.getCommitedState().findDutyLocation(entity);
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
			scheme.getCommitedState().filterPalletLocations(entity, shard -> {
				if (shard.getState().isAlive()) {
					final Serializable payloadType = entity.getUserPayload() != null ? 
							entity.getUserPayload().getClass().getSimpleName() : "[empty]";
					logger.info("{}: Routing event with Payload: {} on {} to Shard: {}", 
							getClass().getSimpleName(), payloadType, entity, shard);
					sent[0] = eventBroker.send(shard.getBrokerChannel(), entity);
				}
			});
		}
		callback.accept(sent[0] ? Reply.sentAsync(entity.getEntity()) : Reply.failedToSend(entity.getEntity()));
	}

}
