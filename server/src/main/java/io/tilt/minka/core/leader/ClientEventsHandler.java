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

import java.io.InputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.concurrent.locks.LockSupport;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.tilt.minka.api.Client;
import io.tilt.minka.api.Config;
import io.tilt.minka.api.Duty;
import io.tilt.minka.api.Reply;
import io.tilt.minka.api.ReplyValue;
import io.tilt.minka.broker.EventBroker;
import io.tilt.minka.broker.EventBroker.BrokerChannel;
import io.tilt.minka.broker.EventBroker.Channel;
import io.tilt.minka.core.leader.data.ShardingState;
import io.tilt.minka.core.leader.data.UncommitedRepository;
import io.tilt.minka.core.leader.data.EntityRepository;
import io.tilt.minka.core.task.Scheduler;
import io.tilt.minka.core.task.Service;
import io.tilt.minka.domain.EntityEvent;
import io.tilt.minka.domain.ShardEntity;
import io.tilt.minka.shard.NetworkShardIdentifier;
import io.tilt.minka.shard.Shard;

/**
 * Drives events generated thru the {@linkplain Client} by the client
 * when the {@linkplain Leader} is not activated in that Shard
 *  
 * @author Cristian Gonzalez
 * @since Dec 2, 2015
 *
 */
public class ClientEventsHandler implements Service, BiConsumer<Serializable, InputStream> {

	private final Logger logger = LoggerFactory.getLogger(getClass());

	private final Config config;
	private final ShardingState shardingState;
	private final Scheduler scheduler;
	private final UncommitedRepository stageRepo;
	private final EventBroker eventBroker;
	private final NetworkShardIdentifier shardId;
	private final EntityRepository entityRepo;

	private Date start;

	public ClientEventsHandler(
			final Config config, 
			final ShardingState shardingState, 
			final Scheduler scheduler,
			final UncommitedRepository stageRepo,
			final EventBroker eventBroker, 
			final NetworkShardIdentifier shardId,
			final EntityRepository entityRepo) {

		this.config = config;
		this.shardingState = shardingState;
		this.scheduler = scheduler;
		this.stageRepo = stageRepo;
		this.eventBroker = eventBroker;
		this.shardId = shardId;
		this.entityRepo = entityRepo;
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
	public void accept(final Serializable event, final InputStream stream) {
		if (inService()) {
			if (event instanceof ShardEntity) {
				final ShardEntity entity = (ShardEntity) event;
				
				final String sid = entity.getJournal().getLast().getTargetId();
				
				/*
				final BrokerChannel origin = eventBroker.buildToTarget(
						config, 
						Channel.LEADTOFOLL, 
						shardingState.getScheme().findShard(sid).getShardID());
				eventBroker.send(origin, event);
				*/
				
				// make available to all followers
				shardingState.getCommitedState().findShards(null, shard-> {
					eventBroker.send(
							eventBroker.buildToTarget(
									config, 
									Channel.LEADTOFOLL, 
									shard.getShardID()), 
							event);
				});
				
				
				
				//mediateOnEntity(singletonList(entity), (r)->{});
			} else if (event instanceof List) {
				mediateOnEntity((List)event, (r)->{});
			}
		} else {
			logger.error("{}: User events came but this master is no longer in service: {}", getClass().getSimpleName(),
					event.getClass().getSimpleName());
		}
	}

	
	public synchronized void mediateOnEntity(final Collection<Duty.LoadedDuty> entities, final Consumer<Reply> callback) {
	    final Duty.LoadedDuty first = entities.iterator().next();
    	if (first.getDuty().is(EntityEvent.UPDATE) || first.getDuty().is(EntityEvent.TRANSFER)) {
			updateOrTransfer(callback, first);
		} else {
		    if (first.getDuty().getType()==ShardEntity.Type.DUTY) {
		    	final List<ShardEntity> coll = entities.stream().map(e->e.getDuty()).collect(Collectors.toList());
				if (first.getDuty().is(EntityEvent.CREATE)) {
					entityRepo.save(entities);
					stageRepo.saveAllDuties(coll, callback);
				} else {
				    stageRepo.removeAllDuties(coll, callback);
				}
			} else {
				final List<ShardEntity> coll = entities.stream().map(e->e.getDuty()).collect(Collectors.toList());
				if (first.getDuty().is(EntityEvent.CREATE)) {
					stageRepo.saveAllPallets(coll, callback);
				} else {
					stageRepo.removeAllPallet(coll, callback);
				}				
			}
		}
	}

	private void updateOrTransfer(final Consumer<Reply> callback, final Duty.LoadedDuty entity) {
		boolean sent[] = { false };
		if (entity.getDuty().getType() == ShardEntity.Type.DUTY) {
			final Shard location = shardingState.getCommitedState().findDutyLocation(entity.getDuty());
			if (location != null && location.getState().isAlive()) {
				logger.info("{}: Routing event on {} to Shard: {}", 
						getClass().getSimpleName(), entity, location);
			} else {
				logger.error("{}: Cannot route event to Duty:{} as Shard:{} is no longer functional", 
						getClass().getSimpleName(), entity.getDuty().toBrief(), location);
			}
			sent[0] = eventBroker.send(location.getBrokerChannel(), entity.getDuty(), entity.getStream());
		} else if (entity.getDuty().getType() == ShardEntity.Type.PALLET) {
			shardingState.getCommitedState().filterPalletLocations(entity.getDuty(), shard -> {
				if (shard.getState().isAlive()) {
					logger.info("{}: Routing event on {} to Shard: {}", 
							getClass().getSimpleName(), entity, shard);
					sent[0] = eventBroker.send(shard.getBrokerChannel(), entity.getDuty(), entity.getStream());
				}
			});
		}
		callback.accept(new Reply(sent[0] ? ReplyValue.SUCCESS_SENT : ReplyValue.FAILURE_NOT_SENT,
				entity.getDuty().getEntity(), null, null, null));
	}

}
