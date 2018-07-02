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

import static java.util.Objects.requireNonNull;

import java.io.InputStream;
import java.time.Instant;
import java.util.function.BiConsumer;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.tilt.minka.api.Config;
import io.tilt.minka.broker.EventBroker;
import io.tilt.minka.broker.EventBroker.Channel;
import io.tilt.minka.core.leader.data.ShardingState;
import io.tilt.minka.core.task.Scheduler;
import io.tilt.minka.core.task.Scheduler.PriorityLock;
import io.tilt.minka.core.task.Service;
import io.tilt.minka.domain.Heartbeat;
import io.tilt.minka.shard.NetworkShardIdentifier;
import io.tilt.minka.shard.Shard;

/**
 * Drives follower's events like {@linkplain Heartbeat} 
 * and maintains a {@linkplain ShardingState} by defining a member's service state
 *  
 * @author Cristian Gonzalez
 * @since Dec 2, 2015
 */
public class FollowerEventsHandler implements Service, BiConsumer<Heartbeat, InputStream> {

	private final Logger logger = LoggerFactory.getLogger(getClass());

	private final Config config;
	private final ShardingState shardingState;
	private final BiConsumer<Heartbeat, Shard> hbConsumer;
	private final EventBroker eventBroker;
	private final Scheduler scheduler;
	private final NetworkShardIdentifier shardId;

	private Instant lastBeat;

	public FollowerEventsHandler(
			final Config config, 
			final ShardingState shardingState, 
			final BiConsumer<Heartbeat, Shard> hbConsumer,
			final EventBroker eventBroker, 
			final Scheduler scheduler, 
			final NetworkShardIdentifier shardId) {

		this.config = requireNonNull(config);
		this.shardingState = requireNonNull(shardingState);
		this.hbConsumer = requireNonNull(hbConsumer);
		this.eventBroker = requireNonNull(eventBroker);
		this.scheduler = requireNonNull(scheduler);
		this.shardId = requireNonNull(shardId);		
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Override
	public void start() {
		logger.info("{}: Starting. Scheduling constant shepherding check", getName());
		final long readQueueSince = System.currentTimeMillis();
		eventBroker.subscribe(
				eventBroker.buildToTarget(config, Channel.FOLLTOLEAD, shardId),
				Heartbeat.class,
				(BiConsumer) this, 
				readQueueSince);
	}

	@Override
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public void stop() {
		logger.info("{}: Stopping", getClass().getSimpleName());
		this.eventBroker.unsubscribe(
				eventBroker.build(config, Channel.FOLLTOLEAD),
				Heartbeat.class,
				(BiConsumer) this);
	}
		
	@Override
	public void accept(final Heartbeat hb, final InputStream stream) {
		lastBeat = Instant.ofEpochMilli(hb.getCreation().getMillis());
		hb.setReception(new DateTime(DateTimeZone.UTC));
		if (logger.isDebugEnabled()) {
			logger.debug("{}: Receiving Heartbeat: {} ({}ms) from {}", getName(), hb.toString(),
					hb.getReceptionDelay(), hb.getShardId());
		}

		scheduler.run(scheduler.getFactory().build(
			Scheduler.Action.PARTITION_TABLE_UPDATE, 
			PriorityLock.MEDIUM_BLOCKING, 
			()-> hbConsumer.accept(hb, getOrRegisterShard(hb))));
	}

    private Shard getOrRegisterShard(final Heartbeat hb) {
		// when a shutdownlock acquired then keep receving HB to evaluate all Slaves are down!
		Shard shard = shardingState.getCommitedState().getShard(hb.getShardId());
		if (shard == null) {
			// new member
			shardingState.getCommitedState().addShard(shard = new Shard(
					eventBroker.buildToTarget(config, Channel.LEADTOFOLL, hb.getShardId()),
					hb.getShardId()));
		}
		return shard;
	}

    public Instant getLastBeat() {
		return lastBeat;
	}
}
