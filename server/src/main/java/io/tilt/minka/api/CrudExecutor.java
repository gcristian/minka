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
package io.tilt.minka.api;

import static java.util.Collections.singleton;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;

import org.apache.commons.lang.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

import io.tilt.minka.api.CommitBatch.CommitBatchRequest;
import io.tilt.minka.api.CommitBatch.CommitBatchResponse;
import io.tilt.minka.api.config.SchedulerSettings;
import io.tilt.minka.broker.EventBroker;
import io.tilt.minka.broker.EventBroker.BrokerChannel;
import io.tilt.minka.broker.EventBroker.Channel;
import io.tilt.minka.core.leader.ClientEventsHandler;
import io.tilt.minka.core.leader.LeaderBootstrap;
import io.tilt.minka.core.leader.data.CommitState;
import io.tilt.minka.core.task.LeaderAware;
import io.tilt.minka.domain.CommitTree;
import io.tilt.minka.domain.EntityEvent;
import io.tilt.minka.domain.EntityState;
import io.tilt.minka.domain.ShardEntity;
import io.tilt.minka.shard.ShardIdentifier;

/**
 * Handles CRUD ops from Client. 
 * With a fireAndForget mechanism to send Leader write requests, 
 * and an async-promise response mechanism, to request for Leader reply and duty state data.
 * 
 * @since Sept 1, 2018
 */
class CrudExecutor {

	private static final Logger logger = LoggerFactory.getLogger(CrudExecutor.class);

	private final Config config;
	private final LeaderBootstrap leaderBootstrap;
	private final EventBroker eventBroker;
	private final ClientEventsHandler clientHandler;
	private final ShardIdentifier shardId;
	private final LeaderAware leaderAware;
	private final ParkingThreads parkingThreads;
	
	private final ExecutorService executor = Executors.newCachedThreadPool(
			new ThreadFactoryBuilder()
				.setNameFormat(SchedulerSettings.PNAME + "CL")
				.build());

	protected CrudExecutor(
			final Config config, 
			final LeaderBootstrap leaderBootstrap, 
			final EventBroker eventBroker,
			final ClientEventsHandler mediator, 
			final ShardIdentifier shardId, 
			final LeaderAware leaderAware,
			final ParkingThreads parkingThreads) {
		this.config = config;
		this.leaderBootstrap = leaderBootstrap;
		this.eventBroker = eventBroker;
		this.clientHandler = mediator;
		this.shardId = shardId;
		this.leaderAware = leaderAware;
		this.parkingThreads = parkingThreads;
	}

	/** flags the request so the leader knows it must not respond a reply or state back */
	Collection<Reply> execute(
			final Collection<? extends Entity> raws, 
			final EntityEvent event, 
			final EntityPayload userPayload) {
		
		Validate.notNull(raws, "an entity is required");
		final long start = System.currentTimeMillis();
		Set<Reply> replies;
		try {
			final List<ShardEntity> entities = toEntities(raws, event, userPayload);
			if (leaderBootstrap.inService()) {
				final Set<Reply> replies_ = replies = new HashSet<>(raws.size());
				clientHandler.mediateOnEntity(entities, r-> {
					replies_.add(r.withTiming(Reply.Timing.CLIENT_CREATED_TS, start));	
					replies_.add(r.withTiming(Reply.Timing.LEADER_REPLY_TS, System.currentTimeMillis()));
				}, false);
				timeReceived(replies_);
			} else {
				final CommitBatchRequest request = new CommitBatchRequest(entities, false);
				boolean success = false;
				if (! (success = sendWithRetries(request))) {
					logger.warn("{}: Couldnt send entities: {}", getClass().getSimpleName());
				}
				replies = singleton(success ? Reply.sentAsync(null) : Reply.failedToSend(null));
			}
		} catch (Exception e) {
			logger.error("Cannot mediate to leaderBootstrap", e);
			replies = singleton(Reply.error(e));
		}
		return replies;
	}

	Future<Collection<Reply>> execute(
			final long futureMaxWaitMs,
			final Collection<? extends Entity> raws, 
			final EntityEvent event, 
			final EntityPayload userPayload) {
		
		Validate.notNull(raws, "an entity is required");
		final Callable<Collection<Reply>> call = () -> resolveReplies(raws, event, userPayload, futureMaxWaitMs);
		return executor.submit(call);
	}

	Future<Reply> executeSingle(
			final long futureMaxWaitMs,
			final Collection<? extends Entity> raws, 
			final EntityEvent event, 
			final EntityPayload userPayload) {
		
		Validate.notNull(raws, "an entity is required");
		final Callable<Reply> call = () -> resolveReplies(raws, event, userPayload, futureMaxWaitMs).iterator().next();
		return executor.submit(call);
	}

	private Collection<Reply> resolveReplies(
			final Collection<? extends Entity> raws, 
			final EntityEvent event, 
			final EntityPayload userPayload,
			final long futureMaxWaitMs) {
		final long start = System.currentTimeMillis();
		final Set<Reply> replies = new HashSet<>(raws.size());
		try {
			final List<ShardEntity> entities = toEntities(raws, event, userPayload);
			if (leaderBootstrap.inService()) {
				clientHandler.mediateOnEntity(entities, r-> {
					replies.add(r.withTiming(Reply.Timing.CLIENT_CREATED_TS, start));	
					replies.add(r.withTiming(Reply.Timing.LEADER_REPLY_TS, System.currentTimeMillis()));
				}, true);
				timeReceived(replies);
			} else {
				replies.addAll(forwardReplyResolution(entities, futureMaxWaitMs));
			}
			if (entities.iterator().next().getType()==ShardEntity.Type.DUTY) {
				linkFuturePromises(replies, futureMaxWaitMs);
			}
		} catch (Exception e) {
			logger.error("Cannot mediate to leaderBootstrap", e);
			replies.add(Reply.error(e));
		}
		return replies;
	}

	/** identify leader: send and wait for response  */
	private Collection<Reply> forwardReplyResolution(final List<ShardEntity> entities, 
			final long futureMaxWaitMs) {
		final long start = System.currentTimeMillis();
		final CommitBatchRequest request = new CommitBatchRequest(entities, true);
		if (sendWithRetries(request)) {
			final long sent = System.currentTimeMillis();
			final CommitBatchResponse response = parkingThreads.wait(request.getId(), futureMaxWaitMs);
			if (response!=null) {
				for (Reply r: response.getReplies()) {
					r.withTiming(Reply.Timing.CLIENT_CREATED_TS, start);
					r.withTiming(Reply.Timing.CLIENT_SENT_TS, sent);
					r.withTiming(Reply.Timing.CLIENT_RECEIVED_REPLY_TS, System.currentTimeMillis());
				}
				return response.getReplies();
			} else {
				logger.warn("{}: {}: (RequestId: {})", getClass().getSimpleName(), 
						ReplyValue.REPLY_TIMED_OUT, request.getId());
				return Collections.singleton(Reply.leaderReplyTimedOut());
			}
		} else {
			logger.warn("{}: Couldnt send entities: {}", getClass().getSimpleName());
			return Collections.singleton(Reply.failedToSend(null));
		}
	}

	private void timeReceived(final Set<Reply> replies) {
		long now = System.currentTimeMillis();
		replies.forEach(r->r.withTiming(Reply.Timing.CLIENT_RECEIVED_REPLY_TS, now));
	}

	/** links the reply with a Future knowing the system's commit-state of a Duty  */
	@SuppressWarnings({ "unchecked", "rawtypes" })
	private void linkFuturePromises(final Collection<Reply> replies, final long maxWaitMs) { 
		for (final Reply reply: replies) {
			// only successful replies
			if ((reply.getValue()==ReplyValue.SUCCESS)) {
				try {
					reply.setFuture((Future)executor.submit(
						new FutureTask<CommitState>(()-> { 
							final CommitState dcs = parkingThreads.wait(reply.getEntity(), maxWaitMs);
							reply.withTiming(Reply.Timing.CLIENT_RECEIVED_STATE_TS, System.currentTimeMillis());
							logger.info("{}: Client op done: {}: for: {} ({} ms)", 
									getClass().getSimpleName(), reply.getState(), reply.getEntity().getId(),
									reply.getTimeElapsedSoFar());
							return dcs;
						})));
				} catch (Exception e) {
					logger.error("{}: Unexpected linking promise for {}", getClass().getSimpleName(), reply, e);
				}
			} else {
				logger.warn("{}: Reply without promise ({}): {}", getClass().getSimpleName(), reply, reply.getEntity().getId());
			}
		}
	}
	
	private boolean sendWithRetries(final CommitBatchRequest request) {
		int tries = 10;
		boolean sent = false;
		while (!Thread.interrupted() && !sent && tries-->0) {
			if (leaderAware.getLeaderShardId()!=null) {
				final BrokerChannel channel = eventBroker.buildToTarget(config, 
						Channel.CLITOLEAD,
						leaderAware.getLeaderShardId());
				sent = eventBroker.send(channel, request);
			} else {
				try {
					Thread.sleep(config.beatToMs(10));
				} catch (InterruptedException ie) {
					break;
				}
			}
		}
		return sent;
	}
	
	private List<ShardEntity> toEntities(
			final Collection<? extends Entity> raws, 
			final EntityEvent event,
			final EntityPayload userPayload) {
		
		final List<ShardEntity> tmp = new ArrayList<>();
		for (final Entity e: raws) {
			final ShardEntity.Builder builder = ShardEntity.Builder.builder(e);
			if (userPayload != null) {
				builder.withPayload(userPayload);
			}
			final ShardEntity tmpp = builder.build();
			tmpp.getCommitTree().addEvent(
					event, 
					EntityState.PREPARED,  
					this.shardId, 
					CommitTree.PLAN_NA);
			tmp.add(tmpp);
		}
		return tmp;
	}

}
