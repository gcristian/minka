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
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.stream.Collectors;

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
import io.tilt.minka.domain.ShardedPartition;
import io.tilt.minka.shard.NetworkShardIdentifier;
import io.tilt.minka.shard.ShardIdentifier;

/**
 * Handles CRUD ops from Client. 
 * With a fireAndForget mechanism to send Leader write requests, 
 * and an async-promise response mechanism, to request for Leader reply and duty state data.
 * It has the logic to retry requests when leader reelection occurs.
 * 
 * @since Sept 1, 2018
 */
class CrudExecutor {

	private static final Logger logger = LoggerFactory.getLogger(CrudExecutor.class);

	private static final int MAX_REELECTION_TOLERANCE = 10;

	private static final int MAX_RETRIES = 3;

	private static final long RETRY_SLEEP = 1000;

	private final Config config;
	private final LeaderBootstrap leaderBootstrap;
	private final EventBroker eventBroker;
	private final ClientEventsHandler clientHandler;
	private final ShardIdentifier shardId;
	private final LeaderAware leaderAware;
	private final RequestLatches requestLatches;
	private final ShardedPartition partition;
	
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
			final RequestLatches requestLatches,
			final ShardedPartition partition) {
		this.config = config;
		this.leaderBootstrap = leaderBootstrap;
		this.eventBroker = eventBroker;
		this.clientHandler = mediator;
		this.shardId = shardId;
		this.leaderAware = leaderAware;
		this.requestLatches = requestLatches;
		this.partition = partition;
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
		return executor.submit(() -> resolve(raws, event, userPayload, futureMaxWaitMs));
	}

	Future<Reply> executeSingle(
			final long maxWaitMs,
			final Collection<? extends Entity> raws, 
			final EntityEvent event, 
			final EntityPayload payload) {
		
		Validate.notNull(raws, "an entity is required");
		return executor.submit(() -> resolve(raws, event, payload, maxWaitMs).iterator().next());
	}

	private Collection<Reply> resolve(
			final Collection<? extends Entity> raws, 
			final EntityEvent event, 
			final EntityPayload userPayload,
			final long maxWaitMs) {
		final long start = System.currentTimeMillis();
		final Set<Reply> replies = new HashSet<>(raws.size());
		forwardLeader(event, maxWaitMs, start, replies, toEntities(raws, event, userPayload));
		return replies;
	}

	private void forwardLeader(
			final EntityEvent event, 
			final long maxWaitMs, 
			final long start, 
			final Set<Reply> source, 
			final List<ShardEntity> entities) {
		try {
			final boolean futurable = entities.iterator().next().getType()==ShardEntity.Type.DUTY;
			if (leaderBootstrap.inService()) {
				clientHandler.mediateOnEntity(entities, r-> {
					r.withTiming(Reply.Timing.CLIENT_CREATED_TS, start);
					r.withTiming(Reply.Timing.LEADER_REPLY_TS, System.currentTimeMillis());
					CommitBatchRequestResponse.addOrReplace(source, r);
				}, true);
				timeReceived(source);
			} else {
				requestBatch(entities, maxWaitMs).mergeOnSource(source);
			}
			if (futurable) {
				buildFutures(source, maxWaitMs, event);
			}
		} catch (Exception e) {
			logger.error("Cannot mediate to leaderBootstrap", e);
			source.add(Reply.error(e));
		}
	}
	
	public static class CommitBatchRequestResponse {
		private final CommitBatchResponse response;
		private final boolean sent;
		private final CommitBatchRequest request;
		public CommitBatchRequestResponse(
				final CommitBatchRequest request, 
				final CommitBatchResponse response, 
				final boolean sent) {
			super();
			this.request = request;
			this.response = response;
			this.sent = sent;
		}
		public boolean isSent() {
			return sent;
		}
		public CommitBatchResponse getResponse() {
			return response;
		}
		public CommitBatchRequest getRequest() {
			return request;
		}
		/** identify leader: send and wait for response  */
		void mergeOnSource(final Set<Reply> source) {
			if (getResponse()!=null ) {
				for (Reply r: getResponse().getReplies()) {
					if (source.isEmpty()) {
						source.add(r);
					} else {
						addOrReplace(source, r);
					}
				}
			} else {
				if (isSent()) {
					logger.warn("{}: {}: (RequestId: {})", CommitBatchRequestResponse.class.getSimpleName(), 
						ReplyValue.REPLY_TIMED_OUT, getRequest().getId());
					if (source.isEmpty()) {
						source.add(Reply.leaderReplyTimedOut());
					} else {
						source.iterator().next().copyFrom(Reply.leaderReplyTimedOut());
					}
				} else {
					logger.error("{}: Couldnt send entities: {}", CommitBatchRequestResponse.class.getSimpleName());
					if (source.isEmpty()) {
						source.add(Reply.failedToSend(null));
					} else {
						source.iterator().next().copyFrom(Reply.failedToSend(null));
					}
				}
			}
		}

		private static void addOrReplace(final Set<Reply> source, final Reply r) {
			if (!source.contains(r)) {
				source.add(r);
			} else {
				for (Reply o: source) {
					if (o.equals(r)) {
						o.copyFrom(r);
					}
				}
			}
		}
	}

	private CommitBatchRequestResponse requestBatch(final List<ShardEntity> entities, final long maxWaitMs) {
		long sentTs = 0;
		final CommitBatchRequest request = new CommitBatchRequest(entities, true);		
		CommitBatchResponse response = null;
		final long start = System.currentTimeMillis();
		// support until 3 leader changes in a row
		boolean sent = true;
		boolean reelection = true;
		
		for (int retries = MAX_REELECTION_TOLERANCE; 
				reelection && sent && retries>0 && !Thread.interrupted() && response==null; retries--) {
			final NetworkShardIdentifier prevLeader = leaderAware.getLeaderShardId();			
			if (sent = sendWithRetries(request)) {
				sentTs = System.currentTimeMillis();
				response = requestLatches.wait(request.getId(), maxWaitMs);
				final boolean newleader = reelection = leaderAware.getLastLeaderChange().toEpochMilli() > start
					&& !leaderAware.getLeaderShardId().equals(prevLeader);
				if (response==null && newleader) {
						logger.warn("{}: New leader, repeating operation: (RequestId: {})", 
								getClass().getSimpleName(), request.getId());
				} else if (response!=null) {
					final long now = System.currentTimeMillis();
					for (Reply r: response.getReplies()) {				
						r.withTiming(Reply.Timing.CLIENT_CREATED_TS, start);
						r.withTiming(Reply.Timing.CLIENT_SENT_TS, sentTs);
						r.withTiming(Reply.Timing.CLIENT_RECEIVED_REPLY_TS, now);
					}
				}
			}
		}
		return new CommitBatchRequestResponse(request, response, sent);
	}

	private void timeReceived(final Set<Reply> replies) {
		long now = System.currentTimeMillis();
		replies.forEach(r->r.withTiming(Reply.Timing.CLIENT_RECEIVED_REPLY_TS, now));
	}

	/** links the reply with a Future knowing the system's commit-state of a Duty  
	 * @param event */
	@SuppressWarnings({ "unchecked", "rawtypes" })
	private void buildFutures(final Collection<Reply> replies, final long maxWaitMs, final EntityEvent event) { 
		final NetworkShardIdentifier prevLeader = leaderAware.getLeaderShardId();
		for (final Reply reply: replies) {
			// only successful replies
			if ((reply.getValue()==ReplyValue.SUCCESS)) {
				try {
					if (reply.getRetryCounter() == 0) {
						reply.setState((Future)executor.submit(
							new FutureTask<CommitState>(()-> 
								waitOrRetry(maxWaitMs, event, prevLeader, reply)
							)));
					} else {
						// Retry: we're already in a blocking future
						// replies is a collection of 1 object so we can block
						reply.setState(CompletableFuture.completedFuture(
								waitOrRetry(maxWaitMs, event, prevLeader, reply)));
					}
				} catch (Exception e) {
					logger.error("{}: Unexpected linking promise for {}", getClass().getSimpleName(), reply, e);
				}
			} else {
				logger.warn("{}: Reply without promise ({}): {}", getClass().getSimpleName(), 
						reply, reply.getEntity().getId());
			}
		}
	}

	/** block and sleep until leader response or retry while quota 
	 * @throws InterruptedException */
	private CommitState waitOrRetry(
			final long maxWaitMs, 
			final EntityEvent event, 
			final NetworkShardIdentifier prevLeader, 
			final Reply reply) throws InterruptedException {
		
		CommitState cs = requestLatches.wait(reply.getEntity(), maxWaitMs);
		if (cs == null) {
			cs = determineState(prevLeader, reply, event);
			if (cs == CommitState.REJECTED) {
				if (reply.getRetryCounter() < MAX_RETRIES) {
					reply.addRetry();
					// take adaptation period
					Thread.sleep(RETRY_SLEEP);
					logger.warn("{}: Retrying after leader reelection, duty: {}", 
							getClass().getSimpleName(), reply.getEntity());
					forwardLeader(event, maxWaitMs, 
							reply.getTiming(Reply.Timing.CLIENT_CREATED_TS), 
							Collections.singleton(reply), 
							toEntities(
									Collections.singletonList(reply.getEntity()), 
									event, null));
				} else {
					logger.error("{}: Reply from Leader retries exhausted for {}", getClass().getSimpleName(), reply);								
				}
			}
		} else {
			reply.withTiming(Reply.Timing.CLIENT_RECEIVED_STATE_TS, System.currentTimeMillis());
			logger.info("{}: Client op done: {}: for: {} ({} ms)", getClass().getSimpleName(), 
					cs, reply.getEntity().getId(), reply.getTimeElapsedSoFar());
		}
		return cs;
	}

	private CommitState determineState(final NetworkShardIdentifier prevLeader, final Reply reply, final EntityEvent ee) {
		// a good reason for leader indifference: reelection happened
		if (leaderAware.getLastLeaderChange().toEpochMilli() 
					> reply.getTiming(Reply.Timing.CLIENT_RECEIVED_REPLY_TS)
				&& !leaderAware.getLeaderShardId().equals(prevLeader)) {
			return convalidate(reply, ee) ? CommitState.FINISHED : CommitState.REJECTED;
		} else {
			logger.error("{}: Client op cancelled for starvation!: for: {} ({} ms)", 
				getClass().getSimpleName(), reply.getEntity().getId(), reply.getTimeElapsedSoFar());
			return CommitState.CANCELLED;
		}
	}

	/**
	 * now how is the current CommitedState about the asked operation ?
	 * convalidate the current state if it reflects the user's intention
	 */
	private boolean convalidate(final Reply r, final EntityEvent ee) {
		// TODO FALSO myself may not be involved
		final Optional<ShardEntity> se = this.partition.getDuties().stream()
				.filter(d->d.getDuty().equals(r.getEntity()))
				.findFirst();
		if (ee==EntityEvent.CREATE && se.isPresent() || ee==EntityEvent.REMOVE && !se.isPresent()) {
			// leader changed after commiting request but before replying state
			return true;
		} else {
			//if (se.isPresent() && se.get().getCommitTree().getLast())
			logger.error("{}: Client op rejected for leader reelection!: for: {} ({} ms)", 
					getClass().getSimpleName(), r.getEntity().getId(), r.getTimeElapsedSoFar());
			return false;
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
			final EntityPayload payload) {
		
		final List<ShardEntity> tmp = new ArrayList<>();
		for (final Entity e: raws) {
			final ShardEntity.Builder builder = ShardEntity.Builder.builder(e);
			if (payload != null) {
				builder.withPayload(payload);
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
