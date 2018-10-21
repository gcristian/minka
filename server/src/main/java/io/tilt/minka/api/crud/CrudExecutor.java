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
package io.tilt.minka.api.crud;

import static java.util.Collections.singleton;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import org.apache.commons.lang.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

import io.tilt.minka.api.Config;
import io.tilt.minka.api.config.SchedulerSettings;
import io.tilt.minka.api.crud.CommitBatch.CommitBatchRequest;
import io.tilt.minka.api.crud.CommitBatch.CommitBatchResponse;
import io.tilt.minka.broker.EventBroker;
import io.tilt.minka.broker.EventBroker.BrokerChannel;
import io.tilt.minka.broker.EventBroker.Channel;
import io.tilt.minka.core.leader.ClientEventsHandler;
import io.tilt.minka.core.leader.LeaderBootstrap;
import io.tilt.minka.core.task.LeaderAware;
import io.tilt.minka.domain.CommitTree;
import io.tilt.minka.domain.EntityEvent;
import io.tilt.minka.domain.EntityState;
import io.tilt.minka.domain.ShardEntity;
import io.tilt.minka.domain.ShardedPartition;
import io.tilt.minka.model.Entity;
import io.tilt.minka.model.EntityPayload;
import io.tilt.minka.shard.NetworkShardIdentifier;
import io.tilt.minka.shard.ShardIdentifier;

/**
 * Transfers the CRUD operation from Client to the Leader. 
 * With a fire and forget mechanism to send Leader write requests, 
 * and an async-promise response mechanism, to request for Leader reply and duty state data.
 * It has the logic to retry requests when leader reelection occurs.
 * 
 * @since Sept 1, 2018
 */
public class CrudExecutor {

	static final Logger logger = LoggerFactory.getLogger(CrudExecutor.class);

	private static final int MAX_REELECTION_TOLERANCE = 10;
	static final int RETRY_SLEEP = 1000;
	static final int MAX_RETRIES = 3;
	private static final int MAX_THREADS = 8;
	private static final int MAX_CONCURRENT = Short.MAX_VALUE;
	
	private final Config config;
	private final LeaderBootstrap leaderBootstrap;
	private final EventBroker eventBroker;
	private final ClientEventsHandler clientHandler;
	private final ShardIdentifier shardId;
	private final LeaderAware leaderAware;
	private final LatchHandler latchHandler;
	private final ShardedPartition partition;
	
	private CommitLinkage linkage;
			
	int maxWaitMs = LatchHandler.NO_EXPIRATION;

	private BlockingQueue<Runnable> replyQueue;
	private BlockingQueue<Runnable> stateQueue;
	private ThreadPoolExecutor replyPool;
	private ThreadPoolExecutor statePool;

	protected CrudExecutor(
			final Config config, 
			final LeaderBootstrap leaderBootstrap, 
			final EventBroker eventBroker,
			final ClientEventsHandler mediator, 
			final ShardIdentifier shardId, 
			final LeaderAware leaderAware,
			final LatchHandler latchHandler,
			final ShardedPartition partition) {
		this.config = config;
		this.leaderBootstrap = leaderBootstrap;
		this.eventBroker = eventBroker;
		this.clientHandler = mediator;
		this.shardId = shardId;
		this.leaderAware = leaderAware;
		this.latchHandler = latchHandler;
		this.partition = partition;
	}

	void setFutureMaxWaitMs(int futureMaxWaitMs) {
		this.maxWaitMs = futureMaxWaitMs;
	}

	
	private synchronized void initReplyPool() {
		if (replyPool==null) {
			replyPool = new ThreadPoolExecutor(0, MAX_THREADS, 30, TimeUnit.SECONDS, 
					replyQueue = new ArrayBlockingQueue<Runnable>(MAX_CONCURRENT), 
					new ThreadFactoryBuilder()
						.setNameFormat(SchedulerSettings.PNAME + "CLRP")
						.build(), 
				new ThreadPoolExecutor.AbortPolicy());
		}
	}

	private synchronized void initStatePool() {
		if (statePool==null) {
			statePool = new ThreadPoolExecutor(0, MAX_THREADS, 30, TimeUnit.SECONDS, 
					stateQueue = new ArrayBlockingQueue<>(MAX_CONCURRENT), 
					new ThreadFactoryBuilder()
						.setNameFormat(SchedulerSettings.PNAME + "CLRP")
						.build(), 
				new ThreadPoolExecutor.AbortPolicy());
			linkage = new CommitLinkage(latchHandler, leaderAware, partition, statePool, maxWaitMs);
		}
	}

	/** flags the request so the leader knows it must not respond a reply or state back */
	Collection<Reply> fireAndForget(
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

	Collection<Reply> executeBlockingWithResponse(
			final Collection<? extends Entity> raws, 
			final EntityEvent event, 
			final EntityPayload userPayload) {
		
		Validate.notNull(raws, "an entity is required");
		initReplyPool();
		initStatePool();
		return resolve(raws, event, userPayload);
	}

	Future<Collection<Reply>> executeWithResponse(
			final Collection<? extends Entity> raws, 
			final EntityEvent event, 
			final EntityPayload userPayload) {
		
		Validate.notNull(raws, "an entity is required");
		initReplyPool();
		initStatePool();
		return replyPool.submit(()->resolve(raws, event, userPayload));
	}

	Future<Reply> executeWithResponseSingle(
			final Entity e, 
			final EntityEvent event, 
			final EntityPayload payload) {
		
		Validate.notNull(e, "an entity is required");
		initReplyPool();
		initStatePool();
		return replyPool.submit(() -> resolveSingle(e, event, payload));
	}
	
	private Reply resolveSingle(
			final Entity e, 
			final EntityEvent event, 
			final EntityPayload userPayload) {
		final long start = System.currentTimeMillis();
		final Set<Reply> replies = new HashSet<>(1);
		initStatePool();
		forwardLeader(event, start, replies, toEntities(Arrays.asList(e), event, userPayload));
		return replies.iterator().next();
	}

	private Collection<Reply> resolve(
			final Collection<? extends Entity> raws, 
			final EntityEvent event, 
			final EntityPayload userPayload) {
		final long start = System.currentTimeMillis();
		final Set<Reply> replies = new HashSet<>(raws.size());
		forwardLeader(event, start, replies, toEntities(raws, event, userPayload));
		return replies;
	}

	private Void forwardLeader(
			final EntityEvent event, 
			final long start, 
			final Set<Reply> source, 
			final List<ShardEntity> entities) {
		try {
			final boolean futurable = entities.iterator().next().getType()==ShardEntity.Type.DUTY;
			final Function<Reply, Void> retrier = (Reply reply)-> forwardLeader(event, 
					reply.getTiming(Reply.Timing.CLIENT_CREATED_TS), 
					Collections.singleton(reply), 
					toEntities(
							Collections.singletonList(reply.getEntity()), 
							event, null));
			if (leaderBootstrap.inService()) {
				clientHandler.mediateOnEntity(entities, r-> {
					r.withTiming(Reply.Timing.CLIENT_CREATED_TS, start);
					r.withTiming(Reply.Timing.LEADER_REPLY_TS, System.currentTimeMillis());
					CommitBatchRequestResponse.addOrReplace(source, r);
				}, true);
				timeReceived(source);
			} else {
				requestBatch(entities).mergeOnSource(source);
			}
			if (futurable) {
				linkage.assignFutures(source, event, retrier);
			}
		} catch (Exception e) {
			logger.error("Cannot mediate to leaderBootstrap", e);
			source.add(Reply.error(e));
		}
		return null;
	}

	private CommitBatchRequestResponse requestBatch(final List<ShardEntity> entities) {
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
				response = latchHandler.waitAndGet(request.getId(), maxWaitMs);
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

	static class CommitBatchRequestResponse {
		
		private final CommitBatchResponse response;
		private final boolean sent;
		private final CommitBatchRequest request;
		
		CommitBatchRequestResponse(
				final CommitBatchRequest request, 
				final CommitBatchResponse response, 
				final boolean sent) {
			super();
			this.request = request;
			this.response = response;
			this.sent = sent;
		}
		boolean isSent() {
			return sent;
		}
		CommitBatchResponse getResponse() {
			return response;
		}
		CommitBatchRequest getRequest() {
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
 
	public Map<String, String> replyPool() {
		if (replyPool==null) {
			return Collections.emptyMap();
		}
		return poolToMap(replyPool, "replies", replyQueue);
	}
	
	public Map<String, String> statePool() {
		if (statePool==null) {
			return Collections.emptyMap();
		}
		return poolToMap(statePool, "commits", stateQueue);
	}
	
	private Map<String, String> poolToMap(final ThreadPoolExecutor tpe, final String prefix, final BlockingQueue q) {
		final Map<String, String> m = new LinkedHashMap<>();
		m.put(prefix+"-queuesize", String.valueOf(q.size()));
		m.put(prefix+"-active-threads", String.valueOf(tpe.getActiveCount()));
		m.put(prefix+"-submitted-tasks", String.valueOf(tpe.getTaskCount()));
		m.put(prefix+"-completed-tasks", String.valueOf(tpe.getCompletedTaskCount()));
		m.put(prefix+"-poolsize", String.valueOf(tpe.getPoolSize()));
		return m;
	}

}
