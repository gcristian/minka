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
package io.tilt.minka.broker.impl;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.function.Consumer;

import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.tilt.minka.api.Config;
import io.tilt.minka.broker.EventBroker;
import io.tilt.minka.core.task.LeaderShardContainer;
import io.tilt.minka.core.task.Scheduler;
import io.tilt.minka.core.task.Scheduler.Frequency;
import io.tilt.minka.core.task.Scheduler.PriorityLock;
import io.tilt.minka.core.task.Semaphore.Action;
import io.tilt.minka.domain.NetworkShardID;
import io.tilt.minka.domain.ShardID;
import io.tilt.minka.spectator.MessageMetadata;

/**
 * Async TCP socket broker based on Netty framework
 * Every shard's broker runs a client listener server using {@linkplain SocketServer} 
 * while outbound messages from it are created on demand using a {@linkplain SocketClient}
 * 
 * Although brokers are directly connected: they dont talk, 
 * i.e. clients dont wait for an answer, servers dont produce it, 
 * they both serve the {@linkplain EventBroker} contract, staying functionally asynchronous 
 * for fluid though slow-paced orchestration, leveraging damage contention 
 * 
 * @author Cristian Gonzalez
 * @since Jan 31, 2016
 */
public class SocketBroker extends AbstractBroker implements EventBroker {

	private final Logger logger = LoggerFactory.getLogger(getClass());

	private final Config config;
	private final LeaderShardContainer leaderShardContainer;
	private final Scheduler scheduler;
	private SocketServer server;

	private Map<DirectChannel, SocketClient> clients;

	public SocketBroker(Config config, NetworkShardID shardId, LeaderShardContainer leaderContainerShard,
			Scheduler scheduler) {

		super(shardId);
		this.config = config;
		this.clients = new HashMap<>();
		this.scheduler = scheduler;
		this.leaderShardContainer = leaderContainerShard;
	}

	@Override
	public void start() {
		if (server == null) {
			this.leaderShardContainer.observeForChange((s) -> onLeaderChange(s));
			scheduler
					.schedule(scheduler
							.getAgentFactory().create(Action.DISCARD_OBSOLETE_CONNECTIONS, PriorityLock.HIGH_ISOLATED,
									Frequency.PERIODIC, () -> discardObsoleteClients())
							.delayed(5000).every(2000).build());

			logger.info("{}: Creating SocketServer", getClass().getSimpleName());
			getShardId().leavePortReservation();
			this.server = new SocketServer(this, config.getBroker().getConnectionHandlerThreads(),
					getShardId().getInetPort(), getShardId().getInetAddress().getHostAddress(),
					config.getBroker().getNetworkInterfase(), scheduler, config.getBroker().getRetryDelayMs(),
					config.getBroker().getMaxRetries(), getShardId().toString());
		}
	}

	@Override
	public void stop() {
		closeServer();
		closeClients();
	}

	/**
	 * when a target shard restarts and/or current shard's client losses its connection
	 * the handler is unaware of it and goes to the limb (a client is rebuilt) and discarded for safe resource mgmt 
	 */
	private void discardObsoleteClients() {
		try {
			final Iterator<Entry<DirectChannel, SocketClient>> it = clients.entrySet().iterator();
			while (it.hasNext()) {
				final Entry<DirectChannel, SocketClient> ch = it.next();
				if (ch.getValue().hasExpired()) {
					logger.warn("{}: ({}) DISCARDING obsolete client: {} for channel: {}", getClass().getSimpleName(),
							getShardId(), ch.getKey(), ch.getKey());
					ch.getValue().close();
					it.remove();
				}
			}
		} catch (Exception e) {
			logger.error("{}: Unexpected while discarding old channels", getClass().getSimpleName(), e);
		}
	}

	private void onLeaderChange(final NetworkShardID newLeader) {
		start();
		// close outbound connections if leader really changed
		final ShardID previous = leaderShardContainer.getPreviousLeaderShardId();
		if (previous == null || !previous.equals(newLeader)) {
			logger.info("{}: ({}) Closing client connections to previous leader: {}, cause new leader is: {}",
					getClass().getSimpleName(), super.getShardId(), previous, newLeader);
			closeClients();
		}
	}

	private void closeClients() {
		if (clients != null) {
			clients.forEach((x, y) -> y.close());
			clients.clear();
		}
	}

	private void closeServer() {
		if (server != null) {
			this.server.close();
		}
	}

	private synchronized boolean post(final BrokerChannel channel, final ChannelHint type, final Object event) {

		SocketClient client = this.clients.get(channel.getAddress());
		if (client == null) {
			logger.info("{}: ({}) CREATING SocketClient for Shard: {}", getClass().getSimpleName(), getShardId(),
					channel.getAddress());
			this.clients.put((DirectChannel)channel, client = new SocketClient(channel, scheduler,
					config.getBroker().getRetryDelayMs(), config.getBroker().getMaxRetries(), getShardId().toString(), config));
		}

		logger.debug("{}: ({}) Posting to Broker: {}:{} ({} into {}))", getClass().getSimpleName(), getShardId(),
				channel.getAddress().getInetAddress().getHostAddress(), channel.getAddress().getInetPort(),
				event.getClass().getSimpleName(), channel.getChannel());

		return client.send(new MessageMetadata(event, channel.getChannel().name()));
	}

	@Override
	public boolean postEvent(final BrokerChannel channel, final Serializable event) {
		return post(channel, channel.getChannel().getType(), event);
	}

	@Override
	public boolean postEvents(final BrokerChannel channel, final List<Serializable> event) {
		return post(channel, channel.getChannel().getType(), event);
	}

	@Override
	public boolean postEvent(final BrokerChannel channel, final ChannelHint type, final Serializable event) {
		return post(channel, type, event);
	}

	@Override
	public boolean unsubscribe(BrokerChannel brokerChannel, Class<? extends Serializable> eventType,
			Consumer<Serializable> driver) {
		return true;
	}

	@Override
	public void setBrokerShutdownCallback(Runnable callback) {
		//server.setShutdownCallback(callback);
	}

	public class DirectChannel implements BrokerChannel {
		private final NetworkShardID target;
		private final Channel channel;
		private DateTime creation;

		public DirectChannel(final String serviceNanme, final NetworkShardID target, final Channel channel) {
			this.target = target;
			this.channel = channel;
			// this way my channels are unique using shard's creation date 
			// to avoid shard's recreation causing limbic clients
			// without relying on business-package's callbacks
			if (target != null) {
				// there're channels without a target in the broker's contract
				this.creation = target.getCreation();
			}
		}

		private DateTime getCreation() {
			return this.creation;
		}

		@Override
		public Channel getChannel() {
			return channel;
		}

		@Override
		public String getFullName() {
			return this.target.getStringIdentity();
		}

		@Override
		public NetworkShardID getAddress() {
			return target;
		}

		@Override
		public int hashCode() {
			return new HashCodeBuilder().append(getCreation()).append(getAddress()).toHashCode();
		}

		@Override
		public boolean equals(Object obj) {
			if (obj != null && obj instanceof DirectChannel) {
				DirectChannel channel = (DirectChannel) obj;
				return new EqualsBuilder().append(channel.getAddress(), getAddress())
						.append(channel.getCreation(), getCreation()).isEquals();
			} else {
				return false;
			}
		}
	}

	@Override
	protected boolean onSubscription(BrokerChannel channel, Class<? extends Serializable> eventType,
			Consumer<Serializable> driver, long sinceTimestamp) {
		return true;
	}

	@Override
	public BrokerChannel build(Config config, Channel channel) {
		return build(config.getBootstrap().getServiceName(), channel);
	}

	@Override
	public BrokerChannel build(String service, Channel channel) {
		return new DirectChannel(service, null, channel);
	}

	@Override
	public BrokerChannel buildToTarget(Config config, Channel channel, NetworkShardID shardId) {
		return new DirectChannel(config.getBootstrap().getServiceName(), shardId, channel);
	}

	@Override
	public BrokerChannel buildToTarget(String service, Channel channel, NetworkShardID shardId) {
		return new DirectChannel(service, shardId, channel);
	}

}
