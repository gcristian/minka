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

import static io.tilt.minka.broker.EventBroker.ChannelHint.EVENT_SET;

import java.io.Serializable;
import java.util.List;
import java.util.function.Consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.tilt.minka.api.NewConfig;
import io.tilt.minka.broker.EventBroker;
import io.tilt.minka.core.task.impl.SpectatorSupplier;
import io.tilt.minka.domain.NetworkShardID;
import io.tilt.minka.spectator.MessageMetadata;
import io.tilt.minka.spectator.Queues;
import io.tilt.minka.spectator.Spectator;
import io.tilt.minka.spectator.Wells;

/**
 * Use {@link Spectator} to receive/send messages with Zookeeper storage
 * @author Cristian Gonzalez
 * @since Nov 25, 2015
 */
public class ZookeeperBroker extends AbstractBroker implements EventBroker, Consumer<MessageMetadata> {

	private final Logger logger = LoggerFactory.getLogger(getClass());

	private final Queues queues;
	private final Wells wells;

	public ZookeeperBroker(final NewConfig config, final NetworkShardID shardId) {
		super(shardId);
		this.queues = new Queues(config.getBootstrap().getZookeeperHostPort());
		this.wells = new Wells(config.getBootstrap().getZookeeperHostPort());
	}

	@Override
	public BrokerChannel buildToTarget(String service, Channel channel, NetworkShardID shardId) {
		return new PathableChannel(service, channel, shardId.getStringIdentity());
	}

	@Override
	public BrokerChannel buildToTarget(final NewConfig config, final Channel channel, final NetworkShardID shardId) {
		return buildToTarget(config.getBootstrap().getServiceName(), channel, shardId);
	}

	@Override
	public BrokerChannel build(final NewConfig config, final Channel channel) {
		return build(config.getBootstrap().getServiceName(), channel);
	}

	@Override
	public BrokerChannel build(final String service, final Channel channel) {
		return new PathableChannel(service, channel, null);
	}

	public class PathableChannel implements BrokerChannel {
		private final Channel channel;
		private final String suffix;
		private final String serviceName;

		private PathableChannel(final String serviceName, final Channel channel, final String usageName) {
			this.suffix = usageName;
			this.channel = channel;
			this.serviceName = serviceName;
		}

		public Channel getChannel() {
			return this.channel;
		}

		public String getFullName() {
			return SpectatorSupplier.MINKA_SUBDOMAIN + "/" + serviceName + "/" + channel.name()
					+ (suffix == null ? "" : suffix);
		}

		@Override
		public NetworkShardID getAddress() {
			return null; /*
								 * ShardID.valueOf(MINKA_SUBDOMAIN + "/" +
								 * serviceName + "/" + channel.getName() + (suffix ==
								 * null ? "" : suffix));
								 */
		}
	}

	@Override
	public boolean postEvent(final BrokerChannel channel, ChannelHint type, final Serializable event) {
		return post(channel, type, event);
	}

	@Override
	public boolean postEvent(final BrokerChannel channel, final Serializable event) {
		return postEvent(channel, channel.getChannel().getType(), event);
	}

	@Override
	public boolean postEvents(final BrokerChannel channel, List<Serializable> events) {
		return post(channel, channel.getChannel().getType(), events);
		//events.forEach(i->postEvent(channel, i));
	}

	private boolean post(final BrokerChannel channel, ChannelHint type, final Object event) {
		logger.info("{}: posting into channel: {} a new event: {}", getClass().getSimpleName(), channel.getFullName(),
				event.toString());
		if (channel.getChannel().getType() == EVENT_SET || type == EVENT_SET) {
			return wells.updateWell(channel.getFullName(), event);
		} else {
			return queues.postBroadcastMessage(channel.getFullName(), event);
		}
	}

	@Override
	public boolean unsubscribe(final BrokerChannel channel, final Class<? extends Serializable> eventType,
			final Consumer<Serializable> handler) {

		logger.info("{}: Unsubscribing to channel: {} with Type: {} with Consumer: {} ", getClass().getSimpleName(),
				channel.getFullName(), eventType.getSimpleName(), handler.getClass().getSimpleName());

		if (channel.getChannel().getType() == EVENT_SET) {
			wells.closeWell(channel.getFullName());
		} else {
			queues.stopSubscription(channel.getFullName());
		}
		return true;
	}

	@Override
	protected boolean onSubscription(BrokerChannel channel, Class<? extends Serializable> eventType,
			Consumer<Serializable> driver, long sinceTimestamp) {

		try {
			return (channel.getChannel().getType() == EVENT_SET) ? wells.runOnUpdate(channel.getFullName(), this)
					: queues.runAsSubscriber(channel.getFullName(), this, sinceTimestamp, 1000 * 30);
		} catch (Exception e) {
			throw new RuntimeException("Event subscription error", e);
		}
	}

	@Override
	public void setBrokerShutdownCallback(Runnable callback) {
		queues.setConnectionLostCallback(callback);
		wells.setConnectionLostCallback(callback);
	}

	@Override
	public void start() {

	}

	@Override
	public void stop() {
		this.queues.close();
		this.wells.close();
	}

}
