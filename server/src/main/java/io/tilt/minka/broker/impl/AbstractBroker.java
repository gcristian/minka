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
import java.util.ArrayList;
import java.util.Collection;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;

import io.tilt.minka.broker.EventBroker;
import io.tilt.minka.core.task.Service;
import io.tilt.minka.domain.NetworkShardIdentifier;
import io.tilt.minka.spectator.MessageMetadata;

/**
 * Registers drivers subscriptons to events and delegate events to their registered drivers
 * 
 * @author Cristian Gonzalez
 * @since Nov 25, 2015
 */
public abstract class AbstractBroker implements Service, EventBroker, Consumer<MessageMetadata> {

	private final Logger logger = LoggerFactory.getLogger(getClass());
	/* save (channel-eventType) -> (many consumers) */
	private Multimap<String, Consumer<Serializable>> consumerPerChannelEventType;
	/* save (consumer) -> (many channeles) */
	private Multimap<Consumer<Serializable>, String> channelsPerConsumer;
	private final NetworkShardIdentifier shardId;

	public AbstractBroker(final NetworkShardIdentifier shardId) {
		this.shardId = shardId;
		this.consumerPerChannelEventType = HashMultimap.create();
		this.channelsPerConsumer = HashMultimap.create();
	}

	public NetworkShardIdentifier getShardId() {
		return this.shardId;
	}

	@Override
	public void accept(final MessageMetadata meta) {
		if (logger.isInfoEnabled()) {
			logger.info("{}: ({}) Receiving {}", getClass().getSimpleName(), shardId, meta.getPayloadType());
		}
		String key = meta.getInbox() + meta.getPayloadType().getSimpleName();
		if (logger.isDebugEnabled()) {
		    logger.debug("{}: ({}) Looking subscribed consumer to Key: {}", getClass().getSimpleName(), shardId, key);
		}

		Collection<Consumer<Serializable>> consumers = consumerPerChannelEventType.get(key);
		if (!consumers.isEmpty()) {
			consumers.forEach(i -> i.accept((Serializable) meta.getPayload()));
		} else {
			logger.error("{}: ({}) No Subscriber for incoming event: {} at channel: {}", getClass().getSimpleName(), shardId,
					meta.getPayloadType(), meta.getInbox());
		}
	}

	protected abstract boolean onSubscription(
			final BrokerChannel channel, 
			final Class<? extends Serializable> eventType,
			final Consumer<Serializable> consumer, 
			final long sinceTimestamp);

	@Override
	public void subscribeEvents(
			final BrokerChannel buildToTarget, 
			final Consumer<Serializable> driver, 
			final long sinceNow, 
			final @SuppressWarnings("unchecked") Class<? extends Serializable>...classes) {
		for (int i=0;i<classes.length;i++) {
			subscribe(buildToTarget, classes[i], driver, sinceNow);
		}
	}

	@Override
	public final boolean subscribe(
			final BrokerChannel channel, 
			final Class<? extends Serializable> eventType,
			final Consumer<Serializable> consumer, 
			final long sinceTimestamp) {

		try {
			// TODO para Pathable usar getFullName...
			final String key = channel.getChannel().name() + eventType.getSimpleName();
			final Collection<Consumer<Serializable>> drivers = consumerPerChannelEventType.get(key);
			if (drivers != null && drivers.contains(consumer)) {
				logger.warn("{}: ({}) Already subscribed to channel-eventType: {}", getClass().getSimpleName(), shardId,
						key);
				return true;
			} else {
				if (logger.isInfoEnabled()) {
					logger.info("{}: ({}) {} Subscribing channel: {} with Type: {} ",
						getClass().getSimpleName(), shardId, consumer.getClass().getSimpleName(), channel.getChannel().name(), 
						eventType.getSimpleName());
			    }
			}

			Collection<String> channeles = channelsPerConsumer.get(consumer);
			if (channeles != null) {
				channeles = channeles.stream().filter(i -> i.equals(channel.getFullName()))
						.collect(Collectors.toCollection(ArrayList::new));
			}

			if (logger.isInfoEnabled()) {
				logger.debug("{}: ({}) Saving handler: {} on Key: {}", getClass().getSimpleName(),
					channel.getAddress().toString(), consumer.getClass().getSimpleName(), key);
			}

			consumerPerChannelEventType.put(key, consumer);
			if (channeles.isEmpty()) {
				channelsPerConsumer.put(consumer, channel.getFullName());
				if (!onSubscription(channel, eventType, consumer, sinceTimestamp)) {
					throw new RuntimeException("Event subscription not guaranteed");
				}
			} else {
				// already done (Spectator supports subscription once: consumer->channel, ignores types)
				// so we cannot subscribe'em twice, though we can redirect to our consumer indexing by type
				return true;
			}
		} catch (Exception e) {
			throw new RuntimeException("Event subscription error on channel:" + channel.getAddress().toString(), e);
		}

		return true;
	}

}
