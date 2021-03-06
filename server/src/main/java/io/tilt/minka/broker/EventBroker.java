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
package io.tilt.minka.broker;

import static io.tilt.minka.broker.EventBroker.ChannelHint.EVENT_QUEUE;
import static io.tilt.minka.broker.EventBroker.ChannelHint.EVENT_SET;

import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import io.tilt.minka.api.Config;
import io.tilt.minka.core.task.Service;
import io.tilt.minka.shard.NetworkShardIdentifier;

/**
 * Transport-agnostic asynchronic exchange to decouple messages between shards.
 * Must remain ignorant about their circumstancial leader and follower roles
 * 
 * @author Cristian Gonzalez
 * @since Nov 23, 2015
 */
public interface EventBroker extends Service {

	/**
	 * A shard-targeted channel 
	 */
	interface BrokerChannel {
		Channel getChannel();

		String getFullName();

		NetworkShardIdentifier getAddress();
	}

	enum ChannelHint {
		/*
		 * for the broker impl to interpret it as an accumulation of events
		 * that may be read alltogether or by time-windows, but non overridable
		 * events.
		 */
		EVENT_QUEUE,
		/*
		 * for the broker impl to interpret it as an overriding event where
		 * only matters the last of each event they are a set of unique among
		 * the same path or folder or topic
		 */
		EVENT_SET;
	};

	public enum Channel {

		/* where the followers put their events to the leader */
		FOLLTOLEAD(EVENT_SET),
		/* where the client put its events to the leader */
		CLITOLEAD(EVENT_QUEUE),
		/* where the leader puts its partition messags to followers */
		LEADTOFOLL(EVENT_QUEUE);

		private final ChannelHint type;

		Channel(final ChannelHint type) {
			this.type = type;
		}

		public ChannelHint getType() {
			return this.type;
		}
	}
	
	BrokerChannel build(Config config, Channel channel);

	default BrokerChannel build(String service, Channel channel) {
		throw new RuntimeException("Unmandatory build was required");
	}

	BrokerChannel buildToTarget(
			Config config, 
			Channel channel, 
			NetworkShardIdentifier shardId);


	/* send an event object to an inbox name */
	boolean send(BrokerChannel channel, Serializable event);

	/* send an event object list to an inbox name */
	boolean send(BrokerChannel channel, List<Serializable> event);

	/* idem overriding channel type */
	boolean send(BrokerChannel channel, ChannelHint hint, Serializable event);

	/* use a driver to handle events of a certain type */
	boolean subscribe(
			BrokerChannel channel, 
			Class<? extends Serializable> type, 
			Consumer<Serializable> driver,
			long sinceTimestamp);

	void subscribe(
			BrokerChannel channel, 
			Consumer<Serializable> driver, 
			long sinceNowLapse, 
			Class<? extends Serializable>... classes);

	/* unregister the driver handling events of a certain type */
	boolean unsubscribe(
			BrokerChannel channel, 
			Class<? extends Serializable> eventType,
			Consumer<Serializable> driver);
	
	Map<BrokerChannel, Object> getSendMetrics();
	Map<String, Map> getReceptionMetrics();
	
}
