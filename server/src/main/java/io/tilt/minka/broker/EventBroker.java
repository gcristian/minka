/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.tilt.minka.broker;

import static io.tilt.minka.broker.EventBroker.ChannelType.EVENT_QUEUE;
import static io.tilt.minka.broker.EventBroker.ChannelType.EVENT_SET;

import java.io.Serializable;
import java.util.List;
import java.util.function.Consumer;

import io.tilt.minka.api.Config;
import io.tilt.minka.business.Service;
import io.tilt.minka.domain.NetworkShardID;

/**
 * Transport-agnostic asynchronic exchange to decouple messages between shards.
 * Must remain ignorant about their circumstancial leader and follower roles
 * 
 * @author Cristian Gonzalez
 * @since Nov 23, 2015
 */
public interface EventBroker extends Service {

    BrokerChannel build(Config config, Channel channel);
    default BrokerChannel build(String service, Channel channel) {
        throw new RuntimeException("Unmandatory build was required");
    }
    
    BrokerChannel buildToTarget(Config config, Channel channel, NetworkShardID shardId);
    default BrokerChannel buildToTarget(String service, Channel channel, NetworkShardID shardId) {
        throw new RuntimeException("Unmandatory build was required");
    }
    
    /**
     * A shard-targeted channel 
     */
    interface BrokerChannel {
        Channel getChannel();
        String getFullName();
        NetworkShardID getAddress();
    }
    
    enum ChannelType {
        /* for the broker impl to interpret it as an accumulation of events that 
         * may be read alltogether or by time-windows, but non overridable events. 
         */
        EVENT_QUEUE,
        /* for the broker impl to interpret it as an 
         * overriding event where only matters the last of each event
         * they are a set of unique among the same path or folder or topic
         */
        EVENT_SET;
    };

    public enum Channel {
        
        
        /* where the followers put their events to the leader */
        HEARTBEATS_TO_LEADER (EVENT_SET, "leader-hb-channel"),
        /* where the client put its events to the leader */
        CLIENT_TO_LEADER (EVENT_QUEUE, "client-channel"),
        /* where the leader puts its partition messags to followers */
        INSTRUCTIONS_TO_FOLLOWER (EVENT_QUEUE, "inbox-partition-shard");
        
        private final ChannelType type;
        private final String name;

        Channel(final ChannelType type, final String name) {
            this.type = type;
            this.name = name;
        }      
        public ChannelType getType() {
            return this.type;
        }
        public String getName() {
            return this.name;
        }
    }
    
    
	/* send an event object to an inbox name */
	boolean postEvent(final BrokerChannel channel, final Serializable event);
	
	/* send an event object list to an inbox name */
	boolean postEvents(final BrokerChannel channel, final List<Serializable> event);

	   /* idem overriding channel type */
    boolean postEvent(BrokerChannel channel, ChannelType type, Serializable event);    

	/* use a driver to handle events of a certain type */
	boolean subscribeEvent(
			final BrokerChannel channel, 
			final Class<? extends Serializable> eventType, 
			final Consumer<Serializable> driver,
			final long sinceTimestamp,
			final long retentionLapse);

	/* unregister the driver handling events of a certain type */
	boolean unsubscribeEvent(
			final BrokerChannel brokerChannel, 
			final Class<? extends Serializable> eventType, 
			final Consumer<Serializable> driver);
	
	/* emergency callback to know when the communication has been broken */
	void setBrokerShutdownCallback(final Runnable callback);
	
	default List<Consumer<?>> getRegisteredDrivers() {
		throw new UnsupportedOperationException();
	}
	
	default List<Class<?>> getRegisteredEvents() {
		throw new UnsupportedOperationException();
	}
	
}
