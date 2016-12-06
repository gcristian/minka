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
package io.tilt.minka.business.leader;

import java.util.function.Consumer;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.tilt.minka.api.Config;
import io.tilt.minka.broker.EventBroker;
import io.tilt.minka.broker.EventBroker.Channel;
import io.tilt.minka.business.Coordinator;
import io.tilt.minka.business.Coordinator.PriorityLock;
import io.tilt.minka.business.Coordinator.SynchronizedFactory;
import io.tilt.minka.business.impl.ServiceImpl;
import io.tilt.minka.domain.Heartbeat;
import io.tilt.minka.domain.NetworkShardID;
import io.tilt.minka.domain.Shard;
import io.tilt.minka.domain.ShardState;

/**
 * Drives follower's events like {@linkplain Heartbeat} 
 * and maintains a {@linkplain PartitionTable} by defining a member's service state
 *  
 * @author Cristian Gonzalez
 * @since Dec 2, 2015
 */
public class FollowerConsumer extends ServiceImpl implements Consumer<Heartbeat> {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    private final Config config;
    private final PartitionTable partitionTable;
    private final Auditor auditor;
    private final EventBroker eventBroker;
    private final Coordinator coordinator;
    private final NetworkShardID shardId;
    
    /* 10 mins min: previous time-window lapse to look for events to rebuild Partition Table */
    private final long START_PAST_LAPSE_MS = 1000 * 60 * 10; 
    
    public FollowerConsumer(
        final Config config,
        final PartitionTable partitionTable,
        final Auditor accounter,
        final EventBroker eventBroker, 
        final Coordinator coordinator, 
        final NetworkShardID shardId) {
        
        this.config = config;
        this.partitionTable = partitionTable;
        this.auditor = accounter;
        this.eventBroker = eventBroker;
        this.coordinator = coordinator;
        this.shardId = shardId;
    }
    
	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Override
	public void start() {
		logger.info("{}: Starting. Scheduling constant shepherding check", getClass().getSimpleName());
		
		final long retentionLapse = Math.max(config.getQueuePartitionRetentionLapseMs(), START_PAST_LAPSE_MS);
		final long readQueueSince = System.currentTimeMillis();
		
		eventBroker.subscribeEvent(eventBroker.buildToTarget(config, Channel.HEARTBEATS_TO_LEADER, shardId), 
				Heartbeat.class, (Consumer)this, readQueueSince, retentionLapse);
				
	}
	
	@Override
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public void stop() {
		logger.info("{}: Stopping", getClass().getSimpleName());
		this.eventBroker.unsubscribeEvent(eventBroker.build(config, Channel.HEARTBEATS_TO_LEADER), 
				Heartbeat.class, (Consumer)this);
	}

	@Override
	public void accept(final Heartbeat hb) {
	    hb.setReception(new DateTime(DateTimeZone.UTC));
	    if (logger.isDebugEnabled()) {
	        logger.debug("{}: Receiving Heartbeat: {} delayed {}ms", 
		        getClass().getSimpleName(), hb.toString(), hb.getReceptionDelay());
	    }
	    
	    coordinator.run(SynchronizedFactory.build(Coordinator.Action.PARTITION_TABLE_UPDATE, 
	            PriorityLock.MEDIUM_BLOCKING, ()-> {
    		// when a shutdownlock acquired then keep receving HB to evaluate all Slaves are down!
    		Shard shard = partitionTable.getShard(hb.getShardId());
    		if (shard == null) {
    			// new member
    			partitionTable.addShard(shard = new Shard(eventBroker.buildToTarget(config, 
    			        Channel.INSTRUCTIONS_TO_FOLLOWER, hb.getShardId()), hb.getShardId()));
    		}
    		if (hb.getStateChange() == ShardState.QUITTED) {
    		    logger.info("{}: ShardID: {} went cleanly: {}", 
    		            getClass().getSimpleName(), shard, hb.getStateChange());
    		    partitionTable.getShard(shard.getShardID()).setState(ShardState.QUITTED);
    		}
    		auditor.account(hb, shard);
	    }));
	}

}