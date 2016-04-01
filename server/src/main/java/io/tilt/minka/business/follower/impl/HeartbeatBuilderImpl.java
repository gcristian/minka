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
package io.tilt.minka.business.follower.impl;

import static io.tilt.minka.domain.ShardEntity.State.CONFIRMED;
import static io.tilt.minka.domain.ShardEntity.State.DANGLING;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.tilt.minka.api.Config;
import io.tilt.minka.api.Duty;
import io.tilt.minka.api.PartitionDelegate;
import io.tilt.minka.business.LeaderShardContainer;
import io.tilt.minka.business.follower.HeartbeatBuilder;
import io.tilt.minka.domain.EntityEvent;
import io.tilt.minka.domain.Heartbeat;
import io.tilt.minka.domain.Partition;
import io.tilt.minka.domain.ShardEntity;
import io.tilt.minka.utils.LogUtils;

/**
 * It keeps sending heartbeats to the leader as long as is alive
 * 
 * @author Cristian Gonzalez
 * @since Nov 17, 2015
 */
@SuppressWarnings("rawtypes")
public class HeartbeatBuilderImpl implements HeartbeatBuilder {

	private final Logger logger = LoggerFactory.getLogger(getClass());
	
    private final PartitionDelegate partitionDelegate;
	private final Partition partition;	
	private final AtomicLong sequence;
	private final Config config;
	
	public HeartbeatBuilderImpl(
	        final Config config,
	        final PartitionDelegate<?> partitionDelegate, 			
			final Partition partition, 
			final LeaderShardContainer leaderShardContainer) {
		
		super();
		this.config = config;
		this.partitionDelegate = partitionDelegate;
		this.partition  = partition;
		this.sequence = new AtomicLong();
	}
		
	@SuppressWarnings({ "unchecked" })
	public Heartbeat build() {
		Set<Duty<?>> reportedDuties;
		try {
			reportedDuties = partitionDelegate.reportTaken();
		} catch (Exception e) {
			logger.error("{}: ({}) PartitionDelegate failure", getClass().getSimpleName(), config.getResolvedShardId(), e);
			reportedDuties = new HashSet();
		}
		
		final List<ShardEntity> shardingDuties = new ArrayList<>();
		boolean warning = false;
		
		// add reported: as confirmed if previously assigned, dangling otherwise. 
		for (final Duty<?> duty: reportedDuties) {
		    ShardEntity shardedDuty = partition.forDuty(duty);
		    if (shardedDuty!=null) {
		        shardedDuty.registerEvent(CONFIRMED);
		    } else {
		        shardedDuty = ShardEntity.create(duty);
		        // shardedDuty.registerEvent(PartitionEvent.ASSIGN, State.DANGLING);
		        shardedDuty.registerEvent(EntityEvent.CREATE, DANGLING);
		        logger.error("{}: ({}) Reporting a Dangling Duty (by Addition): {}", getClass().getSimpleName(), 
		                partition.getId(), shardedDuty);
		        warning = true;
		    }
		    shardingDuties.add(shardedDuty);
		}
		
		// add non-reported: as dangling
		for (final ShardEntity existing: partition.getDuties()) {
		    if (!reportedDuties.contains(existing.getEntity())) {
		        existing.registerEvent(EntityEvent.DELETE, DANGLING);
		        logger.error("{}: ({}) Reporting a Dangling Duty (by Erasure): {}", getClass().getSimpleName(), 
		                partition.getId(),existing);
		        shardingDuties.add(existing);
		    }
		}
		
		final Heartbeat hb = Heartbeat.create(shardingDuties, warning, partition.getId(), sequence.getAndIncrement());
		if (logger.isDebugEnabled()) {
		    logDebugNicely(hb);
		} else {
		    logger.info("{}: ({}) {} SeqID: {}, Duties: {}", getClass().getSimpleName(), hb.getShardId(), 
		            LogUtils.HB_CHAR, hb.getSequenceId(), hb.getDuties().size());
		}
		return hb;
	}
		
    private void logDebugNicely(final Heartbeat hb) {
        final StringBuilder sb = new StringBuilder();
        List<ShardEntity> sorted = hb.getDuties();
        if (!sorted.isEmpty()) {
            sorted.sort(sorted.get(0));
        }
        
        long totalWeight = 0;
        for (ShardEntity i: hb.getDuties()) {
            sb.append(i.getDuty().getId())
                .append("(").append(i.getDuty().getWeight().getLoad()).append(")")
                .append(", ");
            totalWeight+=i.getDuty().getWeight().getLoad();
        }
        
        logger.debug("{}: ({}) {} SeqID: {}, Duties: {}, Weight: {} = [ {}] {}", getClass().getSimpleName(), 
                hb.getShardId(), LogUtils.HB_CHAR, hb.getSequenceId(), 
                hb.getDuties().size(), totalWeight, sb.toString(), hb.getDuties().isEmpty() ? "":"WITH CHANGE");
    }
}
