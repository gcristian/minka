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
package io.tilt.minka.business.follower;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

import io.tilt.minka.api.Config;
import io.tilt.minka.api.PartitionDelegate;
import io.tilt.minka.broker.EventBroker;
import io.tilt.minka.broker.EventBroker.Channel;
import io.tilt.minka.business.Coordinator;
import io.tilt.minka.business.LeaderShardContainer;
import io.tilt.minka.business.Service;
import io.tilt.minka.business.Coordinator.PriorityLock;
import io.tilt.minka.business.Coordinator.Synchronized;
import io.tilt.minka.business.Coordinator.SynchronizedFactory;
import io.tilt.minka.business.Semaphore.Action;
import io.tilt.minka.business.impl.ServiceImpl;
import io.tilt.minka.domain.Clearance;
import io.tilt.minka.domain.Partition;
import io.tilt.minka.domain.ShardCommand;
import io.tilt.minka.domain.ShardDuty;

@SuppressWarnings("rawtypes")
public class LeaderConsumer extends ServiceImpl implements Service, Consumer<Serializable> {

    private final Logger logger = LoggerFactory.getLogger(getClass());
    
    private final PartitionDelegate partitionDelegate;
    private final Partition partition;
    private final PartitionManager partitionManager;
    private final EventBroker eventBroker;
    private final Coordinator coordinator;
    private final Config config;
    private Clearance lastClearance;
    private final LeaderShardContainer leaderContainer;

    /* 10 mins min: previous time-window lapse to look for events from the Leader */
    private final long START_PAST_LAPSE_MS = 1000 * 60 * 10;
    
    public LeaderConsumer(
            final Config config,
            final PartitionDelegate partitionDelegate, 
            final Partition partition,
            final PartitionManager partitionManager, 
            final EventBroker eventBroker, 
            final Coordinator coordinator, 
            final LeaderShardContainer leaderContainer) {
        
        super();
        this.partitionDelegate = partitionDelegate;
        this.partition = partition;
        this.partitionManager = partitionManager;
        this.eventBroker = eventBroker;
        this.coordinator = coordinator;
        this.config = config;
        this.leaderContainer = leaderContainer;
    }

    @Override
    public void start() {
        this.partitionDelegate.activate();
        logger.info("{}: ({}) Preparing for leader events", getClass().getSimpleName(), config.getResolvedShardId());
        final long retentionLapse = Math.max(config.getQueueInboxRetentionLapseMs()*1000, START_PAST_LAPSE_MS);
        final long sinceNow = System.currentTimeMillis();
        eventBroker.subscribeEvent(eventBroker.buildToTarget(config, Channel.INSTRUCTIONS_TO_FOLLOWER, partition.getId()), 
                ShardDuty.class, this, sinceNow, retentionLapse);
        eventBroker.subscribeEvent(eventBroker.buildToTarget(config, Channel.INSTRUCTIONS_TO_FOLLOWER, partition.getId()), 
                Clearance.class, this, sinceNow, retentionLapse);
        eventBroker.subscribeEvent(eventBroker.buildToTarget(config, Channel.INSTRUCTIONS_TO_FOLLOWER, partition.getId()), 
                ArrayList.class, this, sinceNow, retentionLapse);
        
    }
    
    public Clearance getLastClearance() {
        return this.lastClearance;
    }
    
    @Override
    public void stop() {
        logger.info("{}: ({}) Stopping", getClass().getSimpleName(), config.getResolvedShardId());
        partitionManager.releaseAll();
        this.partitionDelegate.deactivate();
        eventBroker.unsubscribeEvent(
                eventBroker.buildToTarget(config, Channel.INSTRUCTIONS_TO_FOLLOWER, partition.getId()),
                Serializable.class, this);
    }
    
    @Override
    @SuppressWarnings("unchecked")
    public void accept(final Serializable event) {
        if (event instanceof ShardCommand) {
            logger.info("{}: ({}) Receiving: {}", getClass().getSimpleName(), config.getResolvedShardId(), event);
            partitionManager.handleClusterOperation((ShardCommand)event);
        } else if (event instanceof ShardDuty) {
            logger.info("{}: ({}) Receiving: {}", getClass().getSimpleName(), config.getResolvedShardId(), event);
            Synchronized handler = SynchronizedFactory.build(
                    Action.INSTRUCT_DELEGATE, PriorityLock.MEDIUM_BLOCKING,  ()->handleDuty((ShardDuty)event));
            coordinator.run(handler);
        } else if (event instanceof ArrayList) {
            logger.info("{}: ({}) Receiving: {}", getClass().getSimpleName(), config.getResolvedShardId(), event);
            final List<ShardDuty> list = (ArrayList<ShardDuty>)event;
            final Synchronized handler = SynchronizedFactory.build(Action.INSTRUCT_DELEGATE, 
                    PriorityLock.MEDIUM_BLOCKING, ()-> {
                if (list.stream().collect(Collectors.groupingBy(ShardDuty::getDutyEvent)).size()>1) {
                    list.forEach(d->handleDuty(d));
                } else {
                    handleDuty(list.toArray(new ShardDuty[list.size()]));
                }
            });
            coordinator.run(handler);
        } else if (event instanceof Clearance) {            
            final Clearance clear = ((Clearance)event);
            if (clear.getLeaderShardId().equals(leaderContainer.getLeaderShardId())) {
                logger.info("{}: ({}) Accepting clearance from: {} (id:{})", getClass().getSimpleName(), 
                        config.getResolvedShardId(), clear.getLeaderShardId(), clear.getSequenceId());
                this.lastClearance = (Clearance)event;
            } else if (clear.getLeaderShardId().equals(leaderContainer.getPreviousLeaderShardId())) {
                logger.info("{}: ({}) Ignoring remaining clearance from previous leader: {} (current is: {})", 
                        getClass().getSimpleName(), config.getResolvedShardId(), clear.getLeaderShardId(), 
                        leaderContainer.getLeaderShardId());
            } else {
                logger.warn("{}: ({}) Ignoring clearance from unacknowledged leader: {} (my container says: {})", 
                        getClass().getSimpleName(), config.getResolvedShardId(), clear.getLeaderShardId(), 
                        leaderContainer.getLeaderShardId());
            }
        } else {
            logger.error("{}: ({}) Unknown event!: " + event.getClass().getSimpleName(), config.getResolvedShardId());
        }
    }

    private void handleDuty(final ShardDuty ...duties) {
        try {
            switch (duties[0].getDutyEvent()) {
                case ASSIGN:
                    partitionManager.assign(Lists.newArrayList(duties));
                    break;
                case UNASSIGN:
                    partitionManager.unassign(Lists.newArrayList(duties));
                    for (ShardDuty duty: duties) {
                        partition.getDuties().remove(duty);
                    }
                    break;
                case UPDATE:
                    partitionManager.update(Lists.newArrayList(duties));
                    break;
                case FINALIZED:
                    partitionManager.finalized(Lists.newArrayList(duties));
                    break;
                default:
                    logger.error("{}: ({}) Not allowed: {}", duties[0].getDutyEvent(), config.getResolvedShardId());
            };
        } catch (Exception e) {
            logger.error("{}: ({}) Unexpected while handling Duty:{}", getClass().getSimpleName(), 
                    config.getResolvedShardId(), duties, e);
        }
    }
    
    public PartitionManager getPartitionManager() {
        return this.partitionManager;
    }
    
}
