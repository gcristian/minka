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

import java.util.Collection;
import java.util.Set;
import java.util.function.Predicate;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Sets;

import io.tilt.minka.api.Duty;
import io.tilt.minka.api.PartitionDelegate;
import io.tilt.minka.business.Coordinator;
import io.tilt.minka.business.LeaderShardContainer;
import io.tilt.minka.business.Coordinator.PriorityLock;
import io.tilt.minka.business.Coordinator.Synchronized;
import io.tilt.minka.business.Coordinator.SynchronizedFactory;
import io.tilt.minka.business.Semaphore.Action;
import io.tilt.minka.business.follower.PartitionManager;
import io.tilt.minka.domain.Partition;
import io.tilt.minka.domain.ShardCommand;
import io.tilt.minka.domain.ShardEntity;

public class PartitionManagerImpl implements PartitionManager {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    @SuppressWarnings("rawtypes")
    private final PartitionDelegate partitionDelegate;
    private final Partition partition;

    private final Coordinator coordinator;
    private final Synchronized releaser;
    
    public PartitionManagerImpl(
            final PartitionDelegate<?> partitionDelegate,       
            final Partition partition, 
            final Coordinator coordinator, 
            final LeaderShardContainer leaderShardContainer) {
        
        super();
        this.partitionDelegate = partitionDelegate;
        this.partition = partition;
        this.coordinator = coordinator;
        this.releaser = SynchronizedFactory.build(Action.INSTRUCT_DELEGATE, PriorityLock.MEDIUM_BLOCKING, ()->releaseAll());
    }
        
    public Void releaseAllCausePolicies() {
        //coordinator.tryBlocking(Semaphore.Action.INSTRUCT_DELEGATE, ()-> {
        coordinator.run(releaser);
        return null;
    }
    
    public Void releaseAll() {
        logger.info("{}: ({}) Instructing PartitionDelegate to RELEASE ALL", 
                getClass().getSimpleName(), partition.getId());
        unassign(partition.getDuties());
        partition.clean();
        return null;
    }
    
    // TODO refactory
    public Void finalized(final Collection<ShardEntity> duties) {
        for (ShardEntity duty: duties) {
            if (partition.getDuties().contains(duty)) {
                logger.info("{}: ({}) Removing finalized Duty from Partition: {}", 
                    getClass().getSimpleName(), partition.getId(), duty.toBrief());
                partition.getDuties().remove(duty);
            } else {
                logger.error("{}: ({}) Unable to ACKNOWLEDGE for finalization a never taken Duty !: {}", 
                        getClass().getSimpleName(), partition.getId(), duty.toBrief());
            }
        }
        return null;
    }

    @SuppressWarnings("unchecked")
    // TODO refactory
    public Void update(final Collection<ShardEntity> duties) {
        for (ShardEntity duty: duties) {
            if (partition.getDuties().contains(duty)) {
                if (duty.getUserPayload() == null) {
                    logger.info("{}: ({}) Instructing PartitionDelegate to UPDATE : {}", 
                            getClass().getSimpleName(), partition.getId(), duty.toBrief());
                    partitionDelegate.update(Sets.newHashSet(duty.getEntity()));
                } else {
                    logger.info("{}: ({}) Instructing PartitionDelegate to RECEIVE: {} with Payload type {}", 
                            getClass().getSimpleName(), partition.getId(), duty.toBrief(), 
                            duty.getUserPayload().getClass().getName());
                    partitionDelegate.receive(Sets.newHashSet(duty.getEntity()), duty.getUserPayload());
                }
            } else {
                logger.error("{}: ({}) Unable to UPDATE a never taken Duty !: {}", 
                        getClass().getSimpleName(), partition.getId(), duty.toBrief());
                // TODO todo mal reportar que no se puede tomar xq alguien la tiene q onda ???
            }
        }
        return null;
    }

    @SuppressWarnings("unchecked")
    public Void unassign(final Collection<ShardEntity> duties) {
        logger.info("{}: ({}) Instructing PartitionDelegate to RELEASE : {}", 
                getClass().getSimpleName(), partition.getId(), ShardEntity.toStringBrief(duties));
        partitionDelegate.release(toSet(duties, duty-> {
            if (!partition.getDuties().contains(duty)) {
                logger.error("{}: ({}) Unable to RELEASE a never taken Duty !: {}", 
                        getClass().getSimpleName(), partition.getId(), duty);
                return false;
            } else {
                return true;
            }
        }));
            /*
            coordinator.release(IdentifiedAction.build(RESERVE_DUTY, duty.getDuty().getId()));
            */
        return null;
    }
    
    @SuppressWarnings("unchecked")
    public Void assign(final Collection<ShardEntity> duties) {
        /*
        if (coordinator.acquire(IdentifiedAction.build(RESERVE_DUTY, duty.getDuty().getId())) == GRANTED) {
        */
            logger.info("{}: ({}) Instructing PartitionDelegate to TAKE: {}", 
                    getClass().getSimpleName(), partition.getId(), ShardEntity.toStringBrief(duties));
            partitionDelegate.take(toSet(duties, null));
            partition.getDuties().addAll(duties);
          /*  
        } else {
            logger.error("{}: ShardID: {}, Unable to TAKE an already Locked Duty !: {}", 
                    getClass().getSimpleName(), partition.getId(), duty.toBrief());
            // TODO todo mal reportar que no se puede tomar xq alguien la tiene q onda ???
        }
        */
            return null;
    }

    private Set<Duty<?>> toSet(final Collection<ShardEntity> duties, Predicate<ShardEntity> filter) {
        Set<Duty<?>> set = Sets.newHashSet();
        for (ShardEntity dudty: duties) {
            if (filter == null || filter.test(dudty)) {
                set.add(dudty.getDuty());
            }
        };
        return set;
    }

    public Void handleClusterOperation(final ShardCommand op) {
        return null;
        /*
        if (op.getOperation() == Command.CLUSTER_CLEAN_SHUTDOWN) {
            stop();
            final Heartbeat lastGoodbye = this.heartpump.buildHeartbeat();
            lastGoodbye.setStateChange(ServiceState.OFFLINE);
            eventBroker.postEvent(eventBroker.buildToTarget(config, Channel.HEARTBEATS_TO_LEADER, 
                    leaderShardContainer.getLeaderShardId()), lastGoodbye);
        }
        */
    }

}