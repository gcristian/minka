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
package io.tilt.minka.business.leader.distributor;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.SetMultimap;

import io.tilt.minka.api.Config;
import io.tilt.minka.api.Config.SpillBalancerStrategy;
import io.tilt.minka.business.leader.PartitionTable;
import io.tilt.minka.domain.DutyEvent;
import io.tilt.minka.domain.Shard;
import io.tilt.minka.domain.ShardDuty;
import io.tilt.minka.domain.Workload;
import io.tilt.minka.domain.ShardDuty.State;
import io.tilt.minka.domain.ShardDuty.StuckCause;
import io.tilt.minka.utils.CircularCollection;

/**
 * Balances and distributes duties by spilling from one shard to another.
 * Useful to save machines without loosing high availability
 * 
 * @author Cristian Gonzalez
 * @since Dec 13, 2015
 */
public class SpillOverBalancer extends AbstractBalancer {

    private final Logger logger = LoggerFactory.getLogger(getClass());
    
    
    public SpillOverBalancer(final Config config) {
        super(config);
    }
    
    protected final void balance(
            final PartitionTable table, 
            final Reallocation realloc, 
            final List<Shard> onlineShards,
            final Set<ShardDuty> dangling, 
            final Set<ShardDuty> creations, 
            final Set<ShardDuty> deletions,
            final int accounted) {
        
        final long maxValue = getConfig().getBalancerSpillOverMaxValue();
        if (maxValue <= 0) {
            final Shard receptorShard = getAnyReceptorShard(table, onlineShards);
            logger.info("{}: For Unbounded duties (max = 0) found Shard receptor: {}", 
                    getClass().getSimpleName(), receptorShard);
            creations.addAll(dangling);
            registerMigrationFromOthersToOne(table, realloc, receptorShard);
            registerCreations(creations, realloc, new CircularCollection<>(Arrays.asList(receptorShard)));
        } else {
            logger.info("{}: Computing Spilling strategy: {} with a Max Value: {}", 
                    getClass().getSimpleName(), getConfig().getBalancerSpillOverStrategy(), maxValue);
            boolean loadStrat = getConfig().getBalancerSpillOverStrategy() == SpillBalancerStrategy.WORKLOAD;
            final Map<Shard, AtomicLong> spaceByReceptor = new HashMap<>();
            final SetMultimap<Shard, ShardDuty> trans = collectTransceivers(table, loadStrat, spaceByReceptor, maxValue);
            if (spaceByReceptor.isEmpty() && !trans.isEmpty()) {
                logger.warn("{}: Couldnt find receptors to spill over to", getClass().getSimpleName());
            } else {
                logger.info("{}: Shard with space for allocating Duties: {}", 
                        getClass().getSimpleName(), spaceByReceptor);
                final List<ShardDuty> unfitting = new ArrayList<>(); 
                final List<ShardDuty> dutiesForBalance = new ArrayList<>();
                dutiesForBalance.addAll(creations); // priority for new comers
                dutiesForBalance.addAll(dangling);
                if (loadStrat) {
                    // so we can get the biggest accomodation of duties instead of the heaviest
                    Collections.sort(dutiesForBalance, Collections.reverseOrder(Workload.getComparator()));  
                }
                registerNewcomers(realloc, loadStrat, spaceByReceptor, unfitting, dutiesForBalance);
                // then move those surplussing
                registerMigrations(realloc, loadStrat, spaceByReceptor, unfitting, trans, maxValue);
                for (ShardDuty unfit: unfitting) {
                    logger.error("{}: Add Shards !! No receptor has space for Duty: {}", 
                            getClass().getSimpleName(), unfit);
                }
            }
        }
        
    }

    private void registerNewcomers(
            final Reallocation realloc, 
            final boolean loadStrat,
            final Map<Shard, AtomicLong> spaceByReceptor, 
            final List<ShardDuty> unfitting,
            final List<ShardDuty> dutiesForBalance) {
        
        for (final ShardDuty newcomer: dutiesForBalance) {
            final Shard receptor = makeSpaceIntoReceptors(loadStrat, newcomer, spaceByReceptor);
            if (receptor!=null) {
                newcomer.registerEvent(DutyEvent.ASSIGN, State.PREPARED);
                realloc.addChange(receptor, newcomer);
                logger.info("{}: Assigning to Shard: {} (space left: {}), Duty: {}", getClass().getSimpleName(), 
                        receptor.getShardID(), spaceByReceptor.get(receptor), newcomer.toBrief());
            } else {
                newcomer.registerEvent(DutyEvent.ASSIGN, State.STUCK);
                newcomer.setStuckCause(StuckCause.UNSUITABLE);
                realloc.getProblems().put(null, newcomer);
                unfitting.add(newcomer);
            }
        }
    }

    private void registerMigrations(
            final Reallocation realloc, 
            final boolean loadStrat,
            final Map<Shard, AtomicLong> spaceByReceptor,
            final List<ShardDuty> unfitting, 
            final SetMultimap<Shard, ShardDuty> trans, 
            final long maxValue) {
        
        for (final Shard emisor: trans.keys()) {
            final Set<ShardDuty> emitting = trans.get(emisor);
            for (final ShardDuty emitted: emitting) {
                final Shard receptor = makeSpaceIntoReceptors(loadStrat, emitted, spaceByReceptor);
                if (receptor==null) {
                    emitted.registerEvent(DutyEvent.ASSIGN, State.STUCK);
                    emitted.setStuckCause(StuckCause.UNSUITABLE);
                    realloc.getProblems().put(null, emitted);
                    unfitting.add(emitted);
                } else {
                    final ShardDuty copy = addMigrationChange(realloc, receptor, emisor, emitted);
                    logger.info("{}: Migrating from: {} to {}: Duty: {}", getClass().getSimpleName(), emisor, 
                            receptor, copy);
                }
            }
        }
    }

    private Shard makeSpaceIntoReceptors(
            final boolean loadStrat,
            final ShardDuty duty,
            final Map<Shard, AtomicLong> spaceByReceptor) {
        
        for (final Shard receptor: spaceByReceptor.keySet()) {
            AtomicLong space = spaceByReceptor.get(receptor);
            final long newWeight = loadStrat ? duty.getDuty().getWeight().getLoad() :1;
            if (space.get() - newWeight >= 0) {
                long surplus = space.addAndGet(-newWeight);
                // add the new weight to keep the map updated for latter emittings 
                if (logger.isDebugEnabled()) {
                    logger.debug("{}: Making space for: {} into Shard: {} still has: {}", 
                            getClass().getSimpleName(), duty, receptor, surplus);
                }
                return receptor;
            }
        }
        return null;
    }
    
    /* elect duties from emisors and compute receiving size at receptors */
    private SetMultimap<Shard, ShardDuty> collectTransceivers(
            final PartitionTable table,
            boolean loadStrat,
            final Map<Shard, AtomicLong> spaceByReceptor, 
            final long maxValue) {
        
        final SetMultimap<Shard, ShardDuty> transmitting = HashMultimap.create();
        for (final Shard shard: table.getAllImmutable()) {
            final Set<ShardDuty> dutiesByShard = table.getDutiesByShard(shard);
            final List<ShardDuty> checkingDuties = new ArrayList<>(dutiesByShard);
            if (loadStrat) {
                // order it so we can obtain sequentially granulated reductions of the transmitting shard
                Collections.sort(checkingDuties, Workload.getComparator() );
            }
            final long totalWeight = dutiesByShard.stream().mapToLong(i->i.getDuty().getWeight().getLoad()).sum();
            final boolean isEmisor = (!loadStrat && dutiesByShard.size() > maxValue) ||
                    (loadStrat && (totalWeight) > maxValue);
            final boolean isReceptor = (!loadStrat && dutiesByShard.size() < maxValue) || 
                    (loadStrat && totalWeight < maxValue);
            if (isEmisor) {
                if (!loadStrat) {
                    for (int i=0;i<dutiesByShard.size() - maxValue;i++) {
                        transmitting.put(shard, checkingDuties.get(i));
                    }
                } else if (loadStrat) {
                    long liftAccumulated = 0;
                    for (final ShardDuty takeMe: dutiesByShard) {
                        long load = takeMe.getDuty().getWeight().getLoad();
                        if ((liftAccumulated += load) <= totalWeight - maxValue) {
                            transmitting.put(shard, takeMe);
                        } else {
                            break;  // following are bigger because sorted
                        }
                    }
                }
            } else if (isReceptor) {
                // until loop ends we cannot for certain which shard is receptor and can receive which duty
                spaceByReceptor.put(shard, new AtomicLong(maxValue - (loadStrat ? totalWeight : dutiesByShard.size())));
            }
        }
        return transmitting;
    }
   
    private void registerMigrationFromOthersToOne(
            final PartitionTable table, 
            final Reallocation realloc,
            final Shard receptorShard) {
        
        for (final Shard shard: table.getAllImmutable()) {
            if (shard != receptorShard) {
                Set<ShardDuty> dutiesByShard = table.getDutiesByShard(shard);
                if (!dutiesByShard.isEmpty()) {
                    for (final ShardDuty duty: dutiesByShard) {
                        final ShardDuty copy = addMigrationChange(realloc, receptorShard, shard, duty);
                        logger.info("{}: Hoarding from Shard: {} to Shard: {}, Duty: {}", 
                                getClass().getSimpleName(), shard, receptorShard, copy);
                    }
                }
            }
        }
    }

    /* return new added */
    private ShardDuty addMigrationChange(
            final Reallocation realloc, 
            final Shard receptorShard, 
            final Shard emisorShard,
            final ShardDuty duty) {
        
        duty.registerEvent(DutyEvent.ASSIGN, State.PREPARED);
        realloc.addChange(receptorShard, duty);
        ShardDuty copy = ShardDuty.copy(duty);
        copy.registerEvent(DutyEvent.UNASSIGN, State.PREPARED);
        realloc.addChange(emisorShard, copy);
        return copy;
    }

    private Shard getAnyReceptorShard(final PartitionTable table, final List<Shard> onlineShards) {        
        for (Shard online: onlineShards) {
            Set<ShardDuty> dutiesByShard = table.getDutiesByShard(online);
            if (!dutiesByShard.isEmpty()) {
                return online;
            }
        }
        return onlineShards.iterator().next();
    }

}