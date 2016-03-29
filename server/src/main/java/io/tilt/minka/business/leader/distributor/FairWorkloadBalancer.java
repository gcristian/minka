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

import static io.tilt.minka.api.Config.FairBalancerPreSort.WORKLOAD;
import static io.tilt.minka.domain.ShardDuty.State.PREPARED;
import static io.tilt.minka.domain.ShardState.ONLINE;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.tilt.minka.api.Config;
import io.tilt.minka.api.Config.FairBalancerPreSort;
import io.tilt.minka.business.leader.PartitionTable;
import io.tilt.minka.domain.DutyEvent;
import io.tilt.minka.domain.Shard;
import io.tilt.minka.domain.ShardDuty;
import io.tilt.minka.domain.Workload;

/**
 * Balances and distributes duties by creating clusters using their processing weight
 * and assigning to Shards in order to have a perfectly balanced workload 
 * 
 * @author Cristian Gonzalez
 * @since Dec 13, 2015
 */
public class FairWorkloadBalancer extends AbstractBalancer {

    private final Logger logger = LoggerFactory.getLogger(getClass());
    
    private final ClusterPartitioneer partitioneer;
    
    public FairWorkloadBalancer(final Config config, final ClusterPartitioneer partitioneer) {
        super(config);
        this.partitioneer = partitioneer;
    }
    
    private Comparator<ShardDuty> getShardDutyCreationDateComparator() {
        return new Comparator<ShardDuty>() {
            @Override
            public int compare(ShardDuty o1, ShardDuty o2) {
                return o1.getEventDateForPartition(DutyEvent.CREATE).compareTo(
                        o2.getEventDateForPartition(DutyEvent.CREATE));
            }
        };
    }
    
    @SuppressWarnings({ "unchecked", "rawtypes" })
    protected final void balance(
            final PartitionTable table, 
            final Reallocation pres, 
            final List<Shard> onlineShards,
            final Set<ShardDuty> dangling, 
            final Set<ShardDuty> creations, 
            final Set<ShardDuty> deletions,
            final int accounted) {
        
        // add danglings as creations prior to migrationsw
        final List<ShardDuty> danglingAsCreations = new ArrayList<>();
        dangling.forEach(i->danglingAsCreations.add(ShardDuty.copy(i)));
        creations.addAll(danglingAsCreations);
        
        // order new ones and current ones in order to get a fair distro 
        final FairBalancerPreSort presort = getConfig().getBalancerFairLoadPresort();
        final Comparator comparator = presort == WORKLOAD ? Workload.getComparator() : getShardDutyCreationDateComparator();
        final Set<ShardDuty> duties = new TreeSet<>(comparator); 
        duties.addAll(creations);   // newcomers have ++priority than table
        duties.addAll(table.getDutiesAllByShardState(ONLINE));
        final List<ShardDuty> dutiesSorted = new ArrayList<>(duties);
        logBeforeBalance(dutiesSorted);
        
        final List<List<ShardDuty>> clusters = formClusters(onlineShards, duties, dutiesSorted);
        if (!clusters.isEmpty()) {
            final Iterator<Shard> itShard = onlineShards.iterator();
            final Iterator<List<ShardDuty>> itCluster = clusters.iterator();
            while (itShard.hasNext()) {
                final boolean moreClusters = itCluster.hasNext();
                final Shard shard = itShard.next();
                final Set<ShardDuty> currents = table.getDutiesByShard(shard);
                registerMigrationsForShard(pres, 
                        moreClusters ? new TreeSet<>(itCluster.next()):null, 
                        shard, currents);
            }
        } else {
            logger.error("{}: Cluster Partitioneer return empty distro !", getClass().getSimpleName());
        }
    }

    private List<List<ShardDuty>> formClusters(
            final List<Shard> onlineShards, 
            final Set<ShardDuty> duties,
            final List<ShardDuty> dutiesSorted) {
        
        List<List<ShardDuty>> clusters = null;
        // sort the shards by first time seen so duties are spread into a stable shard arrange
        //Collections.reverseOrder();
        Collections.sort(onlineShards, Collections.reverseOrder(onlineShards.get(0)));
        if (onlineShards.size() > 1 && dutiesSorted.size() >= onlineShards.size()) { 
            clusters = partitioneer.balance(onlineShards.size(), dutiesSorted);
            logDebug(clusters);
        } else if (onlineShards.size() == 1) {
            logger.warn("{}: Add Shards to benefit from the Fair Balancer ! Shards: {}, Duties: {}", 
                    getClass().getSimpleName(), onlineShards.size(), dutiesSorted.size());
            clusters = new ArrayList<>();
            clusters.add(dutiesSorted);
        } else if (dutiesSorted.size() < onlineShards.size()) {
            logger.warn("{}: Add Duties >= Shards to Load them and benefit from the Fair Balancer ! Shards: {}, Duties: {}", 
                    getClass().getSimpleName(), onlineShards.size(), dutiesSorted.size());
            // then simply prepare as many "clusters" as Duties, so they'll be assigned
            clusters = new ArrayList<>();
            for (final ShardDuty duty: duties) {
                clusters.add(Arrays.asList(duty));
            }
        }
        return clusters;
    }

    private void registerMigrationsForShard(
            final Reallocation realloc, 
            final Set<ShardDuty> clusterSet, 
            final Shard shard,
            final Set<ShardDuty> currents) {
        
        List<ShardDuty> unassigning = clusterSet !=null ? 
                    currents.stream().filter(i->!clusterSet.contains(i)).collect(Collectors.toList())
                    : new ArrayList<>(currents);
        
        if (unassigning.isEmpty() && logger.isDebugEnabled()) {
            logger.debug("{}: Shard: {} has no UnAsignments (calculated are all already assigned)", 
                    getClass().getSimpleName(), shard);
        }
        
        StringBuilder log = new StringBuilder();
        for (ShardDuty unassign: unassigning) {
            // copy because in latter cycles this will be assigned
            // so they're traveling different places
            final ShardDuty copy = ShardDuty.copy(unassign);
            copy.registerEvent(DutyEvent.UNASSIGN, PREPARED);
            realloc.addChange(shard, copy);
            log.append(copy.getDuty().getId()).append(", ");
        }
        if (log.length()>0) {
            logger.info("{}: DisAssigning to shard: {}, duties: {}", getClass().getSimpleName(), 
                shard.getShardID(), log.toString());
        }
        
        if (clusterSet != null) {
            final List<ShardDuty> assigning = clusterSet.stream()
                    .filter(i->!currents.contains(i)).collect(Collectors.toList());
            
            if (assigning.isEmpty() && logger.isDebugEnabled()) {
                logger.debug("{}: Shard: {} has no New Asignments (calculated are all already assigned)",
                        getClass().getSimpleName(), shard);
            }
            log = new StringBuilder();
            for (ShardDuty assign: assigning) {
                // copy because in latter cycles this will be assigned
                // so they're traveling different places
                final ShardDuty copy = ShardDuty.copy(assign);
                copy.registerEvent(DutyEvent.ASSIGN, PREPARED);
                realloc.addChange(shard, copy);
                log.append(copy.getDuty().getId()).append(", ");
            }
            if (log.length()>0) {
                logger.info("{}: Assigning to shard: {}, duty: {}", getClass().getSimpleName(), 
                        shard.getShardID(), log.toString());
            }
        }
    }
    
    private void logBeforeBalance(final List<ShardDuty> dutiesForBalance) {
        if (logger.isDebugEnabled()) {
            logger.debug("{}: Before Balance: {} ({})", getClass().getSimpleName(), 
                    ShardDuty.toStringIds(dutiesForBalance));
        }
    }

    private void logDebug(List<List<ShardDuty>> clusters) {
        if (logger.isDebugEnabled()) {
            final AtomicInteger ai = new AtomicInteger();
            for (final List<ShardDuty> cluster: clusters) {
                int n = ai.incrementAndGet();
                for (final ShardDuty sh: cluster) {
                    logger.debug("{}: Cluster {} = {} ({})", getClass().getSimpleName(),  n, sh.getDuty().getId(), 
                            sh.getDuty().getWeight().getLoad());
                }
            }
        }
    }

}
