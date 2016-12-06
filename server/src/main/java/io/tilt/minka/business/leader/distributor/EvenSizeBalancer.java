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

import static io.tilt.minka.domain.ShardDuty.State.PREPARED;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.tilt.minka.api.Config;
import io.tilt.minka.business.leader.PartitionTable;
import io.tilt.minka.domain.DutyEvent;
import io.tilt.minka.domain.Shard;
import io.tilt.minka.domain.ShardDuty;
import io.tilt.minka.utils.CircularCollection;

/**
 * Balances and distributes evenly all sharding duties into ONLINE shards.
 * Considering dangling and new unpartitioned duties
 * 
 * @author Cristian Gonzalez
 * @since Dec 13, 2015
 */
public class EvenSizeBalancer extends AbstractBalancer {

    private final Logger logger = LoggerFactory.getLogger(getClass());
    
    public EvenSizeBalancer(final Config config) {
        super(config);
    }
    
    /* TODO BUG: no olvidarme de evitar quitarle tareas a los q estan en cuarentena
     * la inconsistencia es que si durante un tiempo prolongado se mantiene un server
     * en cuarentena: promedio por los onlines, y ese nodo no reporto ninguna perdida 
     */
    protected final void balance(
            final PartitionTable table, 
            final Reallocation pres, 
            final List<Shard> onlineShards,
            final Set<ShardDuty> dangling, 
            final Set<ShardDuty> creations, 
            final Set<ShardDuty> deletions,
            final int accounted) {
        
        // get a fair distribution
        final double sum = accounted + dangling.size() + creations.size() - deletions.size();
        final int evenSize = (int)Math.ceil(sum / (double)onlineShards.size());;
        logger.info("{}: Even distribution for {} Shards: #{}  duties, for Creations: {}, Deletions: {}, "
                + "Dangling:{}, Accounted: {} ", getClass().getSimpleName(), onlineShards.size(), evenSize,  
                creations.size(), deletions.size(), dangling.size(), accounted);
        
        // split shards into receptors and emisors while calculating new fair distribution 
        final Set<Shard> receptors = new HashSet<>();
        final Set<Shard> emisors = new HashSet<>();
        deletions.addAll(dangling);
        final Map<Shard, Integer> deltas = checkDeltas(table, onlineShards, evenSize, receptors, emisors, deletions);
        if (deltas.isEmpty()) {
            logger.info("{}: Evenly distributed already (no sharding deltas out of threshold)", 
                    getClass().getSimpleName());
        } else if (!receptors.isEmpty()){
            // 2nd step: assign migrations and creations in serie
            final CircularCollection<Shard> receptiveCircle = new CircularCollection<>(receptors);
            registerMigrations(table, pres, emisors, deltas, receptiveCircle);
            // copy them because they both go to different shards with different events
            final List<ShardDuty> danglingAsCreations = new ArrayList<>();
            dangling.forEach(i->danglingAsCreations.add(ShardDuty.copy(i)));
            creations.addAll(danglingAsCreations);
            registerCreations(creations, pres, receptiveCircle);
        } else {
            logger.warn("{}: There were no receptors collected to get issues", getClass().getSimpleName());
        }
    }

    
    /* move confirmed duties from/to shards with bigger/lesser duty size  */
    private void registerMigrations(
            final PartitionTable table, 
            final Reallocation pres, 
            final Set<Shard> emisors, 
            final Map<Shard, Integer> deltas,
            CircularCollection<Shard> receptiveCircle) {
        
        for (final Shard emisorShard: emisors) {
            Set<ShardDuty> duties = table.getDutiesByShard(emisorShard);
            int i = 0;
            Iterator<ShardDuty> it  = duties.iterator();
            while (it.hasNext() && i++ < Math.abs(deltas.get(emisorShard))) {
                final ShardDuty unassigning = it.next();
                unassigning.registerEvent(DutyEvent.UNASSIGN, PREPARED);
                pres.addChange(emisorShard, unassigning);
                final Shard receptorShard = receptiveCircle.next();
                ShardDuty assigning = ShardDuty.copy(unassigning);
                assigning.registerEvent(DutyEvent.ASSIGN, PREPARED);
                pres.addChange(receptorShard, assigning);
                logger.info("{}: Migrating from: {} to: {}, Duty: {}", getClass().getSimpleName(), 
                        emisorShard.getShardID(), receptorShard.getShardID(), assigning.toString());
            }
        }
    }

    /* evaluate which shards must emit or receive duties by deltas */
    private Map<Shard, Integer> checkDeltas(
            final PartitionTable table, 
            final List<Shard> onlineShards,
            final int fairDistribution, 
            final Set<Shard> receptors, 
            final Set<Shard> emisors, 
            final Set<ShardDuty> deletions) {
        
        final Map<Shard, Integer> deltas = new HashMap<>();
        final int maxDelta = getConfig().getBalancerEvenSizeMaxDutiesDeltaBetweenShards();
        for (final Shard shard: onlineShards) {
            final Set<ShardDuty> shardedDuties = table.getDutiesByShard(shard);
            // check if this shard contains the deleting duties 
            int sizeToRemove = (int)shardedDuties.stream().filter(i->deletions.contains(i)).count();
            final int assignedDuties = shardedDuties.size() - sizeToRemove;
            final int delta = (assignedDuties - fairDistribution);
            if (delta >= maxDelta) {
                // use only the spilling out of maxDelta (not delta per-se)
                deltas.put(shard, delta); 
                logger.info("{}: new Shard Emisor: {} is above threshold: {} with {} ", 
                        getClass().getSimpleName(), shard.getShardID(), maxDelta, delta);
                emisors.add(shard);
            } else if (delta <= -(maxDelta)) {
                deltas.put(shard, delta);
                logger.info("{}: new Shard Receptor: {} is below threshold: {} with {}", 
                        getClass().getSimpleName(), shard.getShardID(), maxDelta, delta);
                receptors.add(shard);
            }
        }
        return deltas;
    }

}