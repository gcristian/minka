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
package io.tilt.minka.core.leader.distributor;

import static io.tilt.minka.domain.ShardEntity.State.PREPARED;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.tilt.minka.api.Config;
import io.tilt.minka.api.Pallet;
import io.tilt.minka.core.leader.PartitionTable;
import io.tilt.minka.domain.EntityEvent;
import io.tilt.minka.domain.Shard;
import io.tilt.minka.domain.ShardEntity;
import io.tilt.minka.domain.ShardState;
import io.tilt.minka.utils.CircularCollection;

/**
 * Balances and distributes evenly all sharding duties into ONLINE shards.
 * Considering dangling and new unpartitioned duties
 * 
 * @author Cristian Gonzalez
 * @since Dec 13, 2015
 */
public class RoundRobinBalancer implements Balancer {

		private final Logger logger = LoggerFactory.getLogger(getClass());
		
		public RoundRobinBalancer(final Config config) {
			
		}
		
		/*
		 * TODO BUG: no olvidarme de evitar quitarle tareas a los q estan en
		 * cuarentena la inconsistencia es que si durante un tiempo prolongado se
		 * mantiene un server en cuarentena: promedio por los onlines, y ese nodo
		 * no reporto ninguna perdida
		 */
		public void balance(
				final Pallet<?> pallet,
				final PartitionTable table, 
				final Reallocation realloc, 
				final List<Shard> onlineShards,
				final Set<ShardEntity> creations, 
				final Set<ShardEntity> deletions, 
				int accounted) {

			// get a fair distribution
			accounted = table.getDutiesAllByShardState(pallet, ShardState.ONLINE).size();
			final double sum =  accounted + creations.size() - deletions.size(); // dangling.size() + 
			final int evenSize = (int) Math.ceil(sum / (double) onlineShards.size());
			
			logger.info( "{}: Even distribution for {} Shards: #{}  duties, for Creations: {}, Deletions: {}, Accounted: {} ",
						getClass().getSimpleName(), onlineShards.size(), evenSize, creations.size(), deletions.size(), accounted);

			// split shards into receptors and emisors while calculating new fair distribution 
			final Set<Shard> receptors = new HashSet<>();
			final Set<Shard> emisors = new HashSet<>();
			//deletions.addAll(dangling);
			final Map<Shard, Integer> deltas = checkDeltas(pallet, table, onlineShards, evenSize, receptors, emisors, deletions);
			if (deltas.isEmpty()) {
				logger.info("{}: Evenly distributed already (no sharding deltas out of threshold)",
							getClass().getSimpleName());
			} else if (!receptors.isEmpty()) {
				// 2nd step: assign migrations and creations in serie
				final CircularCollection<Shard> receptiveCircle = new CircularCollection<>(receptors);
				registerMigrations(pallet, table, realloc, emisors, deltas, receptiveCircle);
				Arranger.registerCreations(creations, realloc, receptiveCircle);
			} else {
				logger.warn("{}: There were no receptors collected to get issues", getClass().getSimpleName());
			}
		}

		/* move confirmed duties from/to shards with bigger/lesser duty size */
		private void registerMigrations(
				final Pallet<?>pallet,
				final PartitionTable table, 
				final Reallocation realloc, 
				final Set<Shard> emisors,
				final Map<Shard, Integer> deltas, 
				final CircularCollection<Shard> receptiveCircle) {

			for (final Shard emisorShard : emisors) {
				Set<ShardEntity> duties = table.getDutiesByShard(pallet, emisorShard);
				int i = 0;
				Iterator<ShardEntity> it = duties.iterator();
				while (it.hasNext() && i++ < Math.abs(deltas.get(emisorShard))) {
						final ShardEntity unassigning = it.next();
						unassigning.registerEvent(EntityEvent.DETACH, PREPARED);
						realloc.addChange(emisorShard, unassigning);
						final Shard receptorShard = receptiveCircle.next();
						ShardEntity assigning = ShardEntity.copy(unassigning);
						assigning.registerEvent(EntityEvent.ATTACH, PREPARED);
						realloc.addChange(receptorShard, assigning);
						logger.info("{}: Migrating from: {} to: {}, Duty: {}", getClass().getSimpleName(),
								emisorShard.getShardID(), receptorShard.getShardID(), assigning.toString());
				}
			}
		}

		/* evaluate which shards must emit or receive duties by deltas */
		private Map<Shard, Integer> checkDeltas(
				final Pallet<?> pallet,
				final PartitionTable table, 
				final List<Shard> onlineShards,
				final int fairDistribution, 
				final Set<Shard> receptors, 
				final Set<Shard> emisors,
				final Set<ShardEntity> deletions) {

			final Map<Shard, Integer> deltas = new HashMap<>();
			final int maxDelta = pallet.getBalancerEvenSizeMaxDutiesDeltaBetweenShards();
			for (final Shard shard : onlineShards) {
				final Set<ShardEntity> shardedDuties = table.getDutiesByShard(pallet, shard);
				// check if this shard contains the deleting duties 
				int sizeToRemove = (int) shardedDuties.stream().filter(i -> deletions.contains(i)).count();
				final int assignedDuties = shardedDuties.size() - sizeToRemove;
				final int delta = (assignedDuties - fairDistribution);
				if (delta >= maxDelta) {
						// use only the spilling out of maxDelta (not delta per-se)
						deltas.put(shard, delta);
						logger.info("{}: found Emisor Shard: {} above threshold: {} with {} ", getClass().getSimpleName(),
								shard.getShardID(), maxDelta, delta);
						emisors.add(shard);
				} else if (delta <= -(maxDelta)) {
						deltas.put(shard, delta);
						logger.info("{}: found Receptor Shard: {} below threshold: {} with {}", getClass().getSimpleName(),
								shard.getShardID(), maxDelta, delta);
						receptors.add(shard);
				}
			}
			return deltas;
		}


}
