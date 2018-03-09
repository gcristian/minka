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
package io.tilt.minka.core.leader.balancer;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.tilt.minka.api.Config;
import io.tilt.minka.api.Pallet;
import io.tilt.minka.core.leader.distributor.Balancer;
import io.tilt.minka.core.leader.distributor.Migrator;
import io.tilt.minka.domain.Shard;
import io.tilt.minka.domain.ShardEntity;
import io.tilt.minka.utils.CollectionUtils;

/**
 * Type balanced.
 * Purpose: a simple balancer using number of duties instead of weight. 
 * Effect: equally duty-sized shards, same amount of entities to all shards when possible.
 * 
 * @author Cristian Gonzalez
 * @since Dec 13, 2015
 */
public class EvenSizeBalancer implements Balancer {

	private final Logger logger = LoggerFactory.getLogger(getClass());

	public static class Metadata implements BalancerMetadata {
		private static final long serialVersionUID = -5997759590727184862L;
		private final int maxDutiesDeltaBetweenShards;
		@Override
		public Class<? extends Balancer> getBalancer() {
			return EvenSizeBalancer.class;
		}
		public Metadata(int maxDutiesDeltaBetweenShards) {
			super();
			this.maxDutiesDeltaBetweenShards = maxDutiesDeltaBetweenShards;
		}
		public Metadata() {
			super();
			this.maxDutiesDeltaBetweenShards = Config.BalancerConf.EVEN_SIZE_MAX_DUTIES_DELTA_BETWEEN_SHARDS;
		}
		protected int getMaxDutiesDeltaBetweenShards() {
			return this.maxDutiesDeltaBetweenShards;
		}
		@Override
		public String toString() {
			return "EvenSize-MaxDutiesDeltaBetweenShards: " + getMaxDutiesDeltaBetweenShards();
		}
	}
	

	
	/*
	 * TODO BUG: no olvidarme de evitar quitarle tareas a los q estan en
	 * cuarentena la inconsistencia es que si durante un tiempo prolongado se
	 * mantiene un server en cuarentena: promedio por los onlines, y ese nodo
	 * no reporto ninguna perdida
	 */
	public void balance(
			final Pallet<?> pallet,
			final Set<ShardEntity> stageDuties, 
			final Map<Shard, Set<ShardEntity>> stageDistro,
			final Set<ShardEntity> creations,
			final Set<ShardEntity> deletions,
			final Migrator migrator) {
		// get a fair distribution
		final int recount = stageDuties.size();
		final double sum = recount + creations.size() - deletions.size(); // dangling.size() +
		final int shardsSize = stageDistro.keySet().size();
		final int evenSize = (int) Math.ceil(sum / (double) shardsSize);

		logger.info("{}: Even distribution for {} Shards: #{}  duties, for Creations: {}, Deletions: {}, Accounted: {} ",
				getClass().getSimpleName(), shardsSize, evenSize, creations.size(), deletions.size(), recount);

		// split shards into receptors and emisors while calculating new fair distribution 
		final Set<Shard> receptors = new HashSet<>();
		final Set<Shard> emisors = new HashSet<>();
		//deletions.addAll(dangling);]
		final Map<Shard, Integer> deltas = checkDeltas(pallet, stageDuties, stageDistro, evenSize, receptors, emisors, deletions);
		if (deltas.isEmpty()) {
			logger.info("{}: Evenly distributed already (no sharding deltas out of threshold)", getClass().getSimpleName());
		} else if (!receptors.isEmpty()) {
			// 2nd step: assign migrations and creations in serie
			final CollectionUtils.CircularCollection<Shard> receptiveCircle = CollectionUtils.circular(receptors);
			for (final Shard emisorShard : emisors) {
				final Set<ShardEntity> duties = stageDistro.get(emisorShard);
				int i = 0;
				final Iterator<ShardEntity> it = duties.iterator();
				while (it.hasNext() && i++ < Math.abs(deltas.get(emisorShard))) {
					migrator.transfer(emisorShard, receptiveCircle.next(), it.next());
				}
			}
			// Q pasa cuando una Dangling viene aca, sigue en la tabla asignada a ese shard ?
			for (ShardEntity duty: creations) {
				migrator.transfer(receptiveCircle.next(), duty);
			}
		} else {
			logger.warn("{}: There were no receptors collected to get issues", getClass().getSimpleName());
		}
	}

	/* evaluate which shards must emit or receive duties by deltas */
	private Map<Shard, Integer> checkDeltas(
			final Pallet<?> pallet,
			final Set<ShardEntity> stageDuties, 
			final Map<Shard, Set<ShardEntity>> stageDistro,
			final int evenSize, 
			final Set<Shard> receptors, 
			final Set<Shard> emisors, 
			final Set<ShardEntity> deletions) {

		final Map<Shard, Integer> deltas = new HashMap<>();
		final int maxDelta = ((Metadata)pallet.getMetadata()).getMaxDutiesDeltaBetweenShards();
		for (final Shard shard : stageDistro.keySet()) {
			final Set<ShardEntity> shardedDuties = stageDistro.get(shard);
			// check if this shard contains the deleting duties 
			int sizeToRemove = (int) shardedDuties.stream().filter(i -> deletions.contains(i)).count();
			final int assignedDuties = shardedDuties.size() - sizeToRemove;
			final int delta = (assignedDuties - evenSize);
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
