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
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.annotation.JsonProperty;

import io.tilt.minka.api.Duty;
import io.tilt.minka.api.Pallet;
import io.tilt.minka.api.config.BalancerConfiguration;
import io.tilt.minka.core.leader.distributor.Migrator;
import io.tilt.minka.domain.EntityEvent;
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
			this.maxDutiesDeltaBetweenShards = BalancerConfiguration.EVEN_SIZE_MAX_DUTIES_DELTA_BETWEEN_SHARDS;
		}
		@JsonProperty
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
			final Pallet pallet,
			final Map<Spot, Set<Duty>> scheme,
			final Map<EntityEvent, Set<Duty>> stage,
			final Migrator migrator) {
		// get a fair distribution
		
		final Set<Duty> deletions = stage.get(EntityEvent.REMOVE);
		final Set<Duty> creations = stage.get(EntityEvent.CREATE);
		final AtomicInteger recount = new AtomicInteger();
		scheme.values().forEach(v->recount.addAndGet(v.size()));
		final double sum = recount.get() + creations.size() - deletions.size(); // dangling.size() +
		final int shardsSize = scheme.keySet().size();
		final int evenSize = (int) Math.ceil(sum / (double) shardsSize);

		logger.info("{}: Even distribution for {} Shards: #{}  duties, for Creations: {}, Deletions: {}, Accounted: {} ",
				getClass().getSimpleName(), shardsSize, evenSize, creations.size(), 
				deletions.size(), recount);

		// split shards into receptors and emisors while calculating new fair distribution 
		final Set<Spot> receptors = new HashSet<>(shardsSize);
		final Set<Spot> emisors = new HashSet<>(shardsSize);
		final Map<Spot, Integer> deltas = checkDeltas(pallet, scheme, evenSize, receptors, emisors, deletions);
		if (deltas.isEmpty()) {
			logger.info("{}: Evenly distributed already (no sharding deltas out of threshold)", getClass().getSimpleName());
		} else if (!receptors.isEmpty()) {
			// 2nd step: assign migrations and creations in serie
			final CollectionUtils.CircularCollection<Spot> receptiveCircle = CollectionUtils.circular(receptors);
			for (final Spot emisor : emisors) {
				final Set<Duty> duties = scheme.get(emisor);
				int i = 0;
				final Iterator<Duty> it = duties.iterator();
				while (it.hasNext() && i++ < Math.abs(deltas.get(emisor))) {
					final Duty d = it.next();
					if (!deletions.contains(d)) {
						migrator.transfer(emisor, receptiveCircle.next(), d);
					}
				}
			}
			// Q pasa cuando una Dangling viene aca, sigue en la tabla asignada a ese shard ?
			for (Duty duty: creations) {
				migrator.transfer(receptiveCircle.next(), duty);
			}
		} else {
			logger.warn("{}: There were no receptors collected to get issues", getClass().getSimpleName());
		}
	}

	/* evaluate which shards must emit or receive duties by deltas */
	private Map<Spot, Integer> checkDeltas(
			final Pallet pallet,
			final Map<Spot, Set<Duty>> scheme,
			final int evenSize, 
			final Set<Spot> receptors, 
			final Set<Spot> emisors, 
			final Set<Duty> deletions) {

		final Map<Spot, Integer> deltas = new HashMap<>(scheme.keySet().size());
		final int maxDelta = ((Metadata)pallet.getMetadata()).getMaxDutiesDeltaBetweenShards();
		for (final Spot shard : scheme.keySet()) {
			final Set<Duty> shardedDuties = scheme.get(shard);
			// check if this shard contains the deleting duties 
			int sizeToRemove = (int) shardedDuties.stream().filter(i -> deletions.contains(i)).count();
			final int assignedDuties = shardedDuties.size() - sizeToRemove;
			final int delta = (assignedDuties - evenSize);
			if (delta >= maxDelta) {
				// use only the spilling out of maxDelta (not delta per-se)
				deltas.put(shard, delta);
				logger.info("{}: found Emisor Shard: {} above threshold: {} with {} ", getClass().getSimpleName(),
						shard, maxDelta, delta);
				emisors.add(shard);
			} else if (delta <= -(maxDelta)) {
				deltas.put(shard, delta);
				logger.info("{}: found Receptor Shard: {} below threshold: {} with {}", getClass().getSimpleName(),
						shard, maxDelta, delta);
				receptors.add(shard);
			}
		}
		return deltas;
	}

}
