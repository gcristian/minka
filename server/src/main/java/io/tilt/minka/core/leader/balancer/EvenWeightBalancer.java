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
package io.tilt.minka.core.leader.distributor.impl;

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
import io.tilt.minka.core.leader.distributor.Arranger.NextTable;
import io.tilt.minka.core.leader.distributor.Balancer;
import io.tilt.minka.core.leader.distributor.Migrator;
import io.tilt.minka.domain.Shard;
import io.tilt.minka.domain.ShardCapacity.Capacity;
import io.tilt.minka.domain.ShardEntity;

/**
 * Type balanced.
 * Purpose: perform an even spread considering duty weights, using a {@linkplain Presort}
 * to order duties based on their creation date or weight.
 * This ignores the shard's capacity, an even spread may result in an obliterating shard load.
 * Effect: equally loaded shards, and many migration when using Presort.DATE
 * Even weights at the cost of unfitting duties in full shards lacking of capacity  
 * 
 * @author Cristian Gonzalez
 * @since Dec 13, 2015
 */
public class EvenWeightBalancer implements Balancer {

	private final Logger logger = LoggerFactory.getLogger(getClass());

	
	public static class Metadata implements BalancerMetadata {
		public static final long serialVersionUID = -2274456002611675425L;
		private final Balancer.PreSort presort;
		@Override
		public Class<? extends Balancer> getBalancer() {
			return EvenWeightBalancer.class;
		}
		public Metadata(Balancer.PreSort presort) {
			super();
			this.presort = presort;
		}
		public Metadata() {
			super();
			this.presort = Config.BalancerConf.EVEN_WEIGHT_PRESORT;
		}
		protected Balancer.PreSort getPresort() {
			return this.presort;
		}
		@Override
		public String toString() {
			return "EvenWeight-PreSortType: " + getPresort();
		}
	}
	
	private final Clusterizer clusterizer = new WeightBasedClusterizer();

	@SuppressWarnings({ "unchecked", "rawtypes" })
	public final Migrator balance(final NextTable next) {
		// order new ones and current ones in order to get a fair distro 
		final Comparator comparator = ((Metadata)next.getPallet().getStrategy()).getPresort().getComparator();
		final Set<ShardEntity> duties = new TreeSet<>(comparator);
		duties.addAll(next.getCreations()); // newcomers have ++priority than table
		duties.addAll(next.getDuties());
		duties.removeAll(next.getDeletions()); // delete those marked for deletion
		final List<ShardEntity> dutiesSorted = new ArrayList<>(duties);
		logger.debug("{}: Before Balance: {} ({})", getClass().getSimpleName(), ShardEntity.toStringIds(dutiesSorted));
		final List<Shard> availShards = next.getIndex().keySet().stream().filter(s->s.getCapacities()
				.get(next.getPallet())!=null).collect(Collectors.toList());
		if (availShards.isEmpty()) {
			logger.error("{}: Still no shard reported capacity for pallet: {}!", getClass().getSimpleName(), next.getPallet());
			return null;
		}
		final List<List<ShardEntity>> clusters = formClusters(availShards, duties, dutiesSorted);
		if (clusters.isEmpty()) {
			logger.error("{}: Cluster Partitioneer return empty distro !", getClass().getSimpleName());
			return null;
		}
		final Iterator<List<ShardEntity>> itCluster = clusters.iterator();
		final Migrator migra = next.buildMigrator();
		for (final Shard shard: availShards) {
			final Set<ShardEntity> cluster = new TreeSet<>();
			final Capacity cap = shard.getCapacities().get(next.getPallet());
			if (itCluster.hasNext()) {
				double accum = 0;
				final List<ShardEntity> selection = itCluster.next();
				for (final ShardEntity duty: selection) {
					if ((accum+=duty.getDuty().getWeight()) > cap.getTotal()) {
						logger.error("{}: Discarding duties since: {} on already full shard {}, with only capacity "
							+ "for {} out of selected {} ", getClass().getSimpleName(), duty, shard, cluster.size(), 
							selection.size());
						break;
					} else {
						cluster.add(duty);
					}
				}
				if (!cluster.isEmpty()) {
					migra.override(shard, cluster);
				}
			}
		}
		return migra;
	}


	private List<List<ShardEntity>> formClusters(final List<Shard> availableShards, final Set<ShardEntity> duties,
			final List<ShardEntity> dutiesSorted) {

		List<List<ShardEntity>> clusters = null;
		// sort the shards by first time seen so duties are spread into a stable shard arrange
		//Collections.reverseOrder();
		Collections.sort(availableShards, Collections.reverseOrder(new Shard.DateComparer()));
		if (availableShards.size() > 1 && dutiesSorted.size() >= availableShards.size()) {
			clusters = clusterizer.split(availableShards.size(), dutiesSorted);
			logDebug(clusters);
		} else if (availableShards.size() == 1) {
			logger.warn("{}: Add Shards to benefit from this balancer ! Shards: {}, Duties: {}",
					getClass().getSimpleName(), availableShards.size(), dutiesSorted.size());
			clusters = new ArrayList<>();
			clusters.add(dutiesSorted);
		} else if (dutiesSorted.size() < availableShards.size()) {
			logger.warn("{}: Add Duties >= Shards to Load them and benefit from the Fair Balancer ! Shards: {}, Duties: {}",
					getClass().getSimpleName(), availableShards.size(), dutiesSorted.size());
			// then simply prepare as many "clusters" as Duties, so they'll be assigned
			clusters = new ArrayList<>();
			for (final ShardEntity duty : duties) {
				clusters.add(Arrays.asList(duty));
			}
		}
		return clusters;
	}

	

	private void logDebug(List<List<ShardEntity>> clusters) {
		if (logger.isDebugEnabled()) {
			final AtomicInteger ai = new AtomicInteger();
			for (final List<ShardEntity> cluster : clusters) {
				int n = ai.incrementAndGet();
				for (final ShardEntity sh : cluster) {
					logger.debug("{}: Cluster {} = {} ({})", getClass().getSimpleName(), n, sh.getEntity().getId(),
							sh.getDuty().getWeight());
				}
			}
		}
	}

}
