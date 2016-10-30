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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.tilt.minka.api.Config;
import io.tilt.minka.api.Pallet;
import io.tilt.minka.core.leader.PartitionTable;
import io.tilt.minka.domain.Shard;
import io.tilt.minka.domain.ShardEntity;

/**
 * Type balanced.
 * Purpose: perform an even spread considering duty weights, using a {@linkplain Presort}
 * to order duties based on their creation date or weight.
 * This ignores the shard's capacity, an even spread may result in an obliterating shard load.
 * Effect: equally loaded shards, and many migration when using Presort.DATE
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
			this.presort = Config.BalancerConf.EVEN_LOAD_PRESORT;
		}
		protected Balancer.PreSort getPresort() {
			return this.presort;
		}
		@Override
		public String toString() {
			return "EvenLoad-PreSortType: " + getPresort();
		}
	}
	
	private final Clusterizer clusterizer = new WeightBasedClusterizer();

	@SuppressWarnings({ "unchecked", "rawtypes" })
	public final void balance(final Pallet<?> pallet, final PartitionTable table, 
			final Reallocation realloc, final List<Shard> onlineShards, final Set<ShardEntity> creations, 
			final Set<ShardEntity> deletions) {

		// order new ones and current ones in order to get a fair distro 
		final Comparator comparator = ((Metadata)pallet.getStrategy()).getPresort().getComparator();
		final Set<ShardEntity> duties = new TreeSet<>(comparator);
		duties.addAll(creations); // newcomers have ++priority than table
		duties.addAll(table.getDutiesAllByShardState(pallet, ONLINE));
		duties.removeAll(deletions); // delete those marked for deletion
		final List<ShardEntity> dutiesSorted = new ArrayList<>(duties);
		logger.debug("{}: Before Balance: {} ({})", getClass().getSimpleName(), ShardEntity.toStringIds(dutiesSorted));

		final List<List<ShardEntity>> clusters = formClusters(onlineShards, duties, dutiesSorted);
		if (clusters.isEmpty()) {
			logger.error("{}: Cluster Partitioneer return empty distro !", getClass().getSimpleName());
			return;
		}
		
		final Migrator migra = new Migrator(table, realloc);
		final Iterator<Shard> itShard = onlineShards.iterator();
		final Iterator<List<ShardEntity>> itCluster = clusters.iterator();
		while (itShard.hasNext()) {
			migra.transfer(pallet, itShard.next(), itCluster.hasNext() ? new TreeSet<>(itCluster.next()) : null);
		}
		migra.execute();
	}


	private List<List<ShardEntity>> formClusters(final List<Shard> onlineShards, final Set<ShardEntity> duties,
			final List<ShardEntity> dutiesSorted) {

		List<List<ShardEntity>> clusters = null;
		// sort the shards by first time seen so duties are spread into a stable shard arrange
		//Collections.reverseOrder();
		Collections.sort(onlineShards, Collections.reverseOrder(onlineShards.get(0)));
		if (onlineShards.size() > 1 && dutiesSorted.size() >= onlineShards.size()) {
			clusters = clusterizer.split(onlineShards.size(), dutiesSorted);
			logDebug(clusters);
		} else if (onlineShards.size() == 1) {
			logger.warn("{}: Add Shards to benefit from this balancer ! Shards: {}, Duties: {}",
					getClass().getSimpleName(), onlineShards.size(), dutiesSorted.size());
			clusters = new ArrayList<>();
			clusters.add(dutiesSorted);
		} else if (dutiesSorted.size() < onlineShards.size()) {
			logger.warn(
					"{}: Add Duties >= Shards to Load them and benefit from the Fair Balancer ! Shards: {}, Duties: {}",
					getClass().getSimpleName(), onlineShards.size(), dutiesSorted.size());
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
