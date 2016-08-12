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
import io.tilt.minka.api.Pallet;
import io.tilt.minka.core.leader.PartitionTable;
import io.tilt.minka.domain.EntityEvent;
import io.tilt.minka.domain.Shard;
import io.tilt.minka.domain.ShardEntity;
import io.tilt.minka.domain.Workload;

/**
 * Balances and distributes duties by creating clusters using their processing weight
 * and assigning to Shards in order to have a perfectly balanced workload 
 * 
 * @author Cristian Gonzalez
 * @since Dec 13, 2015
 */
public class FairWorkloadBalancer implements Balancer {

		private final Logger logger = LoggerFactory.getLogger(getClass());


		public enum PreSortType {
			/**
			 * Dispose duties with perfect mix between all workload values
			 * in order to avoid having two duties of the same workload together
			 * like: 1,2,3,1,2,3,1,2,3 = perfect 
			 */
			DISPERSE,
			/**
			 * Use Creation date order, i.e. natural order.
			 * Use this to keep the migration of duties among shards: to a bare minimum.
			 * Duty workload weight is considered but natural order restricts the re-accomodation much more.
			 * Useful when the master list of duties has lot of changes in time, and low migration is required.
			 * Use this in case your Duties represent Tasks of a short lifecycle.
			 */
			DATE, 
			/**
			 * Use Workload order.
			 * Use this to maximize the clustering algorithm's effectiveness.
			 * In presence of frequent variation of workloads, duties will tend to migrate more. 
			 * Otherwise this's the most optimus strategy.
			 * Use this in case your Duties represent Data or Entities with a long lifecycle 
			 */
			WORKLOAD;
		}
		private final Clusterizer clusterizer;
		
		public FairWorkloadBalancer(final Config config, final Clusterizer partitioneer) {

			this.clusterizer = partitioneer;
		}

		@SuppressWarnings({ "unchecked", "rawtypes" })
		public final void balance(
				final Pallet<?> pallet, 
				final PartitionTable table, 
				final Reallocation realloc, 
				final List<Shard> onlineShards,
				final Set<ShardEntity> creations, 
				final Set<ShardEntity> deletions, 
				final int accounted) {

			// order new ones and current ones in order to get a fair distro 
			final PreSortType presort = pallet.getBalancerFairLoadPresort();
			final Comparator comparator = presort == PreSortType.WORKLOAD ? Workload.getComparator()
						: getShardDutyCreationDateComparator();

			final Set<ShardEntity> duties = new TreeSet<>(comparator);
			duties.addAll(creations); // newcomers have ++priority than table
			duties.addAll(table.getDutiesAllByShardState(pallet, ONLINE));
			final List<ShardEntity> dutiesSorted = new ArrayList<>(duties);
			logger.debug("{}: Before Balance: {} ({})", getClass().getSimpleName(), ShardEntity.toStringIds(dutiesSorted));

			final List<List<ShardEntity>> clusters = formClusters(onlineShards, duties, dutiesSorted);
			if (clusters.isEmpty()) {
					logger.error("{}: Cluster Partitioneer return empty distro !", getClass().getSimpleName());
					return;
			}
			final Iterator<Shard> itShard = onlineShards.iterator();
			final Iterator<List<ShardEntity>> itCluster = clusters.iterator();
			while (itShard.hasNext()) {
					final boolean moreClusters = itCluster.hasNext();
					final Shard shard = itShard.next();
					final Set<ShardEntity> currents = table.getDutiesByShard(pallet, shard);
					registerMigrationsForShard(realloc, moreClusters ? new TreeSet<>(itCluster.next()) : null, shard,
							currents);
			}
		}

		private Comparator<ShardEntity> getShardDutyCreationDateComparator() {
			return new Comparator<ShardEntity>() {
				@Override
				public int compare(ShardEntity o1, ShardEntity o2) {
						int i = o1.getEventDateForPartition(EntityEvent.CREATE)
									.compareTo(o2.getEventDateForPartition(EntityEvent.CREATE));
						return i==0? -1: i;
				}
			};
		}

		private List<List<ShardEntity>> formClusters(
				final List<Shard> onlineShards, 
				final Set<ShardEntity> duties,
				final List<ShardEntity> dutiesSorted) {

			List<List<ShardEntity>> clusters = null;
			// sort the shards by first time seen so duties are spread into a stable shard arrange
			//Collections.reverseOrder();
			Collections.sort(onlineShards, Collections.reverseOrder(onlineShards.get(0)));
			if (onlineShards.size() > 1 && dutiesSorted.size() >= onlineShards.size()) {
				clusters = clusterizer.split(onlineShards.size(), dutiesSorted);
				logDebug(clusters);
			} else if (onlineShards.size() == 1) {
				logger.warn("{}: Add Shards to benefit from the Fair Balancer ! Shards: {}, Duties: {}",
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

		private void registerMigrationsForShard(
				final Reallocation realloc, 
				final Set<ShardEntity> clusterSet,
				final Shard shard, 
				final Set<ShardEntity> currents) {

			List<ShardEntity> unassigning = clusterSet != null
						? currents.stream().filter(i -> !clusterSet.contains(i)).collect(Collectors.toList())
						: new ArrayList<>(currents);

			if (unassigning.isEmpty() && logger.isDebugEnabled()) {
				logger.debug("{}: Shard: {} has no UnAsignments (calculated are all already assigned)",
							getClass().getSimpleName(), shard);
			}

			StringBuilder log = new StringBuilder();
			for (ShardEntity unassign : unassigning) {
				// copy because in latter cycles this will be assigned
				// so they're traveling different places
				final ShardEntity copy = ShardEntity.copy(unassign);
				copy.registerEvent(EntityEvent.UNASSIGN, PREPARED);
				realloc.addChange(shard, copy);
				log.append(copy.getEntity().getId()).append(", ");
			}
			if (log.length() > 0) {
				logger.info("{}: DisAssigning to shard: {}, duties: {}", getClass().getSimpleName(), shard.getShardID(),
							log.toString());
			}

			if (clusterSet != null) {
				final List<ShardEntity> assigning = clusterSet.stream().filter(i -> !currents.contains(i))
							.collect(Collectors.toList());

				if (assigning.isEmpty() && logger.isDebugEnabled()) {
						logger.debug("{}: Shard: {} has no New Asignments (calculated are all already assigned)",
								getClass().getSimpleName(), shard);
				}
				log = new StringBuilder();
				for (ShardEntity assign : assigning) {
						// copy because in latter cycles this will be assigned
						// so they're traveling different places
						final ShardEntity copy = ShardEntity.copy(assign);
						copy.registerEvent(EntityEvent.ASSIGN, PREPARED);
						realloc.addChange(shard, copy);
						log.append(copy.getEntity().getId()).append(", ");
				}
				if (log.length() > 0) {
						logger.info("{}: Assigning to shard: {}, duty: {}", getClass().getSimpleName(), shard.getShardID(),
								log.toString());
				}
			}
		}

		private void logDebug(List<List<ShardEntity>> clusters) {
			if (logger.isDebugEnabled()) {
				final AtomicInteger ai = new AtomicInteger();
				for (final List<ShardEntity> cluster : clusters) {
						int n = ai.incrementAndGet();
						for (final ShardEntity sh : cluster) {
							logger.debug("{}: Cluster {} = {} ({})", getClass().getSimpleName(), n, sh.getEntity().getId(),
										sh.getDuty().getWeight().getLoad());
						}
				}
			}
		}

}
