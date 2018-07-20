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

import static io.tilt.minka.domain.ShardEntity.toDutyStringIds;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import org.apache.commons.lang.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.tilt.minka.api.Duty;
import io.tilt.minka.api.Pallet;
import io.tilt.minka.api.config.BalancerConfiguration;
import io.tilt.minka.core.leader.distributor.Migrator;
import io.tilt.minka.domain.EntityEvent;
import io.tilt.minka.shard.DateComparer;

/**
 * Type balanced.
 * Purpose: perform an even spread considering duty weights, using a Presort
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
		private final PreSort presort;
		@Override
		public Class<? extends Balancer> getBalancer() {
			return EvenWeightBalancer.class;
		}
		public Metadata(PreSort presort) {
			super();
			this.presort = presort;
		}
		public Metadata() {
			super();
			this.presort = BalancerConfiguration.EVEN_WEIGHT_PRESORT;
		}
		protected PreSort getPresort() {
			return this.presort;
		}
		@Override
		public String toString() {
			return "EvenWeight-PreSortType: " + getPresort();
		}
	}
	
	@SuppressWarnings({ "unchecked", "rawtypes" })
	public final void balance(
			final Pallet pallet,
			final Map<NetworkLocation, Set<Duty>> scheme,
			final Map<EntityEvent, Set<Duty>> stage,
			final Migrator migrator) {
		// order new ones and current ones in order to get a fair distro 
		final Comparator comparator = ((Metadata)pallet.getMetadata()).getPresort().getComparator();
		final Set<Duty> duties = new TreeSet<>(comparator);
		duties.addAll(stage.get(EntityEvent.CREATE)); // newcomers have ++priority than table
		scheme.values().forEach(duties::addAll);		
		duties.removeAll(stage.get(EntityEvent.REMOVE)); // delete those marked for deletion
		final List<Duty> dutiesSorted = new ArrayList<>(duties);
		logger.debug("{}: Before Balance: {} ({})", getClass().getSimpleName(), toDutyStringIds(dutiesSorted));
		final List<NetworkLocation> availShards = scheme.keySet().stream().filter(s->s.getCapacities()
				.get(pallet)!=null).collect(Collectors.toList());
		if (availShards.isEmpty()) {
			logger.error("{}: Still no shard reported capacity for pallet: {}!", getClass().getSimpleName(), pallet);
			return;
		}
		final List<List<Duty>> clusters = formClusters(availShards, duties, dutiesSorted);
		if (clusters.isEmpty()) {
			logger.error("{}: Cluster Partitioneer return empty distro !", getClass().getSimpleName());
			return;
		}
		final Iterator<List<Duty>> itCluster = clusters.iterator();
		final Iterator<NetworkLocation> itShards = availShards.iterator();
		while(itCluster.hasNext()) {
			final NetworkLocation loc = itShards.next();
			final List<Duty> selection = itCluster.next();
			final Bascule<NetworkLocation, Duty> bascule = new Bascule(loc, loc.getCapacities().get(pallet).getTotal());
			selection.forEach(d->bascule.tryLift(d, d.getWeight()));
			migrator.override(bascule.getOwner(), bascule.getCargo());
			if (!bascule.getDiscarded().isEmpty()) {
				logger.error("{}: Discarding duties since: {} on already full shard {}, with only capacity "
					+ "for {} out of selected {} ", getClass().getSimpleName(), bascule.getDiscarded(), loc, 
					bascule.getCargo().size(), selection.size());
			}
		}
	}


	private List<List<Duty>> formClusters(
			final List<NetworkLocation> availableShards, 
			final Set<Duty> duties,
			final List<Duty> dutiesSorted) {

		List<List<Duty>> clusters = null;
		// sort the shards by first time seen so duties are spread into a stable shard arrange
		//Collections.reverseOrder();
		Collections.sort(availableShards, Collections.reverseOrder(new DateComparer()));
		if (availableShards.size() > 1 && dutiesSorted.size() >= availableShards.size()) {
			clusters = new WeightBasedClusterizer().split(availableShards.size(), dutiesSorted);
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
			for (final Duty duty : duties) {
				clusters.add(Arrays.asList(duty));
			}
		}
		return clusters;
	}

	/**
	 * Balanced Partition problem: Given a sorted list of Weights, creates a fair distribution of duties
	 * 
	 * Productive adaptation from:
	 * http://sist.sysu.edu.cn/~isslxm/DSA/textbook/Skiena.-.TheAlgorithmDesignManual.pdf
	 * 
	 * The algorighm does the best effort no matter the ordering
	 * If Sorted, there will be cluttering of low weight duties in shards, and a few big ones in others
	 * If Not sorted, -or purposely mixed-, there will be perfect distribution of weight and type
	 * 
	 * @author Cristian Gonzalez
	 * @since Dec 29, 2015
	 */
	public static class WeightBasedClusterizer {

		private final Logger logger = LoggerFactory.getLogger(EvenWeightBalancer.class);
		
		public List<List<Duty>> split(final int shards, final List<Duty> weightedDuties) {
			Validate.isTrue(shards > 1);
			Validate.noNullElements(weightedDuties);
			final int[] indexes = buildIndexes(weightedDuties, shards);
			final List<List<Duty>> distro = new ArrayList<>();
			int fromIdx = 0;
			for (int idx : indexes) {
				distro.add(discoverFormedGroups(weightedDuties, fromIdx, idx));
				fromIdx = idx;
			}
			if (indexes[indexes.length - 1] < weightedDuties.size()) {
				distro.add(discoverFormedGroups(weightedDuties, fromIdx, weightedDuties.size()));
			}
			logDistributionResult(distro);
			return distro;
		}
		
		protected void logDistributionResult(final List<List<Duty>> distro) {
			int i = 0;
			for (List<Duty> group : distro) {
				for (Duty duty : group) {
					if (logger.isDebugEnabled()) {
						logger.debug("{}: Duty: {} Weighting: {} in Group: {} ", getClass().getSimpleName(),
								duty.getId(), duty.getWeight(), i);
					}
				}
				i++;
			}
		}

		private List<Duty> discoverFormedGroups(final List<Duty> duties, int fromIdx, int idx) {
			final List<Duty> group = new ArrayList<>();
			for (int i = fromIdx; i < idx; i++) {
				group.add(duties.get(i));
			}
			return group;
		}

		private int[] buildIndexes(final List<Duty> sortedDuties, final int partitions) {
			final int size = sortedDuties.size();
			Validate.isTrue(partitions > 0 && size >= partitions);

			final double[] sum = new double[size];
			final double firstWeight = accessWeight(sortedDuties, 0);
			sum[0] = firstWeight;

			for (int i = 1; i < size; i++) {
				double weight = accessWeight(sortedDuties, i);
				sum[i] = sum[i - 1] + weight;
			}

			final double[][] partitionsByShards = new double[size + 1][partitions + 1];
			final int[][] results = new int[size + 1][partitions + 1];

			initializeMatrix(partitions, size, sum, firstWeight, partitionsByShards);
			build(partitions, size, sum, partitionsByShards, results);

			int[] dividers = new int[partitions - 1];
			for (int i = partitions, j = size; i > 1; i--) {
				j = dividers[i - 2] = results[j][i];
			}
			return dividers;
		}

		private double accessWeight(final List<Duty> sortedDuties, final int i) {
			try {
				return sortedDuties.get(i).getWeight();
			} catch (Exception e) {
				throw new IllegalStateException("While trying to get weight for Duty: " + sortedDuties.get(i), e);
			}
		}

		private void initializeMatrix(
				final int partitions, 
				final int size, 
				final double[] sum, 
				final double firstWeight,
				final double[][] partitionsByShards) {

			for (int t = 1; t <= size; t++) {
				partitionsByShards[t][1] = sum[t - 1];
			}
			for (int p = 1; p <= partitions; p++) {
				partitionsByShards[1][p] = firstWeight;
			}
		}

		private void build(
				final int partitions, 
				final int size, 
				final double[] sum, 
				final double[][] partitionsByShards,
				final int[][] results) {

			for (int t = 2; t <= size; t++) {
				for (int p = 2; p <= partitions; p++) {
					partitionsByShards[t][p] = Integer.MAX_VALUE;
					for (int i = 1; i < t; i++) {
						final double largest = Math.max(partitionsByShards[i][p - 1], sum[t - 1] - sum[i - 1]);
						if (largest < partitionsByShards[t][p]) {
							partitionsByShards[t][p] = largest;
							results[t][p] = i;
						}
					}
				}
			}
		}
	}

	private void logDebug(final List<List<Duty>> clusters) {
		if (logger.isDebugEnabled()) {
			final AtomicInteger ai = new AtomicInteger();
			for (final List<Duty> cluster : clusters) {
				int n = ai.incrementAndGet();
				for (final Duty sh : cluster) {
					logger.debug("{}: Cluster {} = {} ({})", getClass().getSimpleName(), n, sh.getId(), sh.getWeight());
				}
			}
		}
	}

}
