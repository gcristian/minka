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

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.Validate;

import io.tilt.minka.domain.ShardEntity;

/**
 * Balanced Partition problem: Given a sorted list of Weights, creates a fair distribution of duties
 * 
 * Productive adaptation from:
 * {@linkplain http://sist.sysu.edu.cn/~isslxm/DSA/textbook/Skiena.-.TheAlgorithmDesignManual.pdf}
 * 
 * The algorighm does the best effort no matter the ordering
 * If Sorted, there will be cluttering of low weight duties in shards, and a few big ones in others
 * If Not sorted, -or purposely mixed-, there will be perfect distribution of weight and type
 * 
 * @author Cristian Gonzalez
 * @since Dec 29, 2015
 */
public class WeightBasedClusterizer implements Clusterizer {

		public List<List<ShardEntity>> split(final int shards, final List<ShardEntity> weightedDuties) {
			Validate.isTrue(shards > 1);
			Validate.noNullElements(weightedDuties);
			final int[] indexes = buildIndexes(weightedDuties, shards);
			final List<List<ShardEntity>> distro = new ArrayList<>();
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

		private List<ShardEntity> discoverFormedGroups(final List<ShardEntity> duties, int fromIdx, int idx) {
			final List<ShardEntity> group = new ArrayList<>();
			for (int i = fromIdx; i < idx; i++) {
				group.add(duties.get(i));
			}
			return group;
		}

		private int[] buildIndexes(final List<ShardEntity> sortedDuties, final int partitions) {
			final int size = sortedDuties.size();
			Validate.isTrue(partitions > 0 && size >= partitions);

			final int[] sum = new int[size];
			final int firstWeight = accessWeight(sortedDuties, 0);
			sum[0] = firstWeight;

			for (int i = 1; i < size; i++) {
				int weight = accessWeight(sortedDuties, i);
				sum[i] = sum[i - 1] + weight;
			}

			final int[][] partitionsByShards = new int[size + 1][partitions + 1];
			final int[][] results = new int[size + 1][partitions + 1];

			initializeMatrix(partitions, size, sum, firstWeight, partitionsByShards);
			build(partitions, size, sum, partitionsByShards, results);

			int[] dividers = new int[partitions - 1];
			for (int i = partitions, j = size; i > 1; i--) {
				j = dividers[i - 2] = results[j][i];
			}
			return dividers;
		}

		private int accessWeight(final List<ShardEntity> sortedDuties, final int i) {
			try {
				return sortedDuties.get(i).getDuty().getWeight().getLoad().intValue();
			} catch (Exception e) {
				throw new IllegalStateException("While trying to get weight for Duty: " + sortedDuties.get(i), e);
			}
		}

		private void initializeMatrix(final int partitions, final int size, final int[] sum, final int firstWeight,
				final int[][] partitionsByShards) {

			for (int t = 1; t <= size; t++) {
				partitionsByShards[t][1] = sum[t - 1];
			}
			for (int p = 1; p <= partitions; p++) {
				partitionsByShards[1][p] = firstWeight;
			}
		}

		private void build(final int partitions, final int size, final int[] sum, final int[][] partitionsByShards,
				final int[][] results) {

			for (int t = 2; t <= size; t++) {
				for (int p = 2; p <= partitions; p++) {
						partitionsByShards[t][p] = Integer.MAX_VALUE;
						for (int i = 1; i < t; i++) {
							final int largest = Math.max(partitionsByShards[i][p - 1], sum[t - 1] - sum[i - 1]);
							if (largest < partitionsByShards[t][p]) {
								partitionsByShards[t][p] = largest;
								results[t][p] = i;
							}
						}
				}
			}
		}

}
