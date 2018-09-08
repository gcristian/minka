package io.tilt.minka.core.leader.balancer;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.tilt.minka.api.Duty;

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
public class WeightBasedClusterizer {

	private final Logger logger = LoggerFactory.getLogger(WeightEqualizer.class);
	
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
	
	private void logDistributionResult(final List<List<Duty>> distro) {
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