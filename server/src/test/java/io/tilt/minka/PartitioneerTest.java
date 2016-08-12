/**
 * Copyright (c) 2011-2015 Zauber S.A. -- All rights reserved
 */

package io.tilt.minka;

import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

import io.tilt.minka.api.Duty;
import io.tilt.minka.api.DutyBuilder;
import io.tilt.minka.core.leader.distributor.WeightBasedClusterizer;
import io.tilt.minka.domain.ShardEntity;

/**
 * Balanced Partition problem: Given a sorted list of Weights, creates a fair distribution
 * Productive adaptation from:
 * {@linkplain http://sist.sysu.edu.cn/~isslxm/DSA/textbook/Skiena.-.TheAlgorithmDesignManual.pdf}
 * 
 * @author Cristian Gonzalez
 * @since Dec 29, 2015
 */
public class PartitioneerTest {

		public static Duty<String> buildDutyWithWeight(long weight, String idi) {
			return DutyBuilder.build(String.class, idi, "p1", weight);
		}

		@Test
		public void testBalance() {

			final int shards = 4;
			final List<ShardEntity> weightedDuties = new ArrayList<>();
			weightedDuties.add(ShardEntity.create(buildDutyWithWeight(10l, "1")));
			weightedDuties.add(ShardEntity.create(buildDutyWithWeight(100l, "2")));
			weightedDuties.add(ShardEntity.create(buildDutyWithWeight(200l, "3")));
			weightedDuties.add(ShardEntity.create(buildDutyWithWeight(500l, "4")));
			weightedDuties.add(ShardEntity.create(buildDutyWithWeight(1000l, "5")));
			weightedDuties.add(ShardEntity.create(buildDutyWithWeight(1500l, "6")));
			weightedDuties.add(ShardEntity.create(buildDutyWithWeight(1500l, "7")));

			final WeightBasedClusterizer p = new WeightBasedClusterizer();
			List<List<ShardEntity>> distro = p.split(shards, weightedDuties);
			printDistributionResult(distro);
		}

		private void printDistributionResult(List<List<ShardEntity>> distro) {
			int i = 0;
			for (List<ShardEntity> group : distro) {
				int sum = 0;
				for (ShardEntity duty : group) {
						System.out.println("Group " + i + " with Duty: " + duty.getEntity().getId() + " Weighting: "
								+ duty.getDuty().getWeight().getLoad());
						sum += duty.getDuty().getWeight().getLoad().intValue();
				}
				System.out.println("Total Weights (" + i + ") = " + sum);
				i++;
			}
		}

}
