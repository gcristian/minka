package io.tilt.minka;

import java.util.ArrayList;
import java.util.List;

import org.junit.Test;
import org.springframework.util.Assert;

import io.tilt.minka.api.DutyBuilder;
import io.tilt.minka.core.leader.balancer.EvenWeightBalancer;
import io.tilt.minka.api.Duty;
import io.tilt.minka.domain.ShardEntity;

/**
 * Balanced Partition problem: Given a sorted list of Weights, creates a fair distribution
 * Productive adaptation from:
 * @link http://sist.sysu.edu.cn/~isslxm/DSA/textbook/Skiena.-.TheAlgorithmDesignManual.pdf
 * 
 * @author Cristian Gonzalez
 * @since Dec 29, 2015
 */
public class PartitioneerTest {

	public static Duty<String> buildDutyWithWeight(long weight, String idi) {
		return Duty.<String>builder(idi, "1").with(weight).build();
	}

	@Test
	public void testBalance() {

		final int shards = 4;
		final List<Duty<?>> weightedDuties = new ArrayList<>();
		weightedDuties.add(ShardEntity.Builder.builder(buildDutyWithWeight(10l, "1")).build().getDuty());
		weightedDuties.add(ShardEntity.Builder.builder(buildDutyWithWeight(100l, "2")).build().getDuty());
		weightedDuties.add(ShardEntity.Builder.builder(buildDutyWithWeight(200l, "3")).build().getDuty());
		weightedDuties.add(ShardEntity.Builder.builder(buildDutyWithWeight(500l, "4")).build().getDuty());
		weightedDuties.add(ShardEntity.Builder.builder(buildDutyWithWeight(1000l, "5")).build().getDuty());
		weightedDuties.add(ShardEntity.Builder.builder(buildDutyWithWeight(1500l, "6")).build().getDuty());
		weightedDuties.add(ShardEntity.Builder.builder(buildDutyWithWeight(1500l, "7")).build().getDuty());

		final EvenWeightBalancer.WeightBasedClusterizer p = new EvenWeightBalancer.WeightBasedClusterizer();
		List<List<Duty<?>>> distro = p.split(shards, weightedDuties);
		Assert.isTrue(distro.size() == 4);
		assertDistribution(distro);

		List<List<Duty<?>>> distro2 = p.split(shards, weightedDuties);
		Assert.isTrue(distro2.size() == 4);
		assertDistribution(distro2);

		for (int i = 0; i < distro.size(); i++) {
			Assert.isTrue(distro.get(i).equals(distro2.get(i)),
					"2nd distro element " + distro2.get(i) + " isnt the same than previus:" + distro.get(i));
		}

	}

	private void assertDistribution(List<List<Duty<?>>> distro) {
		int i = 0;
		for (List<Duty<?>> group : distro) {
			int sum = 0;
			for (Duty<?> duty : group) {
				System.out.println("Group " + i + " with Duty: " + duty.getId() + " Weighting: "
						+ duty.getWeight());
				sum += duty.getWeight();
			}
			System.out.println("Total Weights (" + i + ") = " + sum);
			Assert.isTrue(i != 0 || (i == 0 && sum == 810));
			Assert.isTrue(i != 1 || (i == 1 && sum == 1000));
			Assert.isTrue(i != 2 || (i == 2 && sum == 1500));
			Assert.isTrue(i != 3 || (i == 3 && sum == 1500));
			i++;
		}
	}

}
