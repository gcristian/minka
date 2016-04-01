/**
 * Copyright (c) 2011-2015 Zauber S.A. -- All rights reserved
 */

package io.tilt.minka;

import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

import io.tilt.minka.api.Duty;
import io.tilt.minka.business.leader.distributor.ClassicalPartitionSolver;
import io.tilt.minka.domain.ShardEntity;
import io.tilt.minka.domain.Workload;

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
        return new Duty<String>() {
            private final String id=idi;
            @Override
            public int compareTo(String o) {
                return 1;
            }
            @Override
            public Class<String> getClassType() {
                return String.class;
            }
            @Override
            public String get() {
                return null;
            }
            @Override
            public Workload getWeight() {
                return new Workload(weight);
            }
            @Override
            public String getId() {
                return id;
            }
            @Override
            public int compare(String o1, String o2) {
                return o1.compareTo(o2);
            }
            @Override
            public String getPalletId() {
                return "p0";
            }
        };
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
        
        final ClassicalPartitionSolver p = new ClassicalPartitionSolver();
        List<List<ShardEntity>> distro = p.balance(shards, weightedDuties);
        printDistributionResult(distro);
    }

    private void printDistributionResult(List<List<ShardEntity>> distro) {
        int i =0;
        for (List<ShardEntity> group: distro) {
            int sum = 0;
            for (ShardEntity duty: group) {
                System.out.println("Group " + i + " with Duty: " + duty.getEntity().getId()+ " Weighting: " + 
                        duty.getDuty().getWeight().getLoad());
                sum+=duty.getDuty().getWeight().getLoad().intValue();
            }
            System.out.println("Total Weights (" + i + ") = " + sum);
            i++;
        }
    }
    
}
