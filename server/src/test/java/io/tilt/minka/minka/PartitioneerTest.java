/**
 * Copyright (c) 2011-2015 Zauber S.A. -- All rights reserved
 */

package io.tilt.minka.minka;

import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

import io.tilt.minka.api.Duty;
import io.tilt.minka.business.leader.distributor.ClassicalPartitionSolver;
import io.tilt.minka.domain.ShardDuty;
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
        };
    }
        
    @Test
    public void testBalance() {
            
        final int shards = 4;
        final List<ShardDuty> weightedDuties = new ArrayList<>();
        weightedDuties.add(ShardDuty.create(buildDutyWithWeight(10l, "1")));
        weightedDuties.add(ShardDuty.create(buildDutyWithWeight(100l, "2")));
        weightedDuties.add(ShardDuty.create(buildDutyWithWeight(200l, "3")));
        weightedDuties.add(ShardDuty.create(buildDutyWithWeight(500l, "4")));
        weightedDuties.add(ShardDuty.create(buildDutyWithWeight(1000l, "5")));
        weightedDuties.add(ShardDuty.create(buildDutyWithWeight(1500l, "6")));
        weightedDuties.add(ShardDuty.create(buildDutyWithWeight(1500l, "7")));
        
        final ClassicalPartitionSolver p = new ClassicalPartitionSolver();
        List<List<ShardDuty>> distro = p.balance(shards, weightedDuties);
        printDistributionResult(distro);
    }

    private void printDistributionResult(List<List<ShardDuty>> distro) {
        int i =0;
        for (List<ShardDuty> group: distro) {
            int sum = 0;
            for (ShardDuty duty: group) {
                System.out.println("Group " + i + " with Duty: " + duty.getDuty().getId()+ " Weighting: " + 
                        duty.getDuty().getWeight().getLoad());
                sum+=duty.getDuty().getWeight().getLoad().intValue();
            }
            System.out.println("Total Weights (" + i + ") = " + sum);
            i++;
        }
    }
    
}
