/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.tilt.minka.business.leader.distributor;

import io.tilt.minka.business.leader.PartitionTable;
import io.tilt.minka.domain.EntityEvent;
import io.tilt.minka.domain.ShardEntity;

/**
 * Analyze the current {@linkplain PartitionTable} and if neccesary create a {@linkplain Reallocation}
 * registering {@linkplain ShardEntity} with a {@linkplain EntityEvent} and a State.
 * Representing migrations of duties, deletions, creations, dangling, etc.
 * 
 * Previous change only neccesary if not empty
 * 
 * @author Cristian Gonzalez
 * @since Jan 6, 2016
 *
 */
public interface Balancer {
    
    public enum SpillBalancerStrategy {
        /**
         * Use the Max value to compare the sum of all running duties's weights 
         * and restrict new additions over a full shard 
         */ 
        WORKLOAD,
        /**
         * Use the Max value as max number of duties to fit in one shard 
         */
        SIZE
    }

    public enum BalanceStrategy {
        /* all nodes same amount of entities */
        EVEN_SIZE,
        /* duties clustering according weights */
        FAIR_LOAD,
        /* fill each node until spill then fill another node */
        SPILL_OVER;
    }
    
    public enum FairWorkloadBalancerLevel {
        
    }
    
    public enum FairWorkloadBalancerPreSort {
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

    /**
     * @param table     a confirmed state of partitions
     * @return          a change to be made into partitions
     */
    Reallocation evaluate(PartitionTable table, Reallocation previousChange);
}