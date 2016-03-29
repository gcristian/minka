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

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.tilt.minka.domain.ShardDuty;

/**
 * Generate cluster of Duties grouped by some strategical criteria 
 *  
 * @author Cristian Gonzalez
 * @since Dec 29, 2015
 */
public interface ClusterPartitioneer {

    final static Logger logger = LoggerFactory.getLogger(ClusterPartitioneer.class);
    
    /* you were supossed to bring balance to the force !! */
    List<List<ShardDuty>> balance(final int shards, final List<ShardDuty> weightedDuties);

    /* logger */
    default void logDistributionResult(List<List<ShardDuty>> distro) {
        int i =0;
        for (List<ShardDuty> group: distro) {
            for (ShardDuty duty: group) {
                if (logger.isDebugEnabled()) {
                    logger.debug("{}: Duty: {} Weighting: {} in Group: {} ", getClass().getSimpleName(), 
                        duty.getDuty().getId(), duty.getDuty().getWeight().getLoad(), i);
                }
            }
            i++;
        }
    }
}
