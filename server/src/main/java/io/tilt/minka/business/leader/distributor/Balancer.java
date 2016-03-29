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
import io.tilt.minka.domain.DutyEvent;
import io.tilt.minka.domain.ShardDuty;

/**
 * Analyze the current {@linkplain PartitionTable} and if neccesary create a {@linkplain Reallocation}
 * registering {@linkplain ShardDuty} with a {@linkplain DutyEvent} and a State.
 * Representing migrations of duties, deletions, creations, dangling, etc.
 * 
 * Previous change only neccesary if not empty
 * 
 * @author Cristian Gonzalez
 * @since Jan 6, 2016
 *
 */
public interface Balancer {
    
    /**
     * @param table     a confirmed state of partitions
     * @return          a change to be made into partitions
     */
    Reallocation evaluate(PartitionTable table, Reallocation previousChange);
}