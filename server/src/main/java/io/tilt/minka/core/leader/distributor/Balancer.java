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

import java.util.List;
import java.util.Set;

import io.tilt.minka.api.Pallet;
import io.tilt.minka.core.leader.PartitionTable;
import io.tilt.minka.domain.EntityEvent;
import io.tilt.minka.domain.Shard;
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


		/**
		 * Implement according these params:
		 * 
		 * @param table         the current situation 
		 * @param realloc          previous allocation to check for anything of interest
		 * @param onlineShards  the shards to distribute
		 * @param dangling      to treat as creations
		 * @param creations     new additions reported from source or added from partition service to distribute
		 * @param deletions     already registered: passed only for calculation
		 * @param accounted     summarization of already running and stable duties 
		 */
		void balance(
				final Pallet<?> pallet,
				final PartitionTable table, 
				final Reallocation realloc,
				final List<Shard> onlineShards, 
				final Set<ShardEntity> creations,
				final Set<ShardEntity> deletions,
				final int accounted);
		
		public enum BalanceAttributes {
			/**
			 * Keep at least one pallet of each kind
			 */
			NOE;
		}

		public enum BalanceStrategy {
			/* duties within the pallet will stick together wherever they fit*/
			NONE(null),
			/* classic circular assignation */
			ROUND_ROBIN(RoundRobinBalancer.class),
			/* try to fit in the best place, no migration at all */
			WEIGHTED_ROUND_ROBIN(null),
			/* all nodes same amount of entities */
			EVEN_SIZE(EvenSizeBalancer.class),
			/* duties clustering according weights */
			FAIR_LOAD(FairWorkloadBalancer.class),
			/* fill each node until spill then fill another node */
			SPILL_OVER(SpillOverBalancer.class),
			
			;
			
			Class<? extends Balancer> balancer;
			
			BalanceStrategy(Class<? extends Balancer> balancer) {
				this.balancer = balancer;
			}
			
			public Class<? extends Balancer> getBalancer() {
				return this.balancer;
			}
		
		}



		
}