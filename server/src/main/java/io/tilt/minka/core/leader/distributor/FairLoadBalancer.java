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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.tilt.minka.api.Pallet;
import io.tilt.minka.core.leader.PartitionTable;
import io.tilt.minka.core.leader.distributor.Balancer.BalancerMetadata;
import io.tilt.minka.domain.Shard;
import io.tilt.minka.domain.ShardEntity;

/**
 * Result: fairly loaded shards: duty-weight and shard-capacity trade-off distribution
 * Balances and distributes duties by creating clusters using their processing weight
 * and assigning to Shards in order to have a perfectly balanced workload 
 * 
 * @author Cristian Gonzalez
 * @since Dec 13, 2015
 */
public class FairLoadBalancer implements Balancer {

	private final Logger logger = LoggerFactory.getLogger(getClass());

	@Override
	public void balance(Pallet<?> pallet, PartitionTable table, Reallocation realloc, List<Shard> onlineShards,
			Set<ShardEntity> creations, Set<ShardEntity> deletions, int accounted) {
		// TODO Auto-generated method stub

	}
	
	public static class FairLoadMetadata implements BalancerMetadata {

		private final Algorithm algo;
		@Override
		public Class<? extends Balancer> getBalancer() {
			return FairLoadBalancer.class;
		}
		public FairLoadMetadata(Algorithm algo) {
			super();
			this.algo = algo;
		}
		protected Algorithm getAlgorithm(Algorithm algo) {
			return this.algo;
		}
	}
	
	enum Algorithm {
		/* an even load filling so different capacity shards will reach
		 * their maximum load at the same time. This will make the bigger shards work more than smaller */
		EVEN,
		/* a serial filling so smaller shards will fast stop receiving 
		 * duties and bigger one will still receiving. This will */
		ROUND_ROBIN
	}

}
