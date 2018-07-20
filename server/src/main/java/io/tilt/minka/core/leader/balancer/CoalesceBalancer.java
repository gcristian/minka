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
package io.tilt.minka.core.leader.balancer;

import java.util.Map;
import java.util.Set;

import io.tilt.minka.api.Duty;
import io.tilt.minka.api.Pallet;
import io.tilt.minka.core.leader.distributor.Migrator;
import io.tilt.minka.domain.EntityEvent;

/**
 * Unbalanced strategy related to distribution.
 * Purpose: keep agglutination of duties: move them together wherever they are
 * 
 * Effect: a pallet living in one shard at a time, for synchronization matters.
 * If there's another pallet exhausting the same resource making both unfit in the same shard,
 * then duties from one of those pallets is going to migrate all-together 
 */
public class CoalesceBalancer implements Balancer {

	
	public static class Metadata implements BalancerMetadata {
		private static final long serialVersionUID = 8411425951224530387L;
		private final ShardPresort shardPresort;
		private final PreSort presort;
		private final Bound bound;
		
		@Override
		public Class<? extends Balancer> getBalancer() {
			return CoalesceBalancer.class;
		}
		public Metadata() {
			this.bound = Bound.CAPACITY_LIMITED;
			this.presort = PreSort.DATE;
			this.shardPresort = ShardPresort.BY_CREATION_DATE;
		}
		public Metadata(final Bound bound, final PreSort presort, final ShardPresort shardPresort) {
			super();
			this.bound = bound;
			this.presort = presort;
			this.shardPresort = shardPresort;
		}
		public PreSort getPresort() {
			return this.presort;
		}		
		public ShardPresort getShardPresort() {
			return this.shardPresort;
		}
		public Bound getBound() {
			return this.bound;
		}

		enum Bound {
			UNLIMITED,
			CAPACITY_LIMITED,
		}
	}
	
	@Override
	public void balance(
			final Pallet pallet,
			final Map<Spot, Set<Duty>> scheme,
			final Map<EntityEvent, Set<Duty>> stage,
			final Migrator migrator) {

	}

}
