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

import static io.tilt.minka.domain.ShardEntity.State.PREPARED;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
	void balance(final Pallet<?> pallet, final PartitionTable table, final Reallocation realloc,
			final List<Shard> onlineShards, final Set<ShardEntity> creations, final Set<ShardEntity> deletions);
	
	public static class Migration {
		private final Logger logger = LoggerFactory.getLogger(getClass());
		private final Reallocation realloc;
		
		public Migration(Reallocation realloc) {
			super();
			this.realloc = realloc;
		}

		public final void add(final Shard source, final Shard target, final ShardEntity entity) {
			entity.registerEvent(EntityEvent.DETACH, PREPARED);
			realloc.addChange(source, entity);
			ShardEntity assigning = ShardEntity.copy(entity);
			assigning.registerEvent(EntityEvent.ATTACH, PREPARED);
			realloc.addChange(target, assigning);
			logger.info("{}: Migrating from: {} to: {}, Duty: {}", getClass().getSimpleName(),
					source.getShardID(), target.getShardID(), assigning.toString());
		}
	}
	
	
	/** So clients can add new balancers */
	public static class Directory {
		private static final Logger logger = LoggerFactory.getLogger(Balancer.class);
		private final static Map<Class<? extends Balancer>, Balancer> directory = new HashMap<>();
		static {
			try {
				for (Strategy strat: Strategy.values()) {
					directory.put(strat.getBalancer(), strat.getBalancer().newInstance());
				}
			} catch (InstantiationException | IllegalAccessException e) {
				logger.error("Unexpected factorying balancer's directory", e);
			}
		}
		/** @todo check other shards have this class in case of leadership reelection */
		public static void addCustomBalancer(final Balancer b) {
			Validate.notNull(b);
			if (directory.values().contains(b)) {
				throw new IllegalArgumentException("given balancer already exists: " + b.getClass().getName());
			}
			directory.put(b.getClass(), b);
		}
		public static Balancer getByStrategy(final Class<? extends Balancer>strategy) {
			return Directory.directory.get(strategy);
		}
		public static Collection<Balancer> getAll() {
			return Directory.directory.values();
		}
	}
	
	public static interface BalancerMetadata extends java.io.Serializable {
		Class<? extends Balancer> getBalancer();
	}
	
	public enum Strategy {

		/* equally sized shards: each one with same amount of entities or almost */
		ROUND_ROBIN(RoundRobinBalancer.class),
		/* equally loaded shards: duties clustering according weights*/
		EVEN_WEIGHT(EvenLoadBalancer.class),
		/* fairly loaded shards: duty-weight and shard-capacity trade-off distribution */
		//EVEN_CAPACITY(FairLoadBalancer.class),
		
		/* Unbalanced strategies related to distribution */
		
		/* keep minimum usage of shards: until spill then fill another one but keep frugal */
		SPILLOVER(SpillOverBalancer.class),
		/* keep agglutination of duties: move them together wherever they are */
		//COALESCE(CoalesceBalancer.class),
		/* random lazily spread distribution */
		//SCATTER(ShuffleBalancer.class),
		;
		Class<? extends Balancer> balancer;

		Strategy(Class<? extends Balancer> balancer) {
			this.balancer = balancer;
		}

		public Class<? extends Balancer> getBalancer() {
			return this.balancer;
		}
		
		public BalancerMetadata getBalancerInstance() throws InstantiationException, IllegalAccessException, ClassNotFoundException {
			return (BalancerMetadata) Class.forName(getBalancer().getName()+"$Metadata").newInstance();
		}
		

	}

}