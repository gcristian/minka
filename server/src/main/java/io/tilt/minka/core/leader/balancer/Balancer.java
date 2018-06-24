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

import java.time.Instant;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.annotation.JsonIgnore;

import io.tilt.minka.api.Duty;
import io.tilt.minka.api.Pallet;
import io.tilt.minka.core.leader.distributor.ChangePlan;
import io.tilt.minka.core.leader.distributor.Migrator;
import io.tilt.minka.domain.Capacity;
import io.tilt.minka.domain.EntityEvent;
import io.tilt.minka.domain.NetworkShardIdentifier;
import io.tilt.minka.domain.Shard;
import io.tilt.minka.domain.ShardEntity;

/**
 * Analyze the current distribution of {@linkplain Duty}'s and propose changes.
 * Changes are made thru {@linkplain Migrator}, they're optional but must be consistent.
 * Identical i.e. repeatable changes wont produce a new {@linkplain ChangePlan}.
 *
 * @author Cristian Gonzalez
 * @since Jan 6, 2016
 */
public interface Balancer {

	static final Logger logger = LoggerFactory.getLogger(Balancer.class);

	/**
	 * Analyze current distribution and apply overrides or transfers on a {@linkplain Migrator}
	 * The algorithm must keep an idempotent behaviour for predictable cluster distribution, and so stability. 
	 * Try to attach as much duties as possible, dont overwhelm: operation is cancelled.
	 * 
	 * Creations and duties (table's current assigned duties) must be balanced.
	 * Note deletions are included in duties but must be ignored and removed from balancing,
	 * i.e. they must not be included in overrides and transfers.
	 * 
	 * @param scheme		map repr. the partition table's duties assigned to shards
	 * 						all shards included, with or without attached duties.
	 * @param stage   	map repr. set of CRUD duties that must be distibuted and doesnt exist in table object
	 * 						including also recycled duties like danglings and missings which are not new
	 * 						and a set of deletions that will cease to exist in the table (already marked)
	 * @param migrator		a facility to request modifications for duty assignation for the next distribution		
	 */
	void balance(final Pallet<?> pallet,
			final Map<NetworkLocation, Set<Duty<?>>> scheme,
			final Map<EntityEvent, Set<Duty<?>>> stage,
			final Migrator migrator);
	
	/** safety read-only Shard's decorator for balancers to use */
	public static class NetworkLocation implements Comparator<NetworkLocation>, Comparable<NetworkLocation> {
		private final Shard shard;
		public NetworkLocation(final Shard shard) {
			this.shard = shard;
		}
		public Map<Pallet<?>, Capacity> getCapacities() {
			return this.shard.getCapacities();
		}
		public NetworkShardIdentifier getId() {
			return this.shard.getShardID();
		}
		public Instant getCreation() {
			return this.shard.getFirstTimeSeen();
		}
		@java.lang.Override
		public String toString() {
			return this.shard.toString();
		}
		/**
		 * To be used by user's custom balancers for location/server reference. 
		 * @return the tag set with {@linkplain Minka} on setLocationTag(..) 
		 */
		public String getTag() {
		    return this.shard.getShardID().getTag();
		}
		private Shard getShard() {
			return shard;
		}
		@java.lang.Override
		public int compare(final NetworkLocation o1, final NetworkLocation o2) {
			return shard.compare(o1.getShard(), o2.shard);
		}
		@java.lang.Override
		public int compareTo(final NetworkLocation o) {
			return shard.compareTo(o.getShard());
		}
		@java.lang.Override
		public int hashCode() {
			return shard.hashCode();
		}
		@java.lang.Override
		public boolean equals(Object obj) {
			if (obj==null || !(obj instanceof NetworkLocation)) {
				return false;
			} else if (obj==this) {
				return true;
			} else {
				return shard.equals(((NetworkLocation)obj).getShard());
			}
		}
	}
		
	/** so clients can add new balancers */
	public static class Directory {
		private final static Map<Class<? extends Balancer>, Balancer> directory = new HashMap<>(Strategy.values().length);
		static {
			try {
				for (Strategy strat: Strategy.values()) {
					directory.put(strat.getBalancer(), strat.getBalancer().newInstance());
				}
			} catch (InstantiationException | IllegalAccessException e) {
				logger.error("Unexpected factorying balancer's directory", e);
			}
		}
		/* @todo check other shards have this class in case of leadership reelection */
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
	
	/* some balancers need configuration */
	public static interface BalancerMetadata extends java.io.Serializable {
		@JsonIgnore
		Class<? extends Balancer> getBalancer();
	}
	
	/* Sort criteria to select which shards are fully filled first */ 
	public enum ShardPresort {
		/* use date of shard's creation or online mark after being offline */
		BY_CREATION_DATE(),
		/* use their reported for the specified pallet */
		BY_WEIGHT_CAPACITY(),
		;
	}
	
	enum Type {
		/* category for balancers who run a fair spread of duties across shards */
		BALANCED,
		/* category for balancers that cause a unbalance distribution of duties on shards*/
		UNBALANCED,
	}
	
	/* category for balancers that makes use of the duty weight*/
	enum Weighted {
		YES,
		NOT
	}
	enum Strategy {

		/* equally sized shards: each one with same amount of entities or almost */
		EVEN_SIZE(EvenSizeBalancer.class, Type.BALANCED, Weighted.NOT),
		/* equally loaded shards: duties clustering according weights*/
		EVEN_WEIGHT(EvenWeightBalancer.class, Type.BALANCED, Weighted.YES),
		/* fairly loaded shards: duty-weight and shard-capacity trade-off distribution */
		FAIR_WEIGHT(FairWeightBalancer.class, Type.BALANCED, Weighted.YES),
		
		/* Unbalanced strategies related to distribution */
		
		/* keep minimum usage of shards: until spill then fill another one but keep frugal */
		SPILLOVER(SpillOverBalancer.class, Type.UNBALANCED, Weighted.YES),
		/* keep agglutination of duties: move them together wherever they are */
		COALESCE(CoalesceBalancer.class, Type.UNBALANCED, Weighted.NOT),
		/* random lazily spread distribution */
		//SCATTER(ShuffleBalancer.class, Type.UNBALANCED, Weighted.NOT),
		;
		Class<? extends Balancer> balancer;
		Type type;
		Weighted weighted;
		Strategy(Class<? extends Balancer> balancer, Type type, Weighted weighted) {
			this.balancer = balancer;
			this.type = type;
			this.weighted = weighted;
		}
		public Type getType() {
			return this.type;
		}
		public Weighted getWeighted() {
			return this.weighted;
		}
		public Class<? extends Balancer> getBalancer() {
			return this.balancer;
		}		
		public BalancerMetadata getBalancerMetadata() {
			try {
				return (BalancerMetadata) Class.forName(getBalancer().getName()+"$Metadata").newInstance();
			} catch (InstantiationException | IllegalAccessException | ClassNotFoundException e) {
				logger.error("{}: Unable to load balancer: {}", getBalancer().getClass(), Balancer.class.getSimpleName(), e);
				return null;
			}
		}
	}

	enum PreSort {
		/**
		 * Dispose duties with perfect mix between all workload values
		 * in order to avoid having two duties of the same workload together
		 * like: 1,2,3,1,2,3,1,2,3 = Good for migration reduction while balanced distrib.  
		 */
		SAW(null),
		/**
		 * Use hashing order
		 */
		HASH(new ShardEntity.HashComparer()),
		/**
		 * Use Creation date order, i.e. natural order.
		 * Use this to keep the migration of duties among shards: to a bare minimum.
		 * Duty workload weight is considered but natural order restricts the re-accomodation much more.
		 * Useful when the master list of duties has lot of changes in time, and low migration is required.
		 * Use this in case your Duties represent Tasks of a short lifecycle.
		 */
		DATE(new ShardEntity.DateComparer()),
		/**
		 * Use Workload order.
		 * Use this to maximize the clustering algorithm's effectiveness.
		 * In presence of frequent variation of workloads, duties will tend to migrate more. 
		 * Otherwise this's the most optimus strategy.
		 * Use this in case your Duties represent Data or Entities with a long lifecycle 
		 */
		WEIGHT(new ShardEntity.WeightComparer()),
		
		/** Use Pallet's custom comparator */
		CUSTOM(null),
		;
		
		private final Comparator<Duty<?>> comp;
		PreSort(final Comparator<Duty<?>> comp) {
			this.comp = comp;
		}
		public Comparator<Duty<?>> getComparator() { 
			return this.comp;
		}
	}

}