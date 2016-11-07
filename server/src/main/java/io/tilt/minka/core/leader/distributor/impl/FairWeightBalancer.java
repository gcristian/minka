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
package io.tilt.minka.core.leader.distributor.impl;

import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.util.concurrent.AtomicDouble;

import io.tilt.minka.api.Config;
import io.tilt.minka.api.Pallet;
import io.tilt.minka.core.leader.distributor.Arranger.NextTable;
import io.tilt.minka.core.leader.distributor.Balancer;
import io.tilt.minka.core.leader.distributor.Migrator;
import io.tilt.minka.domain.Shard;
import io.tilt.minka.domain.Shard.CapacityComparer;
import io.tilt.minka.domain.ShardCapacity.Capacity;
import io.tilt.minka.domain.ShardEntity;

/**
 * Type balanced.
 * Balancer to achieve a fair load of shards according their reported capacities.
 * Useful for physical resource exhaustion pallets. 
 * Effect: bigger capacity shards hold more duty weight than smaller ones.
 * Purpose: each shard receives duties whose accumulated weight depends on {@linkplain Dispersion}
 *  
 * when using EVEN, each shard's max load matches the following formula: 
 * shard weight = total duty weight * ( shard capacity / cluster capacity )
 * so all shards will reach their maximum load at the same time.
 *
 * when using ROUND_ROBIN, all shards are filled in serie so smaller shards earlier stops receiving
 * duties when reaching out of space, and bigger ones will still receive.
 * 
 * @author Cristian Gonzalez
 */
public class FairWeightBalancer implements Balancer {

	private final Logger logger = LoggerFactory.getLogger(getClass());
	
		
	public static class Metadata implements BalancerMetadata {
		private static final long serialVersionUID = 8411425951224530387L;
		private final PreSort presort;
		private final Dispersion dispersion;
		
		@Override
		public Class<? extends Balancer> getBalancer() {
			return FairWeightBalancer.class;
		}
		public Metadata() {
			this.dispersion = Config.BalancerConf.FAIR_WEIGHT_DISPERSION;
			this.presort = Config.BalancerConf.FAIR_WEIGHT_PRESORT;
		}
		public Metadata(final Dispersion dispersion, final PreSort presort) {
			super();
			this.dispersion = dispersion;
			this.presort = presort;
		}
		public Dispersion getDispersion() {
			return this.dispersion;
		}
		public PreSort getPresort() {
			return this.presort;
		}
		@Override
		public String toString() {
			return "FairWeight-PreSort:" + presort + "-Dispersion:" + dispersion;
		}
	}
	
	public enum Dispersion {
		/* an even load filling so different capacity shards will reach
		 * their maximum load at the same time. This will make the bigger shards work more than smaller
		 * 
		 * cc = cluster cpacity, c(x) = shard capacity, w = duty weight, sw(x) = shard weight
		 * f(sw_x) = w * (c_x / cc) 
		 */
		EVEN,
		/* a serial filling so smaller shards will fast stop receiving 
		 * duties and bigger one will still receiving. This will */
		ROUND_ROBIN
	}
	

	@Override
	public Migrator balance(final NextTable next) {
		final Metadata meta = (Metadata)next.getPallet().getStrategy();
		// order new ones and current ones in order to get a fair distro 
		final Set<ShardEntity> duties = new TreeSet<>(meta.getPresort().getComparator());
		duties.addAll(next.getCreations());
		duties.addAll(next.getDuties());
		duties.removeAll(next.getDeletions()); // delete those marked for deletion
		if (meta.getDispersion()==Dispersion.EVEN) {
			final Map<Shard, Double> fairWeightByShard = calculateEvenFairWeights(next.getPallet(), next.getIndex().keySet(), duties);
			if (fairWeightByShard!=null) {
				return bundleEvenFairness(next, fairWeightByShard, duties, meta.getPresort().getComparator());
			}
		} else {
			logger.error("{}: Strategy yet to code !");
		}
		return null;
	}

	private Migrator bundleEvenFairness(final NextTable next, final Map<Shard, Double> fairWeightByShard, 
			final Set<ShardEntity> duties, final Comparator<ShardEntity> dutyComparator) {
		final Set<Shard> capacitySortedShards = new TreeSet<>(new CapacityComparer(next.getPallet()));
		capacitySortedShards.addAll(next.getIndex().keySet());
		final Iterator<ShardEntity> itDuties = duties.iterator();
		final Set<ShardEntity> tmp = new TreeSet<>(dutyComparator);
		final Migrator migra = next.buildMigrator();
		final Iterator<Shard> itShards = capacitySortedShards.iterator();
		while (itShards.hasNext()) {
			final Shard shard = itShards.next();
			final Double fairload = fairWeightByShard.get(shard);
			if (fairload !=null && fairload > 0) {
				double accum = 0;
				while (itDuties.hasNext()) {
					final ShardEntity duty = itDuties.next();
					if (accum + duty.getDuty().getWeight() > fairload || !itDuties.hasNext() || 
							!itShards.hasNext()) {
						final boolean fixDivRemainders = !itShards.hasNext() && itDuties.hasNext();
						if (fixDivRemainders) {
							tmp.add(duty);
							while (itDuties.hasNext()) tmp.add(itDuties.next());
						}
						migra.override(shard, tmp);
						tmp.clear();
						if (!fixDivRemainders) {
							tmp.add(duty);
						}
						accum=0;
						break;
					} else {
						tmp.add(duty);
						accum+=duty.getDuty().getWeight();
					}
				}
			}
		}
		if (!tmp.isEmpty()) {
			logger.error("{}: Insufficient cluster capacity for Pallet: {}, remaining duties without distribution {}", 
				getClass().getSimpleName(), next.getPallet(), ShardEntity.toString(tmp));
		}
		return migra;
	}

	private final Map<Shard, Double> calculateEvenFairWeights(Pallet<?> pallet, Set<Shard> onlineShards, final Set<ShardEntity> duties) {
		final AtomicDouble clusterCapacity = new AtomicDouble();
		for (final Shard shard: onlineShards) {
			final Capacity cap = shard.getCapacities().get(pallet);
			if (cap==null) {
				logger.error("{}: Excluding Shard: {} without reported capacity for Pallet: {}", 
					getClass().getSimpleName(), shard, pallet);
			} else {
				clusterCapacity.addAndGet(cap.getTotal());
			}
		}
		if (clusterCapacity.get()<=0) {
			logger.error("{}: No available or reported capacity for Pallet: {}", getClass().getSimpleName(), pallet);
			return null;
		}
		// check all duties at least fit in some shard
		final AtomicDouble dutyWeight = new AtomicDouble();
		for (ShardEntity duty: duties) {
			double weight = duty.getDuty().getWeight();
			boolean noFit = true;
			for (final Shard shard: onlineShards) {
				final Capacity cap = shard.getCapacities().get(pallet);
				noFit &= cap==null || cap.getTotal()<weight;
			}
			if (noFit) {
				logger.error("{}: Duty will not be distributed ! no shard with enough capacity: {}", 
				getClass().getSimpleName(), duty);
			} else {
				dutyWeight.addAndGet(weight);
			}
		}
		
		if (dutyWeight.get() > clusterCapacity.get()) {
			logger.error("{}: Pallet: {} with Inssuficient cluster capacity (max: {}, required load: {})", 
					getClass().getSimpleName(), pallet, clusterCapacity, dutyWeight);
		} else if (dutyWeight.get() == clusterCapacity.get()) {
			logger.warn("{}: Cluster capacity (max: {}) at Risk (load: {}) for Pallet: {}", 
					getClass().getSimpleName(), clusterCapacity, dutyWeight, pallet);
		}
		
		// make distribution
		final Map<Shard, Double> ret = new HashMap<>();
		for (final Shard shard: onlineShards) {
			final Capacity cap = shard.getCapacities().get(pallet);
			if (cap!=null) {
				final double maxWeight = cap.getTotal();
				final double load = Math.min(dutyWeight.get() * (maxWeight / clusterCapacity.get()), maxWeight);
				logger.info("{}: Shard: {} Fair load: {}, capacity: {} (c.c. {}, d.w. {})", getClass().getSimpleName(), 
						shard, load, maxWeight, clusterCapacity.get(), dutyWeight.get());
				ret.put(shard, load);
			}
		}
		return ret;
	}


}
