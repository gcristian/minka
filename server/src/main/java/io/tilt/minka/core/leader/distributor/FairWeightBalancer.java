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

import static io.tilt.minka.domain.ShardState.ONLINE;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.util.concurrent.AtomicDouble;

import io.tilt.minka.api.Pallet;
import io.tilt.minka.core.leader.PartitionTable;
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
			this.dispersion = Dispersion.EVEN;
			this.presort = PreSort.DATE;
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
	}
	
	enum Dispersion {
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
	public void balance(final Pallet<?> pallet, final PartitionTable table, final Reallocation realloc, 
			final List<Shard> onlineShards, final Set<ShardEntity> creations, final Set<ShardEntity> deletions) {
		
		final Metadata meta = (Metadata)pallet.getStrategy();
		// order new ones and current ones in order to get a fair distro 
		final Set<ShardEntity> duties = new TreeSet<>(meta.getPresort().getComparator());
		duties.addAll(creations);
		duties.addAll(table.getDutiesAllByShardState(pallet, ONLINE));
		duties.removeAll(deletions); // delete those marked for deletion
		final List<ShardEntity> dutiesSorted = new ArrayList<>(duties);
		logger.debug("{}: Before Balance: {} ({})", getClass().getSimpleName(), ShardEntity.toStringIds(dutiesSorted));

		if (meta.getDispersion()==Dispersion.EVEN) {
			final Map<Shard, Double> fairWeightByShard = getFairWeights(pallet, onlineShards, duties);
			if (fairWeightByShard==null) {
				return;
			}
			balance_(pallet, table, realloc, onlineShards, duties, fairWeightByShard);
		} else {
			logger.error("{}: Strategy yet to code !");
				return ;
		}

	}

	private void balance_(final Pallet<?> pallet, final PartitionTable table, final Reallocation realloc,
			final List<Shard> onlineShards, final Set<ShardEntity> duties, final Map<Shard, Double> fairWeightByShard) {
		final Migrator migra = new Migrator(table, realloc);
		final Set<Shard> capacitySortedShards = new TreeSet<>(new CapacityComparer(pallet));
		capacitySortedShards.addAll(onlineShards);
		final Iterator<ShardEntity> itDuties =  duties.iterator();
		for (final Shard shard: capacitySortedShards) {
			final double fairload = fairWeightByShard.get(shard);
			double accum = 0;
			final Set<ShardEntity> tmp = new HashSet<>();
			while (itDuties.hasNext()) {
				final ShardEntity duty = itDuties.next();
				if (accum + duty.getDuty().getWeight() >= fairload || !itDuties.hasNext()) {
					tmp.add(duty);
					migra.transfer(pallet, shard, tmp);
					tmp.clear();
					accum=0;
					break;
				} else {
					tmp.add(duty);
					accum+=duty.getDuty().getWeight();
				}
			}
		}
		if (itDuties.hasNext()) {
			logger.error("{}: Aborting balancing ! Almost Full: change balancer's PreSort for Pallet: {} to avoid duty splitting "
					+ "coefficients grow outside the last shard", getClass().getSimpleName(), pallet);
			return ;
		}
		migra.execute();
	}

	private final Map<Shard, Double> getFairWeights(Pallet<?> pallet, List<Shard> onlineShards, final Set<ShardEntity> duties) {
		final AtomicDouble clusterCapacity = new AtomicDouble();
		for (final Shard shard: onlineShards) {
			final Capacity cap = shard.getCapacities().get(pallet);
			if (cap==null) {
				logger.error("{}: Aborting balancing ! Shard: {} without reported capacity for Pallet: {}", 
						getClass().getSimpleName(), shard, pallet);
				return null;
			} else {
				clusterCapacity.addAndGet(cap.getTotal());
			}
		}
		// check all duties at least fit in some shard
		final AtomicDouble dutyWeight = new AtomicDouble();
		for (ShardEntity duty: duties) {
			double weight = duty.getDuty().getWeight();
			boolean noFit = true;
			for (final Shard shard: onlineShards) {
				final Capacity cap = shard.getCapacities().get(pallet);
				noFit &= cap.getTotal()<weight;
			}
			if (noFit) {
				logger.warn("{}: Duty will not be distributed ! no shard with enough capacity: {}", 
				getClass().getSimpleName(), duty);
			}
			dutyWeight.addAndGet(weight);
		}
		
		if (dutyWeight.get() > clusterCapacity.get()) {
			logger.error("{}: Aborting balancing ! Cluster capacity (max: {}) Inssuficient (load: {}) for Pallet: {}", 
					getClass().getSimpleName(), clusterCapacity, dutyWeight, pallet);
			return null;
		} else if (dutyWeight.get() == clusterCapacity.get()) {
			logger.warn("{}: Cluster capacity (max: {}) at Risk (load: {}) for Pallet: {}", 
					getClass().getSimpleName(), clusterCapacity, dutyWeight, pallet);
		}
		
		// make distribution
		final Map<Shard, Double> ret = new HashMap<>();
		for (final Shard shard: onlineShards) {
			final double percentual = shard.getCapacities().get(pallet).getTotal() / clusterCapacity.get();
			final double load = dutyWeight.get() * percentual;
			logger.info("{}: Shard: {} Fair load: {}, Capacity: {} (Duty weight: {}, Cluster capacity: {})", 
					getClass().getSimpleName(), shard, load, shard.getCapacities().get(pallet).getTotal(),
					dutyWeight.get(), clusterCapacity.get());
			ret.put(shard, load);
		}
		return ret;
	}


}
