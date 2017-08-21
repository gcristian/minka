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

import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.TreeSet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
	/**
	 * this algorithm makes the best effort allocating all duties over it's fairness formula
	 * also fixing division remainders, but without overwhelming shards. 
	 */
	public void balance(final NextTable next) {
		final Metadata meta = (Metadata)next.getPallet().getMetadata();
		// order new ones and current ones in order to get a fair distro 
		final Set<ShardEntity> duties = new TreeSet<>(meta.getPresort().getComparator());
		duties.addAll(next.getCreations());
		duties.addAll(next.getDuties());
		duties.removeAll(next.getDeletions()); // delete those marked for deletion
		if (meta.getDispersion()==Dispersion.EVEN) {
			final Set<Bascule<Shard, ShardEntity>> bascules = buildBascules(next.getPallet(), next.getIndex().keySet(), duties);
			if (bascules.isEmpty()) {
			    /*for (final Iterator<ShardEntity> itDuties = duties.iterator(); itDuties.hasNext(); 
			            itDuties.next().registerEvent(ShardEntity.State.STUCK));*/
				return;
			}
    		final Migrator migra = next.getMigrator();
    		final Iterator<Bascule<Shard, ShardEntity>> itBascs = bascules.iterator();
    		final Iterator<ShardEntity> itDuties = duties.iterator();
    		ShardEntity duty = null;
    		boolean lifted = true;
    		while (itBascs.hasNext()) {
    			final Bascule<Shard, ShardEntity> bascule = itBascs.next();
    			while (itDuties.hasNext()) {
    				if (lifted) {
    					duty = itDuties.next();
    				}
    				lifted = bascule.testAndLift(duty, duty.getDuty().getWeight());
    				final boolean saveLooseRemainders = lifted && !itBascs.hasNext() && itDuties.hasNext();
                    if (saveLooseRemainders) {
    					// without overwhelming we can irrespect the fair-weight-even desire
    					// adding those left aside by division remainders calc
    					while (itDuties.hasNext()) {
    						if (!bascule.tryLift(duty = itDuties.next(), duty.getDuty().getWeight())) {
    						    duty.addEvent(ShardEntity.State.STUCK);
    						}
    					}
    				}
    				// si esta bascula no la levanto y no queda otra y no hay mas duties
    				if (!lifted || !itBascs.hasNext() || !itDuties.hasNext()) {
    					migra.override(bascule.getOwner(), bascule.getCargo());
    					break;
    				}
    			}
    		}
    		if (itDuties.hasNext()) {
    			logger.error("{}: Insufficient cluster capacity for Pallet: {}, remaining duties without distribution {}", 
    				getClass().getSimpleName(), next.getPallet(), duty.toBrief());
    			while (itDuties.hasNext()) {
    			    itDuties.next().addEvent(ShardEntity.State.STUCK);
    			}
    		}
		} else {
			logger.error("{}: Out of sleeping budget !");
		}
	}

	private final Set<Bascule<Shard, ShardEntity>> buildBascules(
	        final Pallet<?> pallet, 
	        final Set<Shard> onlineShards, 
	        final Set<ShardEntity> duties) {
	    
		final Bascule<Shard, ShardEntity> whole = new Bascule<>();
		duties.forEach(d->whole.lift(d.getDuty().getWeight()));
		final Set<Bascule<Shard, ShardEntity>> bascules = new LinkedHashSet<>();
		final Set<Shard> sorted = new TreeSet<>(new CapacityComparer(pallet));
		sorted.addAll(onlineShards);
		for (final Shard shard: sorted) {
			final Capacity cap = shard.getCapacities().get(pallet);
			if (cap!=null) {
				bascules.add(new Bascule<>(shard, cap.getTotal()));
			}
		}
		double clusterCap = bascules.isEmpty() ? 0 :Bascule.<Shard, ShardEntity>getMaxRealCapacity(bascules);
		if (clusterCap <=0) {
			logger.error("{}: No available or reported capacity for Pallet: {}", getClass().getSimpleName(), pallet);
		} else {
			if (whole.totalLift() >= clusterCap) {
				logger.error("{}: Pallet: {} with Inssuficient/Almost cluster capacity (max: {}, required load: {})", 
					getClass().getSimpleName(), pallet, clusterCap, whole.totalLift());
			}
			for (final Bascule<Shard, ShardEntity> b: bascules) {
				final double fair = Math.min(whole.totalLift() * (b.getMaxRealCapacity() / clusterCap), b.getMaxRealCapacity());
				logger.info("{}: Shard: {} Fair load: {}, capacity: {} (c.c. {}, d.w. {})", getClass().getSimpleName(), 
						b.getOwner(), fair, b.getMaxRealCapacity(), clusterCap, whole.totalLift());
				b.setMaxTestingCapacity(fair);
			}
		}
		return bascules;
	}

}
