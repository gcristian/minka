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
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.tilt.minka.api.Duty;
import io.tilt.minka.api.Pallet;
import io.tilt.minka.api.config.BalancerConfiguration;
import io.tilt.minka.core.leader.distributor.Migrator;
import io.tilt.minka.domain.EntityEvent;
import io.tilt.minka.domain.Shard.CapacityComparer;
import io.tilt.minka.domain.ShardCapacity.Capacity;

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
 * when using ROUND_ROBIN, all shards are filled in serie so smaller shards will 
 * stop receiving duties earlier when reaching out of space, while bigger ones will continue to receive.
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
			this.dispersion = BalancerConfiguration.FAIR_WEIGHT_DISPERSION;
			this.presort = BalancerConfiguration.FAIR_WEIGHT_PRESORT;
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
	public void balance(final Pallet<?> pallet, 
			final Map<NetworkLocation, Set<Duty<?>>> scheme, 
			final Map<EntityEvent, Set<Duty<?>>> backstage,
			final Migrator migrator) {

		final Metadata meta = (Metadata)pallet.getMetadata();
		// order new ones and current ones in order to get a fair distro 
		final Set<Duty<?>> duties = new TreeSet<>(meta.getPresort().getComparator());
		duties.addAll(backstage.get(EntityEvent.CREATE));
		scheme.values().forEach(duties::addAll);
		duties.removeAll(backstage.get(EntityEvent.REMOVE)); // delete those marked for deletion
		if (meta.getDispersion()==Dispersion.EVEN) {
			final Set<Bascule<NetworkLocation, Duty<?>>> bascules = buildBascules(pallet, scheme.keySet(), duties);
			if (bascules==null) {
				return;
			} else if (bascules.isEmpty()) {
				// for (final Iterator<Duty<?>> itDuties = duties.iterator();itDuties.hasNext();
				// migrator.stuck(itDuties.next(), null));
				return;
			}
			final Iterator<Bascule<NetworkLocation, Duty<?>>> itBascs = bascules.iterator();
			final Iterator<Duty<?>> itDuties = duties.iterator();
			Duty<?> duty = null;
			boolean lifted = true;
			while (itBascs.hasNext()) {
				final Bascule<NetworkLocation, Duty<?>> bascule = itBascs.next();
				while (itDuties.hasNext() || !lifted) {
					if (lifted) {
						duty = itDuties.next();
					}
					lifted = bascule.testAndLift(duty, duty.getWeight());
					// save loose remainders
					if (lifted && !itBascs.hasNext() && itDuties.hasNext()) {
						// without overwhelming we can irrespect the fair-weight-even desire
						// adding those left aside by division remainders calc
						while (itDuties.hasNext()) {
							if (!bascule.tryLift(duty = itDuties.next(), duty.getWeight())) {
								migrator.stuck(duty, bascule.getOwner());
							}
						}
					}
					// si esta bascula no la levanto y no queda otra y no hay mas duties
					if (!lifted || !itBascs.hasNext() || !itDuties.hasNext()) {
						while (!itBascs.hasNext() && itDuties.hasNext()) {
							if (!bascule.tryLift(duty = itDuties.next(), duty.getWeight())) {
								migrator.stuck(duty, bascule.getOwner());
							}
						}
						migrator.override(bascule.getOwner(), bascule.getCargo());
						break;
					}
				}
			}
			if (itDuties.hasNext()) {
				logger.error("{}: Insufficient cluster capacity for Pallet: {}, remaining duties without distribution {}",
						getClass().getSimpleName(), pallet, duty.getId());
				while (itDuties.hasNext()) {
					migrator.stuck(itDuties.next(), null);
				}
			}
		} else {
			logger.error("{}: Out of sleeping budget !");
		}
	}

	private final Set<Bascule<NetworkLocation, Duty<?>>> buildBascules(
			final Pallet<?> pallet, 
			final Set<NetworkLocation> onlineShards, 
			final Set<Duty<?>> duties) {

		final Bascule<NetworkLocation, Duty<?>> whole = new Bascule<>();
		duties.forEach(d->whole.lift(d.getWeight()));
		Set<Bascule<NetworkLocation, Duty<?>>> bascules = new LinkedHashSet<>();
		final Set<NetworkLocation> sorted = new TreeSet<>(new CapacityComparer(pallet));
		sorted.addAll(onlineShards);
		for (final NetworkLocation shard: sorted) {
			final Capacity cap = shard.getCapacities().get(pallet);
			if (cap!=null) {
				bascules.add(new Bascule<>(shard, cap.getTotal()));
			}
		}
		double clusterCap = bascules.isEmpty() ? 0 :Bascule.<NetworkLocation, Duty<?>>getMaxRealCapacity(bascules);
		if (clusterCap <=0) {
			logger.error("{}: No available or reported capacity for Pallet: {}", getClass().getSimpleName(), pallet);
			bascules = null;
		} else {
			if (whole.totalLift() >= clusterCap) {
				logger.error("{}: Pallet: {} with Inssuficient/Almost cluster capacity (max: {}, required load: {})", 
					getClass().getSimpleName(), pallet, clusterCap, whole.totalLift());
			}
			for (final Bascule<NetworkLocation, Duty<?>> b: bascules) {
				// fairness thru shard's capacities flattening according total weight
				// so total bascule's fair value ends up close to equal the total weight
				final double fair = Math.min(whole.totalLift() * (b.getMaxRealCapacity() / clusterCap), b.getMaxRealCapacity());
				if (logger.isInfoEnabled()) {
					logger.info("{}: Shard: {} Fair load: {}, capacity: {} (c.c. {}, d.w. {})", 
						getClass().getSimpleName(), 
						b.getOwner(), 
						fair, 
						b.getMaxRealCapacity(), 
						clusterCap, 
						whole.totalLift());
				}
				b.setMaxTestingCapacity(fair);
			}
		}
		return bascules;
	}

}
