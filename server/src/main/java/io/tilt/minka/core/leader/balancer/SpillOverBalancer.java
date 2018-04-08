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

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.SetMultimap;
import com.google.common.util.concurrent.AtomicDouble;

import io.tilt.minka.api.Config;
import io.tilt.minka.api.Duty;
import io.tilt.minka.api.Pallet;
import io.tilt.minka.api.PartitionDelegate;
import io.tilt.minka.core.leader.distributor.Balancer;
import io.tilt.minka.core.leader.distributor.Migrator;
import io.tilt.minka.domain.Shard.CapacityComparer;
import io.tilt.minka.domain.Shard.DateComparer;
import io.tilt.minka.domain.ShardCapacity.Capacity;
import io.tilt.minka.domain.EntityEvent;
import io.tilt.minka.domain.ShardEntity;

/**
 * Type unbalanced.
 * Balances and distributes duties by spilling from one shard to another.
 * 
 * Purpose: progressive usage of the cluster by fully filling a shard at its max capacity
 * before going to the next shard in an order set by {@linkplain ShardPresort}
 * The max capacity is taken from one of the following attributes: 
 * 	  - custom value specified in {@linkplain Metadata} as a measure of Weight or as a Quantity of duties 
 * 	  - calculated using the {@linkplain Capacity} reported by the shard's {@linkplain PartitionDelegate} 
 * Effect: controlled growth from smaller to bigger shards, leave empty shards until needed,
 * without losing high availability.
 * 
 * @author Cristian Gonzalez
 * @since Dec 13, 2015
 */
public class SpillOverBalancer implements Balancer {

	private final Logger logger = LoggerFactory.getLogger(getClass());

	public static class Metadata implements BalancerMetadata {
		private static final long serialVersionUID = 4626080354583725779L;
		private final MaxUnit maxUnit;
		private final double maxValue;
		private final ShardPresort shardPresort;
		private final boolean ascending;
		@Override
		public Class<? extends Balancer> getBalancer() {
			return SpillOverBalancer.class;
		}
		public Metadata(final MaxUnit maxUnit, final double maxValue, 
				final ShardPresort shardPresort, final boolean ascending) {
			super();
			this.maxUnit = maxUnit;
			this.maxValue = maxValue;
			this.ascending = ascending;
			this.shardPresort = shardPresort;
		}
		public Metadata() {
			super();
			this.maxUnit = Config.BalancerConf.SPILL_OVER_MAX_UNIT;
			this.maxValue = Config.BalancerConf.SPILL_OVER_MAX_VALUE;
			this.shardPresort = ShardPresort.BY_CREATION_DATE;
			this.ascending = true;
		}
		protected MaxUnit getMaxUnit() {
			return this.maxUnit;
		}
		public double getMaxValue() {
			return this.maxValue;
		}
		public ShardPresort getShardPresort() {
			return this.shardPresort;
		}
		public boolean isAscending() {
			return this.ascending;
		}
		@Override
		public String toString() {
			return new StringBuilder("Spillover")
					.append("-MaxUnit:").append(getMaxUnit())
					.append("-MaxVale:").append(getMaxValue())
					.append("-ShardPresort:").append(getShardPresort())
					.append("-Ascending:").append(isAscending())
					.toString();
		}
	}
	
	public enum MaxUnit {
		/* Use the Max value to compare the sum of all running duties's weights 
		 * and restrict new additions over a full shard */
		DUTY_WEIGHT,
		/* Use the Max value as max number of duties to fit in one shard */
		DUTY_SIZE,
		/* @default Ignore given value and use Shard's reported capacity as max value */
		USE_CAPACITY,
	}


	@Override
	public void balance(
			final Pallet<?> pallet,
			final Map<NetworkLocation, Set<Duty<?>>> scheme,
			final Map<EntityEvent, Set<Duty<?>>> backstage,
			final Migrator migrator) {

		final Metadata meta = (Metadata)pallet.getMetadata();
		if (meta.getMaxValue() < 0) {
			final NetworkLocation receptor = getAnyReceptor(scheme);
			logger.info("{}: For Unbounded duties (max = -1) found Shard receptor: {}", getClass().getSimpleName(),
					receptor);
			//creations.addAll(dangling);
			moveAllToOne(scheme, backstage, migrator, receptor);
		} else {
			logger.info("{}: Computing Spilling strategy: {} with a Max Value: {}", getClass().getSimpleName(), meta.getMaxUnit(), 
				meta.getMaxUnit() == MaxUnit.USE_CAPACITY ? "{shard's capacity}" : meta.getMaxValue());
			
			boolean loadStrat = meta.getMaxUnit() == MaxUnit.DUTY_WEIGHT || meta.getMaxUnit() == MaxUnit.USE_CAPACITY;
			final Map<NetworkLocation, AtomicDouble> spaceByReceptor = new HashMap<>();
			final SetMultimap<NetworkLocation, Duty<?>> trans = collectTransceivers(pallet, scheme, loadStrat, spaceByReceptor, meta);
			if (trans==null || (spaceByReceptor.isEmpty() && !trans.isEmpty())) {
				logger.warn("{}: Couldnt find receptors to spill over to", getClass().getSimpleName());
			} else {
				logger.info("{}: Shard with space for allocating Duties: {}", getClass().getSimpleName(), spaceByReceptor);
				final List<Duty<?>> unfitting = new ArrayList<>();
				final List<Duty<?>> dutiesForBalance = new ArrayList<>();
				dutiesForBalance.addAll(backstage.get(EntityEvent.CREATE)); // priority for new comers
				//dutiesForBalance.addAll(dangling);
				if (loadStrat) {
					// so we can get the biggest accomodation of duties instead of the heaviest
					Collections.sort(dutiesForBalance, Collections.reverseOrder(new ShardEntity.WeightComparer()));
				}
				registerNewcomers(loadStrat, spaceByReceptor, unfitting, dutiesForBalance, migrator);
				// then move those surplussing
				registerMigrations(pallet, migrator, loadStrat, spaceByReceptor, unfitting, trans);
				logger.error("{}: Add Shards !! No receptor has space for Duties: {}", getClass().getSimpleName(), 
						ShardEntity.toDutyStringIds(unfitting));
			}
		}
	}

	private void registerNewcomers(
			final boolean loadStrat,
			final Map<NetworkLocation, AtomicDouble> spaceByReceptor, 
			final List<Duty<?>> unfitting,
			final List<Duty<?>> dutiesForBalance,
			final Migrator migrator) {

		for (final Duty<?> newcomer : dutiesForBalance) {
			final NetworkLocation receptor = makeSpaceIntoReceptors(loadStrat, newcomer, spaceByReceptor);
			if (receptor != null) {
				migrator.transfer(receptor, newcomer);				
				logger.info("{}: Assigning to Shard: {} (space left: {}), Duty: {}", getClass().getSimpleName(),
						receptor, spaceByReceptor.get(receptor), newcomer.getId());
			} else {
				migrator.stuck(newcomer, null);
				unfitting.add(newcomer);
			}
		}
	}

	private void registerMigrations(
			final Pallet<?> pallet,
			final Migrator migrator, 
			final boolean loadStrat,
			final Map<NetworkLocation, AtomicDouble> spaceByReceptor, 
			final List<Duty<?>> unfitting,
			final SetMultimap<NetworkLocation, Duty<?>> trans) {

		for (final NetworkLocation emisor : trans.keys()) {
			final Set<Duty<?>> emitting = trans.get(emisor);
			for (final Duty<?> emitted : emitting) {
				final NetworkLocation receptor = makeSpaceIntoReceptors(loadStrat, emitted, spaceByReceptor);
				if (receptor == null) {
					migrator.stuck(emitted, emisor);
					unfitting.add(emitted);
				} else {
					migrator.transfer(emisor, receptor, emitted);
					logger.info("{}: Migrating from: {} to {}: Duty: {}", getClass().getSimpleName(), emisor, receptor,
							emitted);
				}
			}
		}
	}


	/* elect duties from emisors and compute receiving size at receptors */
	private SetMultimap<NetworkLocation, Duty<?>> collectTransceivers(
			final Pallet<?> pallet,
			final Map<NetworkLocation, Set<Duty<?>>> scheme,
	        final boolean loadStrat, 
			final Map<NetworkLocation, AtomicDouble> spaceByReceptor, 
			final Metadata meta) {

		final SetMultimap<NetworkLocation, Duty<?>> transmitting = HashMultimap.create();
		
		final List<NetworkLocation> sorted = new ArrayList<>(scheme.keySet());
		final Comparator<NetworkLocation> comp = meta.getShardPresort() == ShardPresort.BY_WEIGHT_CAPACITY ? 
				new CapacityComparer(pallet) : new DateComparer();
		Collections.sort(sorted, meta.isAscending() ? comp : Collections.reverseOrder(comp));
		
		for (final NetworkLocation location : sorted) {		
			final Capacity cap = location.getCapacities().get(pallet);
			double maxValue = 0;
			if (meta.getMaxUnit() == MaxUnit.USE_CAPACITY) {
				if (cap!=null) {
					maxValue = cap.getTotal();
				} else {
					logger.error("{}: Aborting balance ! Shard: {} without reported capacity for pallet: {}", 
							getClass().getSimpleName(), location, pallet);
					return null;
				}
			} else {
				maxValue = meta.getMaxValue();
			}

			final Set<Duty<?>> dutiesByShard = scheme.get(location);
			final List<Duty<?>> checkingDuties = new ArrayList<>(dutiesByShard);
			if (loadStrat) {
				// order it so we can obtain sequentially granulated reductions of the transmitting shard
				Collections.sort(checkingDuties, new ShardEntity.WeightComparer());
			}
			final double totalWeight = dutiesByShard.stream().mapToDouble(i -> i.getWeight()).sum();
			final boolean isEmisor = (!loadStrat && dutiesByShard.size() > maxValue)
					|| (loadStrat && (totalWeight) > maxValue);
			final boolean isReceptor = (!loadStrat && dutiesByShard.size() < maxValue)
					|| (loadStrat && totalWeight < maxValue);
			if (isEmisor) {
				if (!loadStrat) {
					for (int i = 0; i < dutiesByShard.size() - maxValue; i++) {
						transmitting.put(location, checkingDuties.get(i));
					}
				} else if (loadStrat) {
					double liftAccumulated = 0;
					for (final Duty<?> takeMe : dutiesByShard) {
						double load = takeMe.getWeight();
						if ((liftAccumulated += load) <= totalWeight - maxValue) {
							transmitting.put(location, takeMe);
						} else {
							break; // following are bigger because sorted
						}
					}
				}
			} else if (isReceptor) {
				// until loop ends we cannot for certain which shard is receptor and can receive which duty
				spaceByReceptor.put(location,
						new AtomicDouble(maxValue - (loadStrat ? totalWeight : dutiesByShard.size())));
			}
		}
		return transmitting;
	}
	
	private NetworkLocation makeSpaceIntoReceptors(
			final boolean loadStrat, 
			final Duty<?> duty,
			final Map<NetworkLocation, AtomicDouble> spaceByReceptor) {

		for (final NetworkLocation receptor : spaceByReceptor.keySet()) {
			AtomicDouble space = spaceByReceptor.get(receptor);
			final double newWeight = loadStrat ? duty.getWeight() : 1;
			if (space.get() - newWeight >= 0) {
				double surplus = space.addAndGet(-newWeight);
				// add the new weight to keep the map updated for latter emittings 
				if (logger.isDebugEnabled()) {
					logger.debug("{}: Making space for: {} into Shard: {} still has: {}", getClass().getSimpleName(),
							duty, receptor, surplus);
				}
				return receptor;
			}
		}
		return null;
	}

	private void moveAllToOne(
			final Map<NetworkLocation, Set<Duty<?>>> scheme,
			final Map<EntityEvent, Set<Duty<?>>> backstage,
			final Migrator migrator,
			final NetworkLocation receptor) {
		for (final NetworkLocation shard : scheme.keySet()) {
			if (shard != receptor) {
				Set<Duty<?>> dutiesByShard = scheme.get(shard);
				if (!dutiesByShard.isEmpty()) {
					for (final Duty<?> duty : dutiesByShard) {
						if (!backstage.get(EntityEvent.REMOVE).contains(duty)) {
							migrator.transfer(shard, receptor, duty);
							logger.info("{}: Hoarding from Shard: {} to Shard: {}, Duty: {}", getClass().getSimpleName(),
								shard, receptor, duty);
						}
					}
				}
			}
		}
		for (Duty<?> creation: backstage.get(EntityEvent.CREATE)) {
			migrator.transfer(receptor, creation);
		}
	}

	private NetworkLocation getAnyReceptor(final Map<NetworkLocation, Set<Duty<?>>> schemeDistro) {
		for (NetworkLocation shard: schemeDistro.keySet()) {
			Set<Duty<?>> dutiesByShard = schemeDistro.get(shard);
			if (!dutiesByShard.isEmpty()) {
				return shard;
			}
		}
		return schemeDistro.keySet().iterator().next();
	}

}