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
import io.tilt.minka.api.Pallet;
import io.tilt.minka.api.PartitionDelegate;
import io.tilt.minka.core.leader.distributor.Balancer;
import io.tilt.minka.core.leader.distributor.Migrator;
import io.tilt.minka.domain.Shard;
import io.tilt.minka.domain.Shard.CapacityComparer;
import io.tilt.minka.domain.Shard.DateComparer;
import io.tilt.minka.domain.ShardCapacity.Capacity;
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
@SuppressWarnings("deprecation")
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
			final Set<ShardEntity> stageDuties, 
			final Map<Shard, Set<ShardEntity>> stageDistro,
			final Set<ShardEntity> creations,
			final Set<ShardEntity> deletions,
			final Migrator migrator) {

		final Metadata meta = (Metadata)pallet.getMetadata();
		if (meta.getMaxValue() < 0) {
			final Shard receptorShard = getAnyReceptorShard(stageDistro);
			logger.info("{}: For Unbounded duties (max = -1) found Shard receptor: {}", getClass().getSimpleName(),
					receptorShard);
			//creations.addAll(dangling);
			moveAllToOne(stageDistro, creations, deletions, migrator, receptorShard);
		} else {
			logger.info("{}: Computing Spilling strategy: {} with a Max Value: {}", getClass().getSimpleName(), meta.getMaxUnit(), 
				meta.getMaxUnit() == MaxUnit.USE_CAPACITY ? "{shard's capacity}" : meta.getMaxValue());
			
			boolean loadStrat = meta.getMaxUnit() == MaxUnit.DUTY_WEIGHT || meta.getMaxUnit() == MaxUnit.USE_CAPACITY;
			final Map<Shard, AtomicDouble> spaceByReceptor = new HashMap<>();
			final SetMultimap<Shard, ShardEntity> trans = collectTransceivers(pallet, stageDistro, loadStrat, spaceByReceptor, meta);
			if (trans==null || (spaceByReceptor.isEmpty() && !trans.isEmpty())) {
				logger.warn("{}: Couldnt find receptors to spill over to", getClass().getSimpleName());
			} else {
				logger.info("{}: Shard with space for allocating Duties: {}", getClass().getSimpleName(), spaceByReceptor);
				final List<ShardEntity> unfitting = new ArrayList<>();
				final List<ShardEntity> dutiesForBalance = new ArrayList<>();
				dutiesForBalance.addAll(creations); // priority for new comers
				//dutiesForBalance.addAll(dangling);
				if (loadStrat) {
					// so we can get the biggest accomodation of duties instead of the heaviest
					Collections.sort(dutiesForBalance, Collections.reverseOrder(new ShardEntity.WeightComparer()));
				}
				registerNewcomers(loadStrat, spaceByReceptor, unfitting, dutiesForBalance, migrator);
				// then move those surplussing
				registerMigrations(pallet, stageDuties, stageDistro, creations, deletions, migrator, 
						loadStrat, spaceByReceptor, unfitting, trans);
				logger.error("{}: Add Shards !! No receptor has space for Duties: {}", getClass().getSimpleName(), 
						ShardEntity.toStringIds(unfitting));
			}
		}
	}

	private void registerNewcomers(
			final boolean loadStrat,
			final Map<Shard, AtomicDouble> spaceByReceptor, 
			final List<ShardEntity> unfitting,
			final List<ShardEntity> dutiesForBalance,
			final Migrator migrator) {

		for (final ShardEntity newcomer : dutiesForBalance) {
			final Shard receptor = makeSpaceIntoReceptors(loadStrat, newcomer, spaceByReceptor);
			if (receptor != null) {
				migrator.transfer(receptor, newcomer);				
				logger.info("{}: Assigning to Shard: {} (space left: {}), Duty: {}", getClass().getSimpleName(),
						receptor.getShardID(), spaceByReceptor.get(receptor), newcomer.toBrief());
			} else {
				//newcomer.getLog().addEvent(EntityEvent.ATTACH, EntityState.STUCK,
				//        null, 
                //        planId);
				//newcomer.setStuckCause(StuckCause.UNSUITABLE);
				//next.getRoadmap().getProblems().put(null, newcomer);
				unfitting.add(newcomer);
			}
		}
	}

	private void registerMigrations(
			final Pallet<?> pallet,
			final Set<ShardEntity> stageDuties, 
			final Map<Shard, Set<ShardEntity>> stageDistro,
			final Set<ShardEntity> creations,
			final Set<ShardEntity> deletions,
			final Migrator migrator, 
			final boolean loadStrat,
			final Map<Shard, AtomicDouble> spaceByReceptor, 
			final List<ShardEntity> unfitting,
			final SetMultimap<Shard, ShardEntity> trans) {

		for (final Shard emisor : trans.keys()) {
			final Set<ShardEntity> emitting = trans.get(emisor);
			for (final ShardEntity emitted : emitting) {
				final Shard receptor = makeSpaceIntoReceptors(loadStrat, emitted, spaceByReceptor);
				if (receptor == null) {
					/*emitted.getLog().addEvent(EntityEvent.ATTACH, 
							EntityState.STUCK,
					       emisor.getShardID(), 
	                        planId);
					emitted.setStuckCause(StuckCause.UNSUITABLE);
					next.getRoadmap().getProblems().put(null, emitted);
					*/
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
	private SetMultimap<Shard, ShardEntity> collectTransceivers(
			final Pallet<?> pallet,
			final Map<Shard, Set<ShardEntity>> stageDistro,
	        final boolean loadStrat, 
			final Map<Shard, AtomicDouble> spaceByReceptor, 
			final Metadata meta) {

		final SetMultimap<Shard, ShardEntity> transmitting = HashMultimap.create();
		
		final List<Shard> sortedShards = new ArrayList<>(stageDistro.keySet());
		final Comparator<Shard> comp = meta.getShardPresort() == ShardPresort.BY_WEIGHT_CAPACITY ? 
				new CapacityComparer(pallet) : new DateComparer();
		Collections.sort(sortedShards, meta.isAscending() ? comp : Collections.reverseOrder(comp));
		
		for (final Shard shard : sortedShards) {		
			final Capacity cap = shard.getCapacities().get(pallet);
			double maxValue = 0;
			if (meta.getMaxUnit() == MaxUnit.USE_CAPACITY) {
				if (cap!=null) {
					maxValue = cap.getTotal();
				} else {
					logger.error("{}: Aborting balance ! Shard: {} without reported capacity for pallet: {}", 
							getClass().getSimpleName(), shard, pallet);
					return null;
				}
			} else {
				maxValue = meta.getMaxValue();
			}

			final Set<ShardEntity> dutiesByShard = stageDistro.get(shard);
			final List<ShardEntity> checkingDuties = new ArrayList<>(dutiesByShard);
			if (loadStrat) {
				// order it so we can obtain sequentially granulated reductions of the transmitting shard
				Collections.sort(checkingDuties, new ShardEntity.WeightComparer());
			}
			final double totalWeight = dutiesByShard.stream().mapToDouble(i -> i.getDuty().getWeight()).sum();
			final boolean isEmisor = (!loadStrat && dutiesByShard.size() > maxValue)
					|| (loadStrat && (totalWeight) > maxValue);
			final boolean isReceptor = (!loadStrat && dutiesByShard.size() < maxValue)
					|| (loadStrat && totalWeight < maxValue);
			if (isEmisor) {
				if (!loadStrat) {
					for (int i = 0; i < dutiesByShard.size() - maxValue; i++) {
						transmitting.put(shard, checkingDuties.get(i));
					}
				} else if (loadStrat) {
					double liftAccumulated = 0;
					for (final ShardEntity takeMe : dutiesByShard) {
						double load = takeMe.getDuty().getWeight();
						if ((liftAccumulated += load) <= totalWeight - maxValue) {
							transmitting.put(shard, takeMe);
						} else {
							break; // following are bigger because sorted
						}
					}
				}
			} else if (isReceptor) {
				// until loop ends we cannot for certain which shard is receptor and can receive which duty
				spaceByReceptor.put(shard,
						new AtomicDouble(maxValue - (loadStrat ? totalWeight : dutiesByShard.size())));
			}
		}
		return transmitting;
	}
	
	private Shard makeSpaceIntoReceptors(
			final boolean loadStrat, 
			final ShardEntity duty,
			final Map<Shard, AtomicDouble> spaceByReceptor) {

		for (final Shard receptor : spaceByReceptor.keySet()) {
			AtomicDouble space = spaceByReceptor.get(receptor);
			final double newWeight = loadStrat ? duty.getDuty().getWeight() : 1;
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
			final Map<Shard, Set<ShardEntity>> stageDistro,
			final Set<ShardEntity> creations,
			final Set<ShardEntity> deletions,
			final Migrator migrator,
			final Shard receptorShard) {
		for (final Shard shard : stageDistro.keySet()) {
			if (shard != receptorShard) {
				Set<ShardEntity> dutiesByShard = stageDistro.get(shard);
				if (!dutiesByShard.isEmpty()) {
					for (final ShardEntity duty : dutiesByShard) {
						if (!deletions.contains(duty)) {
							migrator.transfer(shard, receptorShard, duty);
							logger.info("{}: Hoarding from Shard: {} to Shard: {}, Duty: {}", getClass().getSimpleName(),
								shard, receptorShard, duty);
						}
					}
				}
			}
		}
		for (ShardEntity creation: creations) {
			migrator.transfer(receptorShard, creation);
		}
	}

	private Shard getAnyReceptorShard(final Map<Shard, Set<ShardEntity>> stageDistro) {
		for (Shard shard: stageDistro.keySet()) {
			Set<ShardEntity> dutiesByShard = stageDistro.get(shard);
			if (!dutiesByShard.isEmpty()) {
				return shard;
			}
		}
		return stageDistro.keySet().iterator().next();
	}

}