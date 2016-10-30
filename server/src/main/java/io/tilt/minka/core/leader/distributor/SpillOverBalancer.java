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

import java.util.ArrayList;
import java.util.Arrays;
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
import io.tilt.minka.core.leader.PartitionTable;
import io.tilt.minka.domain.EntityEvent;
import io.tilt.minka.domain.Shard;
import io.tilt.minka.domain.Shard.CapacityComparer;
import io.tilt.minka.domain.Shard.DateComparer;
import io.tilt.minka.domain.ShardCapacity.Capacity;
import io.tilt.minka.domain.ShardEntity;
import io.tilt.minka.domain.ShardEntity.State;
import io.tilt.minka.domain.ShardEntity.StuckCause;
import io.tilt.minka.utils.CircularCollection;

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
	
	/* Sort criteria to select which shards are fully filled first */ 
	public enum ShardPresort {
		/* use date of shard's creation or online mark after being offline */
		BY_CREATION_DATE,
		/* use their reported for the specified pallet */
		BY_WEIGHT_CAPACITY
	}
	
	public enum MaxUnit {
		/* Use the Max value to compare the sum of all running duties's weights 
		 * and restrict new additions over a full shard */
		DUTY_WEIGHT,
		/* Use the Max value as max number of duties to fit in one shard */
		DUTY_SIZE,
		/* Ignore given value and use Shard's reported capacity as max value */
		USE_CAPACITY,
	}


	@Override
	public final void balance(final Pallet<?> pallet, final PartitionTable table, final Reallocation realloc,
			final List<Shard> onlineShards, final Set<ShardEntity> creations, final Set<ShardEntity> deletions) {

		final Metadata meta = (Metadata)pallet.getStrategy();
		if (meta.getMaxValue() < 0) {
			final Shard receptorShard = getAnyReceptorShard(pallet, table, onlineShards);
			logger.info("{}: For Unbounded duties (max = -1) found Shard receptor: {}", getClass().getSimpleName(),
					receptorShard);
			//creations.addAll(dangling);
			registerMigrationFromOthersToOne(pallet, table, realloc, receptorShard);
			Arranger.registerCreations(creations, realloc, new CircularCollection<>(Arrays.asList(receptorShard)));
		} else {
			logger.info("{}: Computing Spilling strategy: {} with a Max Value: {}", getClass().getSimpleName(), meta.getMaxUnit(), 
				meta.getMaxUnit() == MaxUnit.USE_CAPACITY ? "{shard's capacity}" : meta.getMaxValue());
			
			boolean loadStrat = meta.getMaxUnit() == MaxUnit.DUTY_WEIGHT || meta.getMaxUnit() == MaxUnit.USE_CAPACITY;
			final Map<Shard, AtomicDouble> spaceByReceptor = new HashMap<>();
			final SetMultimap<Shard, ShardEntity> trans = collectTransceivers(pallet, table, loadStrat, spaceByReceptor, meta);
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
					Collections.sort(dutiesForBalance, Collections.reverseOrder(new Duty.WeightComparer()));
				}
				registerNewcomers(realloc, loadStrat, spaceByReceptor, unfitting, dutiesForBalance);
				// then move those surplussing
				registerMigrations(realloc, loadStrat, spaceByReceptor, unfitting, trans);
				for (ShardEntity unfit : unfitting) {
					logger.error("{}: Add Shards !! No receptor has space for Duty: {}", getClass().getSimpleName(), unfit);
				}
			}
		}
	}

	private void registerNewcomers(final Reallocation realloc, final boolean loadStrat,
			final Map<Shard, AtomicDouble> spaceByReceptor, final List<ShardEntity> unfitting,
			final List<ShardEntity> dutiesForBalance) {

		for (final ShardEntity newcomer : dutiesForBalance) {
			final Shard receptor = makeSpaceIntoReceptors(loadStrat, newcomer, spaceByReceptor);
			if (receptor != null) {
				newcomer.registerEvent(EntityEvent.ATTACH, State.PREPARED);
				realloc.addChange(receptor, newcomer);
				logger.info("{}: Assigning to Shard: {} (space left: {}), Duty: {}", getClass().getSimpleName(),
						receptor.getShardID(), spaceByReceptor.get(receptor), newcomer.toBrief());
			} else {
				newcomer.registerEvent(EntityEvent.ATTACH, State.STUCK);
				newcomer.setStuckCause(StuckCause.UNSUITABLE);
				realloc.getProblems().put(null, newcomer);
				unfitting.add(newcomer);
			}
		}
	}

	private void registerMigrations(final Reallocation realloc, final boolean loadStrat,
			final Map<Shard, AtomicDouble> spaceByReceptor, final List<ShardEntity> unfitting,
			final SetMultimap<Shard, ShardEntity> trans) {

		for (final Shard emisor : trans.keys()) {
			final Set<ShardEntity> emitting = trans.get(emisor);
			for (final ShardEntity emitted : emitting) {
				final Shard receptor = makeSpaceIntoReceptors(loadStrat, emitted, spaceByReceptor);
				if (receptor == null) {
					emitted.registerEvent(EntityEvent.ATTACH, State.STUCK);
					emitted.setStuckCause(StuckCause.UNSUITABLE);
					realloc.getProblems().put(null, emitted);
					unfitting.add(emitted);
				} else {
					final ShardEntity copy = addMigrationChange(realloc, receptor, emisor, emitted);
					logger.info("{}: Migrating from: {} to {}: Duty: {}", getClass().getSimpleName(), emisor, receptor,
							copy);
				}
			}
		}
	}

	private Shard makeSpaceIntoReceptors(final boolean loadStrat, final ShardEntity duty,
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

	/* elect duties from emisors and compute receiving size at receptors */
	private SetMultimap<Shard, ShardEntity> collectTransceivers(final Pallet<?> pallet, final PartitionTable table,
			boolean loadStrat, final Map<Shard, AtomicDouble> spaceByReceptor, final Metadata meta) {

		final SetMultimap<Shard, ShardEntity> transmitting = HashMultimap.create();
		
		final List<Shard> sortedShards = new ArrayList<>(table.getAllImmutable());
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

			
			final Set<ShardEntity> dutiesByShard = table.getDutiesByShard(pallet, shard);
			final List<ShardEntity> checkingDuties = new ArrayList<>(dutiesByShard);
			if (loadStrat) {
				// order it so we can obtain sequentially granulated reductions of the transmitting shard
				Collections.sort(checkingDuties, new Duty.WeightComparer());
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
					long liftAccumulated = 0;
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

	private void registerMigrationFromOthersToOne(final Pallet<?> pallet, final PartitionTable table,
			final Reallocation realloc, final Shard receptorShard) {

		for (final Shard shard : table.getAllImmutable()) {
			if (shard != receptorShard) {
				Set<ShardEntity> dutiesByShard = table.getDutiesByShard(pallet, shard);
				if (!dutiesByShard.isEmpty()) {
					for (final ShardEntity duty : dutiesByShard) {
						final ShardEntity copy = addMigrationChange(realloc, receptorShard, shard, duty);
						logger.info("{}: Hoarding from Shard: {} to Shard: {}, Duty: {}", getClass().getSimpleName(),
								shard, receptorShard, copy);
					}
				}
			}
		}
	}

	/* return new added */
	private ShardEntity addMigrationChange(final Reallocation realloc, final Shard receptorShard,
			final Shard emisorShard, final ShardEntity duty) {

		duty.registerEvent(EntityEvent.ATTACH, State.PREPARED);
		realloc.addChange(receptorShard, duty);
		ShardEntity copy = ShardEntity.copy(duty);
		copy.registerEvent(EntityEvent.DETACH, State.PREPARED);
		realloc.addChange(emisorShard, copy);
		return copy;
	}

	private Shard getAnyReceptorShard(final Pallet<?> pallet, final PartitionTable table,
			final List<Shard> onlineShards) {
		for (Shard online : onlineShards) {
			Set<ShardEntity> dutiesByShard = table.getDutiesByShard(pallet, online);
			if (!dutiesByShard.isEmpty()) {
				return online;
			}
		}
		return onlineShards.iterator().next();
	}

}