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
package io.tilt.minka.core.leader;

import static io.tilt.minka.core.leader.PartitionScheme.ClusterHealth.STABLE;
import static io.tilt.minka.core.leader.PartitionScheme.ClusterHealth.UNSTABLE;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

import io.tilt.minka.api.ConsistencyException;
import io.tilt.minka.api.Duty;
import io.tilt.minka.api.Entity;
import io.tilt.minka.api.Pallet;
import io.tilt.minka.core.leader.distributor.ChangePlan;
import io.tilt.minka.domain.EntityEvent;
import io.tilt.minka.domain.NetworkShardIdentifier;
import io.tilt.minka.domain.Shard;
import io.tilt.minka.domain.Shard.ShardState;
import io.tilt.minka.domain.ShardCapacity.Capacity;
import io.tilt.minka.domain.ShardEntity;
import io.tilt.minka.domain.ShardIdentifier;
import io.tilt.minka.domain.ShardedPartition;
import io.tilt.minka.domain.EntityState;
import io.tilt.minka.utils.CollectionUtils;
import io.tilt.minka.utils.CollectionUtils.SlidingSortedSet;

/**
 * Only one modifier allowed: {@linkplain SchemeSentry} with a {@linkplain ChangePlan} after a distribution process.
 * 
 * Contains the relations between {@linkplain Shard} and {@linkplain Duty}.
 * Continuously checked truth in {@linkplain Scheme}.
 * Client CRUD requests and detected problems in {@linkplain Backstage}
 * Built at leader promotion.
 * 
 * @author Cristian Gonzalez
 * @since Dec 2, 2015	
 */
public class PartitionScheme {

	private static final Logger logger = LoggerFactory.getLogger(PartitionScheme.class);
	
	/**
	 * Sharding registry of {@linkplain Shard} and {@linkplain Duty} objects and their relations.
	 * State of continuously checked truth on duties attached to shards.
	 * any modifications and problem detection goes to {@linkplain Backstage}
	 * Only maintainer: {@linkplain SchemeSentry}
	 */
	public static class Scheme {
		
		private final Map<ShardIdentifier, Shard> shardsByID;
		private final Map<Shard, ShardedPartition> partitionsByShard;
		private final Map<String, ShardEntity> palletsById;
		
		public Scheme() {
			this.shardsByID = new HashMap<>();
			this.partitionsByShard = new HashMap<>();
			this.palletsById = new HashMap<>();
		}
		
		private boolean stealthChange;
		public void stealthChange(final boolean value) {
			this.stealthChange = value;
		}
		/** @return true when changes happened that are worthy of distribution phase run  */
		public boolean isStealthChange() {
			return this.stealthChange;
		}
		
		public void onShards(final Predicate<Shard> test, final Consumer<Shard> consumer) {
			for (Shard sh: shardsByID.values()) {
				if (test == null || test.test(sh) || test == null) {
					consumer.accept(sh);
				}
			}
		}
		public Shard findShard(final Predicate<Shard> test) {
			for (Shard sh: shardsByID.values()) {
				if (test == null || test.test(sh) || test == null) {
					return sh;
				}
			}
			return null;
		}

		/** @return true if any shard passed the predicate */
		public boolean filterShards(final Predicate<Shard> test) {
			return shardsByID.values().stream().anyMatch(test);
		}
		
		public int shardsSize(final Predicate<Shard> test) {
			return test == null
					? shardsByID.size() 
					: (int)shardsByID.values().stream().filter(test).count();
		}

		public ShardEntity getPalletById(final String id) {
			return this.palletsById.get(id);
		}
		public void onPallets(final Consumer<ShardEntity> consumer) {
			palletsById.values().forEach(consumer);
		}
		public boolean filterPallets(final Predicate<ShardEntity> test) {
			return palletsById.values().stream().anyMatch(test);
		}
		
		
		/**
		 * When a Shard has been offline and their dangling duties reassigned
		 * after completing Change's cycles: the shard must be deleted
		 * @param shard	a shard to delete from cluster
		 * @return true if the action was performed
		 */
		public boolean removeShard(final Shard shard) {
			logger.info("{}: Removing Shard {} - bye bye ! come back some day !", getClass().getSimpleName(), shard);
			final Shard rem = this.shardsByID.remove(shard.getShardID());
			final ShardedPartition part = this.partitionsByShard.remove(shard);
			if (rem == null || part == null) {
				logger.error("{}: trying to delete unexisting Shard: {}", getClass().getSimpleName(), shard);
			}
			final boolean changed = rem!=null && part!=null;
			stealthChange |= changed;
			return changed;
		}
		/**
		 * @param shard	to add to the table
		 * @return true if the action was performed
		 */
		public boolean addShard(final Shard shard) {
			if (this.shardsByID.containsKey(shard.getShardID())) {
				logger.error("{}: Inconsistency trying to add an already added Shard {}", getClass().getSimpleName(),
						shard);
				return false;
			} else {
				logger.info("{}: Adding new Shard {}", getClass().getSimpleName(), shard);
				this.shardsByID.put(shard.getShardID(), shard);
				stealthChange = true;
				return true;
			}
		}
		
		/**
		 * Account the end of the duty movement operation.
		 * Only access-point to adding and removing duties.
		 * @param duty 		the entity to act on
		 * @param where		the sard where it resides
		 * @param callback	called when writting is possible
		 * @return if there was a Scheme change after the action 
		 */
		public boolean write(final ShardEntity duty, final Shard where, final EntityEvent event, final Runnable callback) {
			final boolean add = event.is(EntityEvent.ATTACH) || event.is(EntityEvent.CREATE);
			final boolean del = !add && (event.is(EntityEvent.DETACH) || event.is(EntityEvent.REMOVE));
			final ShardedPartition part = getPartition(where);
			if (add) {
				checkDuplicationFailure(duty, where);
			} 
			if ((add && part.add(duty)) || (del && part.remove(duty))) {
				stealthChange = true; 
				if (callback!=null) {
					callback.run();
				}
				logger.info("{}: Written {} on: {} at [{}]", getClass().getSimpleName(), event.name(), duty, where);
			} else {
				throw new ConsistencyException("Attach failure. Confirmed attach/creation already exists");
			}
			return true;
		}
		
		private void checkDuplicationFailure(final ShardEntity duty, final Shard reporter) {
			for (Shard sh : partitionsByShard.keySet()) {
				if (!sh.equals(reporter) && partitionsByShard.get(sh).contains(duty)) {
					throw new ConcurrentDutyException("Duplication failure: Shard %s tries to Report Duty: %s already "
							+ "in ptable's Shard: %s", reporter, duty, sh);
				}
			}
		}
		/** 
		 * @param shard	the shard to get duties from
		 * @return a copy of duties by shard: dont change duties ! */
		public Collection<ShardEntity> getDutiesByShard(final Shard shard) {
			return getPartition(shard).getDuties();
		}

		public void onDuties(final Shard shard, final Pallet<?> pallet, final Consumer<ShardEntity> consumer) {
			for (ShardEntity e: getPartition(shard).getDuties()) {
				if (pallet==null || e.getDuty().getPalletId().equals(pallet.getId())) {
					consumer.accept(e);
				}
			}
		}
		
		public void onDuties(final Consumer<ShardEntity> consumer) {
			onDuties(null, null, consumer, null, false);
		}
		public void onDutiesByPallet(final Pallet<?> pallet, final Consumer<ShardEntity> consumer) {
			onDuties(pallet, null, consumer, null, false);
		}
		public boolean dutyExists(final ShardEntity e) {
			final boolean ret[] = new boolean[1];
			onDuties(null, null, ee-> ret[0]=true, ee-> ee.equals(e), true);
			return ret[0];
		}
		
		public boolean dutyExistsAt(final ShardEntity e, final Shard shard) {
			return getPartition(shard).getDuties().contains(e);
		}
		
		private void onDuties(final Pallet<?> pallet, 
				final ShardState state, 
				final Consumer<ShardEntity> consumer, 
				final Predicate<ShardEntity> test, 
				final boolean one) {
			for (Shard shard: shardsByID.values()) {
				if (state==null || shard.getState()==state) {
					for (final ShardEntity e: partitionsByShard.get(shard).getDuties()) {
						if (pallet == null || e.getDuty().getPalletId().equals(pallet.getId())) {
							if (test==null || test.test(e)) {
								consumer.accept(e);
								if (one) {
									return;
								}
							}
						}
					}	
				}
			}
		}
		
		private synchronized ShardedPartition getPartition(final Shard shard) {
			ShardedPartition po = this.partitionsByShard.get(shard);
			if (po == null) {
				this.partitionsByShard.put(shard, po = ShardedPartition.partitionForFollower(shard.getShardID()));
			}
			return po;
		}

		public Shard getShard(NetworkShardIdentifier id) {
			return shardsByID.get(id);
		}

		public Shard getDutyLocation(final ShardEntity se) {
			for (final Shard shard : partitionsByShard.keySet()) {
				for (ShardEntity st : partitionsByShard.get(shard).getDuties()) {
					if (st.equals(se)) {
						return shard;
					}
				}
			}
			return null;
		}
		
		public Shard getDutyLocation(final Duty<?> duty) {
			for (final Shard shard : partitionsByShard.keySet()) {
				for (ShardEntity st : partitionsByShard.get(shard).getDuties()) {
					if (st.getDuty().getId().equals(duty.getId())) {
						return shard;
					}
				}
			}
			return null;
		}

		public void filterPalletLocations(final ShardEntity pallet, final Consumer<Shard> consumer) {
			for (final Shard shard : partitionsByShard.keySet()) {
				for (ShardEntity pp : partitionsByShard.get(shard).getPallets()) {
					if (pp.equals(pallet)) {
						consumer.accept(shard);
						break;
					}
				}
			}
		}
		
		public int shardsSize() {
			return this.shardsByID.values().size();
		}

		/** Read-only access */
		public static class SchemeExtractor {
			private final Scheme reference;

			public SchemeExtractor(final Scheme reference) {
				this.reference = reference;
			}

			public double getCapacity(final Pallet<?> pallet, final Shard quest) {
				double total = 0;
				for (final Shard shard : getShards()) {
					if (quest == null || shard.equals(quest)) {
						final Capacity cap = shard.getCapacities().get(pallet);
						total += cap != null ? cap.getTotal() : 0;
					}
				}
				return total;
			}

			public double getCapacityTotal(final Pallet<?> pallet) {
				return getCapacity(pallet, null);
			}

			public int getSizeTotal() {
				return getSize(null, null);
			}

			public int getSizeTotal(final Pallet<?> pallet) {
				return getSize(pallet, null);
			}

			public int getSize(final Pallet<?> pallet, final Shard quest) {
				int total = 0;
				for (final ShardedPartition part : reference.partitionsByShard.values()) {
					if (quest == null || part.getId().equals(quest.getShardID())) {
						total += (pallet == null ? part.getDuties().size() : part.getDutiesSize(pallet));
					}
				}
				return total;
			}

			public double getWeightTotal(final Pallet<?> pallet) {
				return getWeight(pallet, null);
			}

			public double getWeight(final Pallet<?> pallet, final Shard quest) {
				int total = 0;
				for (final ShardedPartition part : reference.partitionsByShard.values()) {
					if (quest == null || part.getId().equals(quest.getShardID())) {
						total += part.getWeight(pallet);
					}
				}
				return total;
			}

			public Collection<ShardEntity> getPallets() {
				return reference.palletsById.values();
			}

			public Collection<Shard> getShards() {
				return reference.shardsByID.values();
			}

			public int getAccountConfirmed(final Pallet<?> filter) {
				int total = 0;
				for (Shard shard : reference.partitionsByShard.keySet()) {
					if (shard.getState() == ShardState.ONLINE) {
						total += reference.partitionsByShard.get(shard).getDutiesSize(filter);
					}
				}
				return total;
			}
		}
	
	}
	
	/** 
	 * temporal state of modifications willing to be added to the scheme 
	 * including inconsistencies detected by the bookkeeper
	 * */
	public static class Backstage {
		private Map<Pallet<?>, ShardEntity> palletCrud;
		private Map<Duty<?>, ShardEntity> dutyCrud;
		private Map<Duty<?>, ShardEntity> dutyMissings;
		private Map<Duty<?>, ShardEntity> dutyDangling;
		
		public Backstage() {
			this.palletCrud = new HashMap<>();
			this.dutyCrud = new HashMap<>();
			this.dutyMissings = new HashMap<>();
			this.dutyDangling = new HashMap<>();
		}

		private boolean stealthChange;
		public void stealthChange(final boolean value) {
			this.stealthChange = value;
		}
		/** @return true when changes happened that are worthy of distribution phase run  */
		public boolean isStealthChange() {
			return this.stealthChange;
		}
		
		/** @return read only set */
		public Collection<ShardEntity> getDutiesDangling() {
			return Collections.unmodifiableCollection(this.dutyDangling.values());
		}
		
		/** @return true if added for the first time else is being replaced */
		public boolean addDangling(final Collection<ShardEntity> dangling) {
			boolean added = false; 
			for (ShardEntity d: dangling) {
				added |= this.dutyDangling.put(d.getDuty(), d) == null;
			}
			stealthChange |= added;
			return added;
		}
		public boolean addDangling(final ShardEntity dangling) {
			final boolean added = this.dutyDangling.put(dangling.getDuty(), dangling) == null;
			stealthChange |= added;
			return added;

		}
		public void cleanAllocatedDanglings() {
			removeNonStuck(dutyDangling);
		}
		private void removeNonStuck(final Map<? extends Entity, ShardEntity> map) {
			final Iterator<? extends Entity> it = map.keySet().iterator();
			while (it.hasNext()) {
				final ShardEntity e = map.get(it.next());
				if (e.getLastState() != EntityState.STUCK) {
					stealthChange |=true;
					it.remove();
				}
			}
		}

		public int accountCrudDuties() {
			return this.dutyCrud.size();
		}

		/* add it for the next Distribution cycle consideration */
		/** @return true if added for the first time else is being replaced */
		public boolean addCrudDuty(final ShardEntity duty) {
			// the uniqueness of it's wrapped object doesnt define the uniqueness of the wrapper
			// updates and transfer go in their own manner
			//dutyCrud.remove(duty);
			final boolean added = dutyCrud.put(duty.getDuty(), duty) == null;
			stealthChange |= added;
			return added;

		}
		public Collection<ShardEntity> getDutiesCrud() {
			return Collections.unmodifiableCollection(this.dutyCrud.values());
		}
		public boolean removeCrud(final ShardEntity entity) {
			final boolean removed =this.dutyCrud.remove(entity.getDuty()) == null;
			stealthChange |= removed;
			return removed;
		}

		/** @return read only set */
		public Collection<ShardEntity> getDutiesMissing() {
			return Collections.unmodifiableCollection(this.dutyMissings.values());
		}
		public void clearAllocatedMissing() {
			removeNonStuck(dutyMissings);
		}
		
		public boolean addMissing(final Collection<ShardEntity> duties) {
			boolean added = false;
			for (ShardEntity d: duties) {
				added |= dutyMissings.put(d.getDuty(), d) == null;
			}
			stealthChange |= added;
			return true;

		}

		public boolean addMissing(final ShardEntity duty) {
			final boolean replaced = this.dutyMissings.put(duty.getDuty(), duty) !=null;
			stealthChange |= replaced;
			return true;
		}

		public void removeCrudDuties() {
			this.dutyCrud.clear();
			this.dutyCrud = new HashMap<>();
			stealthChange = true;
		}
		public int sizeDutiesCrud(final Predicate<EntityEvent> event, final Predicate<EntityState> state) {
			final int[] size = new int[1];
			onEntitiesCrud(ShardEntity.Type.DUTY, event, state, e->size[0]++);
			return size[0];
		}
		public void onDutiesCrud(
				final Predicate<EntityEvent> event, 
				final Predicate<EntityState> state, 
				final Consumer<ShardEntity> consumer) {
			onEntitiesCrud(ShardEntity.Type.DUTY, event, state, consumer);
		}
		public void onPalletsCrud(
				final Predicate<EntityEvent> event, 
				final Predicate<EntityState> state, 
				final Consumer<ShardEntity> consumer) {
			onEntitiesCrud(ShardEntity.Type.PALLET, event, state, consumer);
		}
		private void onEntitiesCrud(
				final ShardEntity.Type type, 
				final Predicate<EntityEvent> eventPredicate, 
				final Predicate<EntityState> statePredicate, 
				final Consumer<ShardEntity> consumer) {
			
			
			(type == ShardEntity.Type.DUTY ? getDutiesCrud() : palletCrud.values()).stream()
				.filter(e -> (eventPredicate == null || eventPredicate.test(e.getJournal().getLast().getEvent())) 
					&& (statePredicate == null || (statePredicate.test(e.getJournal().getLast().getLastState()))))
				.forEach(consumer);
		}

	}
	
	private ClusterHealth visibilityHealth;
	private ClusterHealth distributionHealth;
	private ClusterCapacity capacity;
	private final Scheme scheme;
	private final Backstage backstage;
	private ChangePlan currentPlan;
	private SlidingSortedSet<ChangePlan> history;
	
	/**
	 * status for the cluster taken as Avg. for the last 5 cycles
	 * when this happens the cluster rebalances workload 
	 * by making shards exchange duties  
	 */
	public enum ClusterHealth {
		/* visibility: if there was no shard changing status working: theere's
		 * work-load balance among the nodes */
		STABLE,
		/* visibility: if there was at least one shard changing status that
		 * will provoke a duty reallocation working: there's a reallocation in progress */
		UNSTABLE,;
	}

	/** the shard's total reported capacities for all pallets */ 
	public enum ClusterCapacity {
		/* no weighting duties loaded */
		IDLE, 
		/* good: yet enough capacity for more duties of avg. weight */
		NORMAL,
		/* warning: no more capacity for average duty weight */
		FRAGILE,
		/* bad: dutie's total weight bigger than shard's total capacity, 
		 * some duties are not being distributed !! */
		INSUFFICIENT,
	}

	public PartitionScheme() {
		this.visibilityHealth = ClusterHealth.STABLE;
		this.distributionHealth = ClusterHealth.STABLE;
		this.capacity = ClusterCapacity.IDLE;
		this.scheme = new Scheme();
		this.backstage = new Backstage();
		this.history = CollectionUtils.sliding(20);
	}
		
	public Set<ChangePlan> getHistory() {
		return this.history.values();
	}

	public ChangePlan getCurrentPlan() {
		return this.currentPlan;
	}

	public void addPlan(final ChangePlan change) {
		this.currentPlan = change;
		this.history.add(change);
	}

	
	public Backstage getBackstage() {
		return this.backstage;
	}
	
	public Scheme getScheme() {
		return this.scheme;
	}

	public ClusterHealth getHealth() {
		return this.distributionHealth == visibilityHealth 
				&& distributionHealth == STABLE ? STABLE : UNSTABLE;
	}

	public ClusterHealth getDistributionHealth() {
		return this.distributionHealth;
	}

	public void setDistributionHealth(ClusterHealth distributingHealth) {
		this.distributionHealth = distributingHealth;
	}

	public ClusterHealth getShardsHealth() {
		return this.visibilityHealth;
	}

	public void setShardsHealth(ClusterHealth health) {
		this.visibilityHealth = health;
	}

	public ClusterCapacity getCapacity() {
		return this.capacity;
	}

	public void setCapacity(ClusterCapacity capacity) {
		this.capacity = capacity;
	}


	private StringBuilder buildLogForDuties(final List<ShardEntity> sorted) {
		final StringBuilder sb = new StringBuilder();
		if (!sorted.isEmpty()) {
			sorted.sort(sorted.get(0));
		}
		sorted.forEach(i -> sb.append(i.getEntity().getId()).append(", "));
		return sb;
	}

	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder()
				.append("Shards: ")
				.append(getScheme().shardsByID.size())
				.append(" Crud Duties: ")
				.append(getBackstage().dutyCrud.size());
		//.append(" Change: ").append(change.getGroupedIssues().size());
		return sb.toString();
	}


	/* add without considerations (they're staged but not distributed per se) */
	public void addCrudPallet(final ShardEntity pallet) {
		getScheme().palletsById.put(pallet.getPallet().getId(), pallet);
		getBackstage().palletCrud.put(pallet.getPallet(), pallet);
	}

	public void logStatus() {
		if (getScheme().shardsByID.isEmpty()) {
			logger.warn("{}: Status without Shards", getClass().getSimpleName());
		} else {
			if (!logger.isInfoEnabled()) {
				return;
			}
			for (final Shard shard : getScheme().shardsByID.values()) {
				final ShardedPartition partition = getScheme().partitionsByShard.get(shard);
				final Map<Pallet<?>, Capacity> capacities = shard.getCapacities();
				if (partition == null) {
					logger.info("{}: {} = Empty", getClass().getSimpleName(), shard);
				} else {
					for (ShardEntity p: getScheme().palletsById.values()) {
						final StringBuilder sb = new StringBuilder();
						sb.append(shard).append(" Pallet: ").append(p.getPallet().getId());
						final Capacity cap = capacities.get(p.getPallet());
						sb.append(" Size: ").append(partition.getDutiesSize(p.getPallet()));
						sb.append(" Weight/Capacity: ");
						final double[] weight = new double[1];
						partition.getDuties().stream()
							.filter(d->d.getDuty().getPalletId().equals(p.getPallet().getId()))
							.forEach(d->weight[0]+=d.getDuty().getWeight());
						sb.append(weight[0]).append("/");
						sb.append(cap!=null ? cap.getTotal(): "Unreported");
						if (logger.isDebugEnabled()) {
							sb.append(" Duties: [").append(partition.toString()).append("]");
						}
						logger.info("{}: {}", getClass().getSimpleName(), sb.toString());
					}
				}
			}
		}
		if (!this.getBackstage().dutyMissings.isEmpty()) {
			logger.warn("{}: {} Missing duties: [ {}]", getClass().getSimpleName(), getBackstage().dutyMissings.size(),
					this.getBackstage().dutyMissings.toString());
		}
		if (!this.getBackstage().dutyDangling.isEmpty()) {
			logger.warn("{}: {} Dangling duties: [ {}]", getClass().getSimpleName(), getBackstage().dutyDangling.size(),
					this.getBackstage().dutyDangling.toString());
		}
		if (this.getBackstage().dutyCrud.isEmpty()) {
			logger.info("{}: no CRUD duties", getClass().getSimpleName());
		} else {
			logger.info("{}: with {} CRUD duties: [ {}]", getClass().getSimpleName(), getBackstage().dutyCrud.size(),
					buildLogForDuties(Lists.newArrayList(getBackstage().getDutiesCrud())));
		}
		logger.info("{}: Health: {}", getClass().getSimpleName(), getDistributionHealth());
	}

}
