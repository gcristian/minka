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

import static io.tilt.minka.core.leader.ShardingScheme.ClusterHealth.STABLE;
import static io.tilt.minka.core.leader.ShardingScheme.ClusterHealth.UNSTABLE;

import java.io.Serializable;
import java.time.Instant;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Predicate;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.tilt.minka.api.ConsistencyException;
import io.tilt.minka.api.Duty;
import io.tilt.minka.api.Entity;
import io.tilt.minka.api.Pallet;
import io.tilt.minka.core.leader.distributor.ChangePlan;
import io.tilt.minka.domain.EntityEvent;
import io.tilt.minka.domain.EntityJournal.Log;
import io.tilt.minka.domain.EntityState;
import io.tilt.minka.domain.NetworkShardIdentifier;
import io.tilt.minka.domain.Shard;
import io.tilt.minka.domain.Shard.Change;
import io.tilt.minka.domain.Shard.ShardState;
import io.tilt.minka.domain.ShardCapacity.Capacity;
import io.tilt.minka.domain.ShardEntity;
import io.tilt.minka.domain.ShardIdentifier;
import io.tilt.minka.domain.ShardedPartition;
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
public class ShardingScheme {

	private static final Logger logger = LoggerFactory.getLogger(Scheme.class);
	
	/**
	 * Repr. of distribution scheme after proper confirmation of the followers 
	 * Holds the sharding registry of duties.
	 * Only maintainer: {@linkplain SchemeSentry}
	 */
	public static class Scheme {
	
		private static final Logger logger = LoggerFactory.getLogger(Scheme.class);

		private final Map<ShardIdentifier, Shard> shardsByID;
		private final Map<Shard, ShardedPartition> partitionsByShard;
		private final Map<String, ShardEntity> palletsById;
		private final Map<ShardIdentifier, SlidingSortedSet<Shard.Change>> goneShards;
		
		public Scheme() {
			this.goneShards = new HashMap<>();
			this.shardsByID = new HashMap<>();
			this.partitionsByShard = new HashMap<>();
			this.palletsById = new HashMap<>();
		}
		
		private boolean stealthChange;
		public void stealthChange(final boolean value) {
			this.stealthChange = value;
		}
		/** @return true when the scheme has changes worthy of distribution phase run  */
		public boolean isStealthChange() {
			return this.stealthChange;
		}
		
		public void findShards(final Predicate<Shard> test, final Consumer<Shard> consumer) {
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
		public void findPallets(final Consumer<ShardEntity> consumer) {
			palletsById.values().forEach(consumer);
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
			addGoneShard(shard);
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
		
		/** backup gone shards for history */
		private void addGoneShard(final Shard shard) {
			int max = 5;
			for (final Iterator<Change> it = shard.getChanges().iterator(); it.hasNext() && max>0;max--) {
				SlidingSortedSet<Change> set = goneShards.get(shard.getShardID());
				if (set==null) {
					goneShards.put(shard.getShardID(), set = CollectionUtils.sliding(max));
				}
				set.add(it.next());
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
				if (logger.isInfoEnabled()) {
					logger.info("{}: Written {} on: {} at [{}]", getClass().getSimpleName(), event.name(), duty, where);
				}
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

		public void findDuties(final Shard shard, final Pallet<?> pallet, final Consumer<ShardEntity> consumer) {
			for (ShardEntity e: getPartition(shard).getDuties()) {
				if (pallet==null || e.getDuty().getPalletId().equals(pallet.getId())) {
					consumer.accept(e);
				}
			}
		}
		
		public ShardEntity getByDuty(final Duty<?> duty) {
			ShardEntity ret = null;
			for (final Shard shard : partitionsByShard.keySet()) {
				ret = partitionsByShard.get(shard).getByDuty(duty);
				if (ret!=null) {
					break;
				}
			}
			return ret;
		}
		
		public void findDuties(final Consumer<ShardEntity> consumer) {
			onDuties(null, null, consumer, null, false);
		}
		public void findDutiesByPallet(final Pallet<?> pallet, final Consumer<ShardEntity> consumer) {
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

		public Shard findDutyLocation(final ShardEntity se) {
			for (final Shard shard : partitionsByShard.keySet()) {
				for (ShardEntity st : partitionsByShard.get(shard).getDuties()) {
					if (st.equals(se)) {
						return shard;
					}
				}
			}
			return null;
		}
		
		public Shard findDutyLocation(final Duty<?> duty) {
			for (final Shard shard : partitionsByShard.keySet()) {
				final ShardEntity st = partitionsByShard.get(shard).getByDuty(duty);
				if (st!=null) {
					return shard;
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

		public Map<ShardIdentifier, SlidingSortedSet<Shard.Change>> getGoneShards() {
			return goneShards;
		}
		
		public void logStatus() {
			if (shardsByID.isEmpty()) {
				logger.warn("{}: Status without Shards", getClass().getSimpleName());
			} else {
				if (!logger.isInfoEnabled()) {
					return;
				}
				for (final Shard shard : shardsByID.values()) {
					final ShardedPartition partition = partitionsByShard.get(shard);
					final Map<Pallet<?>, Capacity> capacities = shard.getCapacities();
					if (partition == null) {
						logger.info("{}: {} = Empty", getClass().getSimpleName(), shard);
					} else {
						for (ShardEntity p: palletsById.values()) {
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
	 * Temporal state of modifications willing to be added to the {@linkplain Scheme}
	 * including inconsistencies detected by the sentry
	 * Only maintainers: {@linkplain SchemeSentry} and {@linkplain SchemeRepository}
	 * */
	public static class Backstage {

		private static final Logger logger = LoggerFactory.getLogger(Backstage.class);

	    // creations and removes willing to be attached or detached to/from shards.
		private Map<Pallet<?>, ShardEntity> palletCrud;
		private Map<Duty<?>, ShardEntity> dutyCrud;
		// absences in shards's reports
		private Map<Duty<?>, ShardEntity> dutyMissings;
		// fallen shards's duties
		private Map<Duty<?>, ShardEntity> dutyDangling;
		private Instant snaptake;
		
		// read-only snapshot for ChangePlanBuilder thread (not to be modified, backstage remains MASTER)
		private Backstage snapshot;
		
		/** @return a frozen state of Backstage, so message-events threads 
		 * (threadpool-size independently) can still modify the instance for further change plans */
		public synchronized Backstage snapshot() {
			if (snapshot==null) {
				final Backstage tmp = new Backstage();
				tmp.dutyCrud = new HashMap<>(this.dutyCrud);
				tmp.dutyDangling = new HashMap<>(this.dutyDangling);
				tmp.dutyMissings = new HashMap<>(this.dutyMissings);
				tmp.palletCrud = new HashMap<>(this.palletCrud);
				tmp.snaptake = Instant.now();
				this.snapshot = tmp;
			}
			return snapshot;
		}
		
		public void dropSnapshot() {
			this.snapshot = null;
		}
		
		/** @return true if the last entity event ocurred before the last snapshot creation */
		public boolean after(final ShardEntity e) {
			if (snaptake==null) {
				throw new RuntimeException("bad call");
			}
			final Log last = e.getJournal().getLast();
			return last==null || snaptake.toEpochMilli() >=last.getHead().getTime();
		}

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
		/** @return true when the backstage has changes worthy of distribution phase run  */
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
			if (snapshot!=null && !dutyDangling.isEmpty()) {
				remove(snapshot.dutyDangling, dutyDangling, s->s.getLastState() != EntityState.STUCK);
			}
		}
		private void remove(
				final Map<? extends Entity<? extends Serializable>, ShardEntity> deletes, 
				final Map<? extends Entity<? extends Serializable>, ShardEntity> target, 
				final Predicate<ShardEntity> test) {
			for (Map.Entry<? extends Entity<? extends Serializable>, ShardEntity> e: deletes.entrySet()) {
				if (test.test(e.getValue())) {
					target.remove(e.getKey());
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
			final boolean added = dutyCrud.put(duty.getDuty(), duty) == null;
			stealthChange |= added;
			return added;

		}
		public Collection<ShardEntity> getDutiesCrud() {
			return Collections.unmodifiableCollection(this.dutyCrud.values());
		}
		public boolean removeCrud(final ShardEntity entity) {
			boolean removed = false;
			final ShardEntity candidate = dutyCrud.get(entity.getDuty());
			// consistency check: only if they're the same action 
			if (candidate!=null) {// && candidate.getLastEvent()==entity.getLastEvent().getRootCause()) {
				removed =this.dutyCrud.remove(entity.getDuty()) == null;
				stealthChange |= removed;
			}
			return removed;
		}

		/** @return read only set */
		public Collection<ShardEntity> getDutiesMissing() {
			return Collections.unmodifiableCollection(this.dutyMissings.values());
		}
		
		public void clearAllocatedMissing() {
			if (snapshot!=null && !dutyMissings.isEmpty()) {
				remove(snapshot.dutyMissings, dutyMissings, s->s.getLastState() != EntityState.STUCK);
			}
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
		public void findDutiesCrud(
				final Predicate<EntityEvent> event, 
				final Predicate<EntityState> state, 
				final Consumer<ShardEntity> consumer) {
			onEntitiesCrud(ShardEntity.Type.DUTY, event, state, consumer);
		}
		public void findPalletsCrud(
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
			(type == ShardEntity.Type.DUTY ? getDutiesCrud() : palletCrud.values())
				.stream()
				.filter(e -> (eventPredicate == null || eventPredicate.test(e.getJournal().getLast().getEvent())) 
					&& (statePredicate == null || (statePredicate.test(e.getJournal().getLast().getLastState()))))
				.forEach(consumer);
		}
		public ShardEntity getCrudByDuty(final Duty<?> duty) {
			return this.dutyCrud.get(duty);
		}

		public void logStatus() {
			if (!dutyMissings.isEmpty()) {
				logger.warn("{}: {} Missing duties: [ {}]", getClass().getSimpleName(), dutyMissings.size(),
						dutyMissings.toString());
			}
			if (!dutyDangling.isEmpty()) {
				logger.warn("{}: {} Dangling duties: [ {}]", getClass().getSimpleName(), dutyDangling.size(),
						dutyDangling.toString());
			}
			if (dutyCrud.isEmpty()) {
				logger.info("{}: no CRUD duties", getClass().getSimpleName());
			} else {
				logger.info("{}: with {} CRUD duties: [ {}]", getClass().getSimpleName(), dutyCrud.size(),
						buildLogForDuties(Lists.newArrayList(getDutiesCrud())));
			}
		}
		
		private StringBuilder buildLogForDuties(final List<ShardEntity> sorted) {
			final StringBuilder sb = new StringBuilder();
			if (!sorted.isEmpty()) {
				sorted.sort(sorted.get(0));
			}
			sorted.forEach(i -> sb.append(i.getEntity().getId()).append(", "));
			return sb;
		}

	}
	
	private ClusterHealth visibilityHealth;
	private ClusterHealth distributionHealth;
	
	private final Scheme scheme;
	private final Backstage backstage;
	private ChangePlan currentPlan;
	private List<Runnable> observers;
	
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

	public ShardingScheme() {
		this.visibilityHealth = ClusterHealth.STABLE;
		this.distributionHealth = ClusterHealth.STABLE;
		this.scheme = new Scheme();
		this.backstage = new Backstage();
	}
	
	public ChangePlan getCurrentPlan() {
		return this.currentPlan;
	}

	public void setPlan(final ChangePlan change) {
		this.currentPlan = change;
		notifyObservers();
	}
	
	private void notifyObservers() {
		if (observers!=null) {
			for (Runnable run: observers) {
				try {
					run.run();
				} catch (Exception e) {
				}
			}
		}
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

	public void logStatus() {
		getScheme().logStatus();
		getBackstage().logStatus();
		logger.info("{}: Health: {}", getClass().getSimpleName(), getDistributionHealth());
	}

	/** 
	 * add without considerations (they're staged but not distributed per se)
	 * @return TRUE if the operation is done for the first time 
	 */
	public boolean addCrudPallet(final ShardEntity pallet) {
		final boolean schematized = getScheme().palletsById.put(pallet.getPallet().getId(), pallet)==null;
		final boolean staged = getBackstage().palletCrud.put(pallet.getPallet(), pallet)==null;
		return schematized && staged;
	}

	public void addChangeObserver(final Runnable observer) {
		if (observers==null) {
			this.observers = new LinkedList<>();
		}
		observers.add(observer);
	}


}
