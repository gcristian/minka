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

import static io.tilt.minka.core.leader.PartitionTable.ClusterHealth.STABLE;
import static io.tilt.minka.core.leader.PartitionTable.ClusterHealth.UNSTABLE;
import static io.tilt.minka.domain.ShardState.ONLINE;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.util.concurrent.AtomicDouble;

import io.tilt.minka.api.Duty;
import io.tilt.minka.api.Pallet;
import io.tilt.minka.domain.AttachedPartition;
import io.tilt.minka.domain.EntityEvent;
import io.tilt.minka.domain.NetworkShardID;
import io.tilt.minka.domain.Shard;
import io.tilt.minka.domain.ShardCapacity.Capacity;
import io.tilt.minka.domain.ShardEntity;
import io.tilt.minka.domain.ShardEntity.State;
import io.tilt.minka.domain.ShardID;
import io.tilt.minka.domain.ShardState;
import jersey.repackaged.com.google.common.collect.Sets;

/**
 * Only one modifier allowed: {@linkplain Auditor} with a {@linkplain Roadmap} after a distribution process.
 * 
 * Contains the relations between {@linkplain Shard} and {@linkplain Duty}.
 * Continuously checked truth in {@linkplain Stage}.
 * Client CRUD requests and detected problems in {@linkplain NextStage}
 * Built at leader promotion.
 * 
 * @author Cristian Gonzalez
 * @since Dec 2, 2015<	
 */
public class PartitionTable {

	private static final Logger logger = LoggerFactory.getLogger(PartitionTable.class);

	/**
	 * the result of a {@linkplain Distributor} phase run. 
	 * state of continuously checked truth on duties attached to shards.
	 * any modifications and problem detection goes to {@linkplain NextStage}
	 */
	public static class Stage {
		
		private final Map<ShardID, Shard> shardsByID;
		private final Map<Shard, AttachedPartition> partitionsByShard;
		private final Map<String, ShardEntity> palletsById;
		
		public Stage() {
			this.shardsByID = new HashMap<>();
			this.partitionsByShard = new HashMap<>();
			this.palletsById = new HashMap<>();
		}
		
		public double getCapacityTotal(final Pallet<?> pallet) {
			return getCapacity(pallet, null);
		}
		public double getCapacity(final Pallet<?> pallet, final Shard quest) {
			double total = 0;
			for (final Shard shard: getShards()) {
				if (quest==null || shard.equals(quest)) {
					final Capacity cap = shard.getCapacities().get(pallet);
					total+=cap!=null ? cap.getTotal() : 0;
				}
			}
			return total;
		}
		
		public int getSizeTotal(final Pallet<?> pallet) {
			return getSize(pallet, null);
		}
		public int getSize(final Pallet<?> pallet, final Shard quest) {
			int total = 0;
			for (final AttachedPartition part: partitionsByShard.values()) {
				if (quest==null || part.getId().equals(quest.getShardID())) {
					total +=part.getDuties(pallet).size();
				}
			}
			return total;
		}
		
		public double getWeightTotal(final Pallet<?> pallet) {
			return getWeight(pallet, null);
		}
		public double getWeight(final Pallet<?> pallet, final Shard quest) {
			int total = 0;
			for (final AttachedPartition part: partitionsByShard.values()) {
				if (quest==null || part.getId().equals(quest.getShardID())) {
					total +=part.getDuties(pallet).size();
				}
			}
			return total;
		}
		
		/** @return  shards by state filter or all if null */
		public List<Shard> getShardsByState(final ShardState filter) {
			return shardsByID.values().stream().filter(i -> i.getState() == filter || filter == null)
					.collect(Collectors.toList());
		}
		public ShardEntity getPalletById(final String id) {
			return this.palletsById.get(id);
		}
		public Set<ShardEntity> getPallets() {
			return Collections.unmodifiableSet(new HashSet<>(this.palletsById.values()));
		}
		/**
		 * When a Shard has been offline and their dangling duties reassigned
		 * after completing Change's cycles: the shard must be deleted
		 * @param shard
		 */
		public void removeShard(final Shard shard) {
			logger.info("{}: Removing Shard {} - bye bye ! come back some day !", getClass().getSimpleName(), shard);
			final Shard rem = this.shardsByID.remove(shard.getShardID());
			final AttachedPartition part = this.partitionsByShard.remove(shard);
			if (rem == null || part == null) {
				logger.error("{}: trying to delete unexisting Shard: {}", getClass().getSimpleName(), shard);
			}
		}
		public void addShard(final Shard shard) {
			if (this.shardsByID.containsKey(shard.getShardID())) {
				logger.error("{}: Inconsistency trying to add an already added Shard {}", getClass().getSimpleName(),
						shard);
			} else {
				logger.info("{}: Adding new Shard {}", getClass().getSimpleName(), shard);
				this.shardsByID.put(shard.getShardID(), shard);
			}
		}
		/** confirmation after reallocation phase */
		public void confirmDutyAboutShard(final ShardEntity duty, final Shard where) {
			if (duty.getDutyEvent().is(EntityEvent.ATTACH) || duty.getDutyEvent().is(EntityEvent.CREATE)) {
				for (Shard sh : partitionsByShard.keySet()) {
					if (!sh.equals(where) && partitionsByShard.get(sh).getDuties().contains(duty)) {
						String msg = new StringBuilder().append("Shard ").append(where).append(" tries to Report Duty: ")
								.append(duty).append(" already in ptable's Shard: ").append(sh).toString();
						throw new ConcurrentDutyException("Duplication failure: " + msg);
					}
				}
				getPartition(where).getDuties().add(duty);
			} else if (duty.getDutyEvent().is(EntityEvent.DETACH) || duty.getDutyEvent().is(EntityEvent.REMOVE)) {
				if (!getPartition(where).getDuties().remove(duty)) {
					throw new IllegalStateException("Absence failure. Confirmed deletion actually doesnt exist or it " + 
							"was already confirmed");
				}
			}
		}
		/** @return a copy of duties by shard: dont change duties ! */
		public Set<ShardEntity> getDutiesByShard(final Shard shard) {
			return Sets.newHashSet(getPartition(shard).getDuties());
		}

		public Set<ShardEntity> getDutiesByShard(final Pallet<?> pallet, final Shard shard) {
			return Sets.newHashSet(getPartition(shard).getDuties().stream()
					.filter(e -> e.getDuty().getPalletId().equals(pallet.getId())).collect(Collectors.toList()));
		}

		public Set<ShardEntity> getDutiesAttached() {
			return getDutiesAllByShardState(null, null);
		}
		public Set<ShardEntity> getDutiesAllByShardState(final Pallet<?> pallet, final ShardState state) {
			final Set<ShardEntity> allDuties = new HashSet<>();
			for (Shard shard : partitionsByShard.keySet().stream().filter(s -> s.getState() == state || state == null)
					.collect(Collectors.toList())) {
				allDuties.addAll(pallet == null ? partitionsByShard.get(shard).getDuties()
						: partitionsByShard.get(shard).getDuties().stream()
								.filter(d -> d.getDuty().getPalletId().equals(pallet.getId()))
								.collect(Collectors.toList()));
			}
			return allDuties;
		}

		private AttachedPartition getPartition(Shard shard) {
			AttachedPartition po = this.partitionsByShard.get(shard);
			if (po == null) {
				this.partitionsByShard.put(shard, po = AttachedPartition.partitionForFollower(shard.getShardID()));
			}
			return po;
		}

		public Shard getShard(NetworkShardID id) {
			return shardsByID.get(id);
		}

		public int getAccountConfirmed(final Pallet<?> filter) {
			int total = 0;
			for (Shard shard : partitionsByShard.keySet()) {
				if (shard.getState() == ONLINE) {
					total +=partitionsByShard.get(shard).getDuties(filter).size();
				}
			}
			return total;
		}

		public Shard getDutyLocation(final ShardEntity se) {
			for (final Shard shard : partitionsByShard.keySet()) {
				final AttachedPartition part = partitionsByShard.get(shard);
				for (ShardEntity st : part.getDuties()) {
					if (st.equals(se)) {
						return shard;
					}
				}
			}
			return null;
		}

		public List<Shard> getShards() {
			return Lists.newArrayList(this.shardsByID.values());
		}

	}
	
	/** 
	 * temporal state of modifications willing to be added to the next stage 
	 * including inconsistencies detected by the auditor
	 * */
	public static class NextStage {
		private final Set<ShardEntity> palletCrud;
		private Set<ShardEntity> dutyCrud;
		private final Set<ShardEntity> dutyMissings;
		private final Set<ShardEntity> dutyDangling;
		
		public NextStage() {
			this.palletCrud = new HashSet<>();
			this.dutyCrud = new HashSet<>();
			this.dutyMissings = new HashSet<>();
			this.dutyDangling = new HashSet<>();
		}
		public Set<ShardEntity> getDutiesDangling() {
			return this.dutyDangling;
		}

		public int accountCrudDuties() {
			return this.dutyCrud.size();
		}

		/* add it for the next Distribution cycle consideration */
		public void addCrudDuty(final ShardEntity duty) {
			if (duty.getDutyEvent().isCrud()) {
				dutyCrud.remove(duty);
				dutyCrud.add(duty);
			} else {
				throw new RuntimeException("bad idea");
			}
		}
		public Set<ShardEntity> getDutiesCrud() {
			return this.dutyCrud;
		}

		public Set<ShardEntity> getPalletsCrud() {
			return this.palletCrud;
		}

		public Set<ShardEntity> getDutiesMissing() {
			return this.dutyMissings;
		}
		public void removeCrudDuties() {
			this.dutyCrud = new HashSet<>();
		}
		private Set<ShardEntity> getEntityCrudWithFilter(final ShardEntity.Type type, final EntityEvent event,
				final State state) {
			return (type == ShardEntity.Type.DUTY ? getDutiesCrud() : getPalletsCrud()).stream()
					.filter(i -> i.getDutyEvent() == event && i.getState() == state)
					.collect(Collectors.toCollection(HashSet::new));
		}
		public Set<ShardEntity> getDutiesCrudWithFilters(final EntityEvent event, final State state) {
			return getEntityCrudWithFilter(ShardEntity.Type.DUTY, event, state);
		}
		public Set<ShardEntity> getPalletsCrudWithFilters(final EntityEvent event, final State state) {
			return getEntityCrudWithFilter(ShardEntity.Type.PALLET, event, state);
		}
	}
	
	private ClusterHealth visibilityHealth;
	private ClusterHealth workingHealth;
	private ClusterCapacity capacity;
	private final Stage stage;
	private final NextStage nextStage;
	
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

	public PartitionTable() {
		this.visibilityHealth = ClusterHealth.STABLE;
		this.workingHealth = ClusterHealth.STABLE;
		this.capacity = ClusterCapacity.IDLE;
		this.stage = new Stage();
		this.nextStage = new NextStage();
	}
	
	public NextStage getNextStage() {
		return this.nextStage;
	}
	
	public Stage getStage() {
		return this.stage;
	}

	public ClusterHealth getHealth() {
		return this.workingHealth == visibilityHealth && workingHealth == STABLE ? STABLE : UNSTABLE;
	}

	public ClusterHealth getWorkingHealth() {
		return this.workingHealth;
	}

	public void setWorkingHealth(ClusterHealth distributingHealth) {
		this.workingHealth = distributingHealth;
	}

	public ClusterHealth getVisibilityHealth() {
		return this.visibilityHealth;
	}

	public void setVisibilityHealth(ClusterHealth health) {
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
		StringBuilder sb = new StringBuilder().append("Shards: ").append(getStage().shardsByID.size()).append(" Crud Duties: ")
				.append(getNextStage().dutyCrud.size());
		//.append(" Change: ").append(change.getGroupedIssues().size());
		return sb.toString();
	}


	/* add without considerations (pallets are not distributed per se) */
	public void addCrudPallet(final ShardEntity pallet) {
		if (pallet.getDutyEvent().isCrud()) {
			getStage().palletsById.put(pallet.getPallet().getId(), pallet);
			getNextStage().palletCrud.add(pallet);
		} else {
			throw new RuntimeException("bad idea");			
		}
	}

	public void logStatus() {
		if (getStage().shardsByID.isEmpty()) {
			logger.warn("{}: Status without Shards", getClass().getSimpleName());
		} else {
			if (!logger.isInfoEnabled()) {
				return;
			}
			for (final Shard shard : getStage().shardsByID.values()) {
				final AttachedPartition partition = getStage().partitionsByShard.get(shard);
				final Map<Pallet<?>, Capacity> capacities = shard.getCapacities();
				if (partition == null) {
					logger.info("{}: {} = Empty", getClass().getSimpleName(), shard);
				} else {
					for (ShardEntity p: getStage().palletsById.values()) {
						final StringBuilder sb = new StringBuilder();
						sb.append(shard).append(" Pallet: ").append(p.getPallet().getId());
						final Capacity cap = capacities.get(p.getPallet());
						sb.append(" Size: ").append(partition.getDuties(p.getPallet()).size());
						sb.append(" Weight/Capacity: ");
						AtomicDouble weight = new AtomicDouble();
						partition.getDuties(p.getPallet()).forEach(d->weight.addAndGet(d.getDuty().getWeight()));
						sb.append(weight.get()).append("/");
						sb.append(cap!=null ? cap.getTotal(): "Unreported");
						if (logger.isDebugEnabled()) {
							sb.append(" Duties: [").append(partition.toString()).append("]");
						}
						logger.info("{}: {}", getClass().getSimpleName(), sb.toString());
					}
				}
			}
		}
		if (!this.getNextStage().dutyMissings.isEmpty()) {
			logger.warn("{}: {} Missing duties: [ {}]", getClass().getSimpleName(), getNextStage().dutyMissings.size(),
					this.getNextStage().dutyMissings.toString());
		}
		if (!this.getNextStage().dutyDangling.isEmpty()) {
			logger.warn("{}: {} Dangling duties: [ {}]", getClass().getSimpleName(), getNextStage().dutyDangling.size(),
					this.getNextStage().dutyDangling.toString());
		}
		if (this.getNextStage().dutyCrud.isEmpty()) {
			logger.info("{}: no CRUD duties", getClass().getSimpleName());
		} else {
			logger.info("{}: with {} CRUD duties: [ {}]", getClass().getSimpleName(), getNextStage().dutyCrud.size(),
					buildLogForDuties(Lists.newArrayList(getNextStage().getDutiesCrud())));
		}
		logger.info("{}: Health: {}", getClass().getSimpleName(), getWorkingHealth());
	}

}
