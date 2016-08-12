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

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

import io.tilt.minka.api.Duty;
import io.tilt.minka.api.Pallet;
import io.tilt.minka.domain.EntityEvent;
import io.tilt.minka.domain.NetworkShardID;
import io.tilt.minka.domain.Partition;
import io.tilt.minka.domain.Shard;
import io.tilt.minka.domain.ShardEntity;
import io.tilt.minka.domain.ShardEntity.State;
import io.tilt.minka.domain.ShardID;
import io.tilt.minka.domain.ShardState;
import jersey.repackaged.com.google.common.collect.Sets;

/**
 * Dynamic table of relations between {@linkplain Shard} and {@linkplain Duty}
 * Built at leader start-up, i.e. when it fells and another gets promoted leader: this will also get build. 
 * 
 * @author Cristian Gonzalez
 * @since Dec 2, 2015
 */
public class PartitionTable {

		private final Logger logger = LoggerFactory.getLogger(getClass());

		private Map<ShardID, Shard> shardsByID;
		private Map<Shard, Partition> partitionsByShard;

		/* only for saving client's additions */
		private Set<ShardEntity> pallets;
		private Set<ShardEntity> crud;
		private Set<ShardEntity> missing;
		private Set<ShardEntity> dangling;
		private ClusterHealth visibilityHealth;
		private ClusterHealth workingHealth;
		private ClusterCapacity capacity;

		/**
		 * status for the cluster taken as Avg. for the last 5 cycles
		 * when this happens the cluster rebalances workload 
		 * by making shards exchange duties  
		 */
		public enum ClusterHealth {
			/*
			 * visibility: if there was no shard changing status working: theere's
			 * work-load balance among the nodes
			 */
			STABLE,
			/*
			 * visibility: if there was at least one shard changing status that
			 * will provoke a duty reallocation working: there's a reallocation in
			 * progress
			 */
			UNSTABLE,;
		}

		public enum ClusterCapacity {
			IDLE, NORMAL,
			/*
			 * if there's only one Shard alive, or workload calculation needs more
			 * working shards
			 */
			INSUFFICIENT,
		}

		public PartitionTable() {
			this.shardsByID = new HashMap<>();
			this.partitionsByShard = new HashMap<>();

			this.pallets = new HashSet<>();
			this.crud = new HashSet<>();
			this.missing = new HashSet<>();
			this.dangling = new HashSet<>();

			this.visibilityHealth = ClusterHealth.STABLE;
			this.workingHealth = ClusterHealth.STABLE;
			this.capacity = ClusterCapacity.IDLE;
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

		public Set<ShardEntity> getDutiesDangling() {
			return this.dangling;
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
			StringBuilder sb = new StringBuilder().append("Shards: ").append(shardsByID.size()).append(" Crud Duties: ")
						.append(this.crud.size());
			//.append(" Change: ").append(change.getGroupedIssues().size());
			return sb.toString();
		}

		public int accountCrudDuties() {
			return this.crud.size();
		}

		public void addCrudDuty(final ShardEntity duty) {
			if (duty.getDutyEvent().isCrud()) {
				crud.remove(duty);
				crud.add(duty);
			} else {
				throw new RuntimeException("bad idea");
			}
		}

		public void removeCrudDuties() {
			this.crud = new HashSet<>();
		}

		public Set<ShardEntity> getDutiesCrud() {
			return this.crud;
		}

		public Set<ShardEntity> getDutiesMissing() {
			return this.missing;
		}

		public Set<ShardEntity> getDutiesCrudByFilters(final EntityEvent event, final State state) {
			return getDutiesCrud().stream().filter(i -> i.getDutyEvent() == event && i.getState() == state)
						.collect(Collectors.toCollection(HashSet::new));
		}

		public Set<ShardEntity> getPallets() {
			return this.pallets;
		}

		/**
		 * When a Shard has been offline and their dangling duties reassigned
		 * after completing Change's cycles: the shard must be deleted
		 * @param shard
		 */
		public void removeShard(final Shard shard) {
			logger.info("{}: Removing Shard {} - bye bye ! come back some day !", getClass().getSimpleName(), shard);
			final Shard rem = this.shardsByID.remove(shard.getShardID());
			final Partition part = this.partitionsByShard.remove(shard);
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
			if (duty.getDutyEvent().is(EntityEvent.ASSIGN) || duty.getDutyEvent().is(EntityEvent.CREATE)) {
				for (Shard sh : partitionsByShard.keySet()) {
						if (!sh.equals(where) && partitionsByShard.get(sh).getDuties().contains(duty)) {
							String msg = new StringBuilder().append("Shard ").append(where).append(" tries to Report Duty: ")
										.append(duty).append(" already in ptable's Shard: ").append(sh).toString();
							throw new ConcurrentDutyException("Duplication failure: " + msg);
						}
				}
				getPartition(where).getDuties().add(duty);
			} else if (duty.getDutyEvent().is(EntityEvent.UNASSIGN) || duty.getDutyEvent().is(EntityEvent.DELETE)) {
				if (!getPartition(where).getDuties().remove(duty)) {
						throw new IllegalStateException(
								"Absence failure. Confirmed deletion actually doesnt exist or it " + "was already confirmed");
				}
			}
		}

		/** @return a copy of duties by shard: dont change duties ! */
		public Set<ShardEntity> getDutiesByShard(final Shard shard) {
			return Sets.newHashSet(getPartition(shard).getDuties());
		}
		
		public Set<ShardEntity> getDutiesByShard(final Pallet<?> pallet, final Shard shard) {
			return Sets.newHashSet(getPartition(shard).getDuties().stream()
					.filter(e->e.getDuty().getPalletId().equals(pallet.getId()))
					.collect(Collectors.toList()));
		}

		public Set<ShardEntity> getDutiesAllByShardState(final Pallet<?> pallet, final ShardState state) {
			final Set<ShardEntity> allDuties = new HashSet<>();
			for (Shard shard : partitionsByShard.keySet().stream()
						.filter(s -> s.getState() == state || state == null)
						.collect(Collectors.toList())) {
				allDuties.addAll(pallet == null ? partitionsByShard.get(shard).getDuties() : 
						partitionsByShard.get(shard).getDuties().stream()
						.filter(d->d.getDuty().getPalletId().equals(pallet.getId()))
						.collect(Collectors.toList()));
			}
			return allDuties;
		}

		private Partition getPartition(Shard shard) {
			Partition po = this.partitionsByShard.get(shard);
			if (po == null) {
				this.partitionsByShard.put(shard, po = Partition.partitionForFollower(shard.getShardID()));
			}
			return po;
		}

		public Shard getShard(NetworkShardID id) {
			return shardsByID.get(id);
		}

		public int getAccountConfirmed() {
			int total = 0;
			for (Shard shard : partitionsByShard.keySet()) {
				if (shard.getState() == ONLINE) {
						Partition part = partitionsByShard.get(shard);
						total += part.getDuties().size();
				}
			}
			return total;
		}

		public Shard getDutyLocation(final ShardEntity se) {
			for (final Shard shard : partitionsByShard.keySet()) {
				final Partition part = partitionsByShard.get(shard);
				for (ShardEntity st : part.getDuties()) {
						if (st.equals(se)) {
							return shard;
						}
				}
			}
			return null;
		}

		/** @return  shards by state filter or all if null */
		public List<Shard> getShardsByState(final ShardState filter) {
			return shardsByID.values().stream().filter(i -> i.getState() == filter || filter == null)
						.collect(Collectors.toList());
		}

		public List<Shard> getAllImmutable() {
			return Lists.newArrayList(this.shardsByID.values());
		}

		public void logStatus() {
			if (shardsByID.isEmpty()) {
				logger.warn("{}: Status without Shards", getClass().getSimpleName());
			} else {
				for (final Shard shard : shardsByID.values()) {
						final Partition partition = partitionsByShard.get(shard);
						if (partition == null) {
							logger.info("{}: {} = Empty", getClass().getSimpleName(), shard);
						} else {
							if (logger.isDebugEnabled()) {
								logger.debug("{}: Status for Shard: {} = Weight: {} with {} Duties: [ {}]",
											getClass().getSimpleName(), shard, partition.getWeight(), partition.getDuties().size(),
											partition.toString());
							} else {
								logger.info("{}: Status for Shard: {} = Weight: {} with {} Duties", getClass().getSimpleName(),
											shard, partition.getWeight(), partition.getDuties().size());
							}

						}
				}
			}
			if (!this.missing.isEmpty()) {
				logger.warn("{}: {} Missing duties: [ {}]", getClass().getSimpleName(), missing.size(),
							this.missing.toString());
			}
			if (!this.dangling.isEmpty()) {
				logger.warn("{}: {} Dangling duties: [ {}]", getClass().getSimpleName(), dangling.size(),
							this.dangling.toString());
			}
			if (this.crud.isEmpty()) {
				logger.info("{}: no CRUD duties", getClass().getSimpleName());
			} else {
				logger.info("{}: with {} CRUD duties: [ {}]", getClass().getSimpleName(), crud.size(),
							buildLogForDuties(Lists.newArrayList(getDutiesCrud())));
			}
			logger.info("{}: Health: {}", getClass().getSimpleName(), getWorkingHealth());
		}

}
