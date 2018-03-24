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

import java.util.ArrayList;
import java.util.Collection;
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
import io.tilt.minka.core.leader.distributor.Plan;
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
import jersey.repackaged.com.google.common.collect.Sets;

/**
 * Only one modifier allowed: {@linkplain Bookkeeper} with a {@linkplain Plan} after a distribution process.
 * 
 * Contains the relations between {@linkplain Shard} and {@linkplain Duty}.
 * Continuously checked truth in {@linkplain Stage}.
 * Client CRUD requests and detected problems in {@linkplain Backstage}
 * Built at leader promotion.
 * 
 * @author Cristian Gonzalez
 * @since Dec 2, 2015	
 */
public class PartitionTable {

	private static final Logger logger = LoggerFactory.getLogger(PartitionTable.class);
	/**
	 * the result of a {@linkplain Distributor} phase run. 
	 * state of continuously checked truth on duties attached to shards.
	 * any modifications and problem detection goes to {@linkplain Backstage}
	 */
	public static class Stage {
		
        public static class StageExtractor {
            private final Stage reference;
            public StageExtractor(final Stage reference) {
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
                        total += (pallet == null ? part.getDuties().size() : part.getDuties(pallet).size());
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
                        total += reference.partitionsByShard.get(shard).getDuties(filter).size();
                    }
                }
                return total;
            }
        }
	
		private final Map<ShardIdentifier, Shard> shardsByID;
		private final Map<Shard, ShardedPartition> partitionsByShard;
		private final Map<String, ShardEntity> palletsById;
		
		public Stage() {
			this.shardsByID = new HashMap<>();
			this.partitionsByShard = new HashMap<>();
			this.palletsById = new HashMap<>();
		}
		
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
		 * @param shard	a shard to delete from cluster
		 */
		public void removeShard(final Shard shard) {
			logger.info("{}: Removing Shard {} - bye bye ! come back some day !", getClass().getSimpleName(), shard);
			final Shard rem = this.shardsByID.remove(shard.getShardID());
			final ShardedPartition part = this.partitionsByShard.remove(shard);
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
		/**
		 * @param duty the entity to act on
		 * @param where	the sard where it resides 
		 * @return if there was a Stage change caused by the confirmation after reallocation phase */
		public boolean writeDuty(final ShardEntity duty, final Shard where, final EntityEvent event) {
			if (event.is(EntityEvent.ATTACH) || event.is(EntityEvent.CREATE)) {
				checkDuplicationFailure(duty, where);
				if (getPartition(where).add(duty)) {
					logger.info("{}: Written {} with: {} on shard {}", getClass().getSimpleName(), event, duty, where);
				} else {
					throw new IllegalStateException("Attach failure. Confirmed attach/creation already exists");
				}
				return true;
			} else if (event.is(EntityEvent.DETACH) || event.is(EntityEvent.REMOVE)) {
				if (getPartition(where).remove(duty)) {
					logger.info("{}: Written {} with: {} on shard {}", getClass().getSimpleName(), event.toVerb(), duty, where);
				} else {
					throw new IllegalStateException("Absence failure. Confirmed deletion actually doesnt exist or it " + 
							"was already confirmed");
				}
				return true;
			}
			return false;
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
		public Set<ShardEntity> getDutiesByShard(final Shard shard) {
			return Sets.newHashSet(getPartition(shard).getDuties());
		}

		public Set<ShardEntity> getDutiesByShard(final Pallet<?> pallet, final Shard shard) {
			return Sets.newHashSet(getPartition(shard)
					.getDuties().stream()
					.filter(e -> e.getDuty().getPalletId().equals(pallet.getId()))
					.collect(Collectors.toList()));
		}

		public Set<ShardEntity> getDutiesAttached() {
			return getDutiesAllByShardState(null, null);
		}
		public Set<ShardEntity> getDuties() {
			return getDutiesAllByShardState(null, null);
		}
		public Set<ShardEntity> getDutiesByPallet(final Pallet<?> pallet) {
			return getDutiesAllByShardState(pallet, null);
		}
		private Set<ShardEntity> getDutiesAllByShardState(final Pallet<?> pallet, final ShardState state) {
			final Set<ShardEntity> allDuties = new HashSet<>();
			for (Shard shard : partitionsByShard.keySet().stream()
			        .filter(s -> s.getState() == state || state == null)
					.collect(Collectors.toList())) {
				allDuties.addAll(pallet == null ? 
				        partitionsByShard.get(shard).getDuties() :
						partitionsByShard.get(shard).getDuties().stream()
								.filter(d -> d.getDuty().getPalletId().equals(pallet.getId()))
								.collect(Collectors.toList()));
			}
			return allDuties;
		}

		private synchronized ShardedPartition getPartition(Shard shard) {
			ShardedPartition po = this.partitionsByShard.get(shard);
			if (po == null) {
				this.partitionsByShard.put(shard, 
				        po = ShardedPartition.partitionForFollower(shard.getShardID()));
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

		public List<Shard> getPalletLocations(final ShardEntity se) {
			final List<Shard> ret = new ArrayList<>();
			for (final Shard shard : partitionsByShard.keySet()) {
				for (ShardEntity st : partitionsByShard.get(shard).getPallets()) {
					if (st.equals(se)) {
						ret.add(shard);
						break;
					}
				}
			}
			return ret;
		}

		public List<Shard> getShards() {
			return Lists.newArrayList(this.shardsByID.values());
		}

	}
	
	/** 
	 * temporal state of modifications willing to be added to the stage 
	 * including inconsistencies detected by the bookkeeper
	 * */
	public static class Backstage {
		private Set<ShardEntity> palletCrud;
		private Set<ShardEntity> dutyCrud;
		private Set<ShardEntity> dutyMissings;
		private Set<ShardEntity> dutyDangling;
		
		public Backstage() {
			this.palletCrud = new HashSet<>();
			this.dutyCrud = new HashSet<>();
			this.dutyMissings = new HashSet<>();
			this.dutyDangling = new HashSet<>();
		}
		/** @return read only set */
		public Set<ShardEntity> getDutiesDangling() {
	        return Collections.unmodifiableSet(this.dutyDangling);
		}
		public void addDangling(final Set<ShardEntity> dangling) {
		    this.dutyDangling.addAll(dangling);
		}
		public void cleanAllocatedDanglings() {
	        dutyDangling.removeAll(
	            dutyDangling.stream()
                    .filter(e->e.getLastState()!=EntityState.STUCK)
                    .collect(Collectors.toList()));
		}

		public int accountCrudDuties() {
			return this.dutyCrud.size();
		}

		/* add it for the next Distribution cycle consideration */
		public void addCrudDuty(final ShardEntity duty) {
			dutyCrud.remove(duty);
			dutyCrud.add(duty);
		}
		public Set<ShardEntity> getDutiesCrud() {
			return Collections.unmodifiableSet(this.dutyCrud);
		}
		public boolean removeCrud(final ShardEntity entity) {
		    return this.dutyCrud.remove(entity);
		}

		/** @return read only set */
		public Set<ShardEntity> getDutiesMissing() {
			return Collections.unmodifiableSet(this.dutyMissings);
		}
		public void clearAllocatedMissing() {
		    dutyMissings.removeAll(
		            dutyMissings.stream()
                        .filter(e->e.getLastState()!=EntityState.STUCK)
                        .collect(Collectors.toList()));
		}
		
		public void addMissing(final Set<ShardEntity> duties) {
		    this.dutyMissings.addAll(duties);
		}
        public void addMissing(final ShardEntity duty) {
            this.dutyMissings.add(duty);
        }
        
		public void removeCrudDuties() {
			this.dutyCrud = new HashSet<>();
		}
		private Set<ShardEntity> getEntityCrudWithFilter(
		        final ShardEntity.Type type, 
		        final EntityEvent event,
				final EntityState state) {
			return (type == ShardEntity.Type.DUTY ? 
			        getDutiesCrud() : 
		            palletCrud).stream()
    					.filter(e -> (event == null || e.getLog().getLast().getEvent() == event) && 
    					        (state == null || e.getLog().getLast().getLastState() == state))
    					.collect(Collectors.toCollection(HashSet::new));
		}
		public Set<ShardEntity> getDutiesCrud(final EntityEvent event, final EntityState state) {
			return getEntityCrudWithFilter(ShardEntity.Type.DUTY, event, state);
		}
		public Set<ShardEntity> getPalletsCrud(final EntityEvent event, final EntityState state) {
			return getEntityCrudWithFilter(ShardEntity.Type.PALLET, event, state);
		}
	}
	
	private ClusterHealth visibilityHealth;
	private ClusterHealth workingHealth;
	private ClusterCapacity capacity;
	private final Stage stage;
	private final Backstage backstage;
	private Backstage previousNextStage;
	private Plan currentPlan;
	private SlidingSortedSet<Plan> history;
	
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
		this.backstage = new Backstage();
		this.history = CollectionUtils.sliding(20);
	}
		
	public List<Plan> getHistory() {
		return this.history.values();
	}

	public Plan getCurrentPlan() {
		return this.currentPlan;
	}

	public void addPlan(final Plan change) {
		this.currentPlan = change;
		this.history.add(change);
	}

	
	public Backstage getBackstage() {
		return this.backstage;
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
		StringBuilder sb = new StringBuilder()
		        .append("Shards: ")
		        .append(getStage().shardsByID.size())
		        .append(" Crud Duties: ")
				.append(getBackstage().dutyCrud.size());
		//.append(" Change: ").append(change.getGroupedIssues().size());
		return sb.toString();
	}


	/* add without considerations (they're staged but not distributed per se) */
	public void addCrudPallet(final ShardEntity pallet) {
		getStage().palletsById.put(pallet.getPallet().getId(), pallet);
		getBackstage().palletCrud.add(pallet);
	}

	public void logStatus() {
		if (getStage().shardsByID.isEmpty()) {
			logger.warn("{}: Status without Shards", getClass().getSimpleName());
		} else {
			if (!logger.isInfoEnabled()) {
				return;
			}
			for (final Shard shard : getStage().shardsByID.values()) {
				final ShardedPartition partition = getStage().partitionsByShard.get(shard);
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
		logger.info("{}: Health: {}", getClass().getSimpleName(), getWorkingHealth());
	}

}
