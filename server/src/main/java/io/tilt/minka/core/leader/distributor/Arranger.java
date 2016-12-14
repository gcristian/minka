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

import static io.tilt.minka.domain.ShardState.ONLINE;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.tilt.minka.api.Config;
import io.tilt.minka.api.Pallet;
import io.tilt.minka.core.leader.PartitionTable;
import io.tilt.minka.core.leader.distributor.Balancer.BalancerMetadata;
import io.tilt.minka.domain.EntityEvent;
import io.tilt.minka.domain.Shard;
import io.tilt.minka.domain.ShardCapacity.Capacity;
import io.tilt.minka.domain.ShardEntity;
import io.tilt.minka.domain.ShardEntity.State;
import io.tilt.minka.domain.ShardState;
import io.tilt.minka.utils.LogUtils;

/**
 * Common fixed flow for balancing
 * 
 * @author Cristian Gonzalez
 * @since Ene 4, 2015
 */
public class Arranger {

	private static final Logger logger = LoggerFactory.getLogger(Arranger.class);

	private final Config config;

	protected Arranger(final Config config) {
		this.config = config;
	}

	public final Roadmap callForBalance(final PartitionTable table, final Roadmap previousChange) {
		final Roadmap roadmap = new Roadmap();
		final List<Shard> onlineShards = table.getStage().getShardsByState(ONLINE);
		// recently fallen shards
		final Set<ShardEntity> dangling = table.getNextStage().getDutiesDangling();
		registerMissing(table, roadmap, table.getNextStage().getDutiesMissing());
		// add previous fallen and never confirmed migrations
		dangling.addAll(restoreUnfinishedBusiness(previousChange));
		// add danglings as creations prior to migrations
		final List<ShardEntity> danglingAsCreations = new ArrayList<>();
		dangling.forEach(i -> danglingAsCreations.add(ShardEntity.Builder.builderFrom(i).build()));
		final Set<ShardEntity> dutyCreations = table.getNextStage().getDutiesCrudWithFilters(EntityEvent.CREATE, State.PREPARED);
		dutyCreations.addAll(danglingAsCreations);
		
		final Set<ShardEntity> dutyDeletions = table.getNextStage().getDutiesCrudWithFilters(EntityEvent.REMOVE, State.PREPARED);
		// lets add those duties of a certain deleting pallet
		table.getNextStage().getPalletsCrudWithFilters(EntityEvent.REMOVE, State.PREPARED)
			.forEach(p->dutyDeletions.addAll(table.getStage().getDutiesAll(p.getPallet())));
		registerDeletions(table, roadmap, dutyDeletions);
		
		final Set<ShardEntity> ents = new HashSet<>(table.getStage().getDutiesAttached());
		ents.addAll(dutyCreations);
		final PalletCollector allColl = new PalletCollector(ents, table.getStage().getPallets());
		final Iterator<Set<ShardEntity>> itPallet = allColl.getPalletsIterator();
		while (itPallet.hasNext()) {
			try {
				Iterator<ShardEntity> itDuties = itPallet.next().iterator();
				final ShardEntity pallet = allColl.getPallet(itDuties.next().getDuty().getPalletId());
				final BalancerMetadata meta = pallet.getPallet().getMetadata();
				final Balancer balancer = Balancer.Directory.getByStrategy(meta.getBalancer());
				
				if (balancer!=null) {
					final Set<ShardEntity> removes = dutyDeletions.stream().filter(d->d.getDuty().getPalletId()
							.equals(pallet.getPallet().getId())).collect(Collectors.toSet());
					final Set<ShardEntity> adds = dutyCreations.stream().filter(d->d.getDuty().getPalletId()
							.equals(pallet.getPallet().getId())).collect(Collectors.toSet());
					
					logStatus(table, onlineShards, dutyCreations, dutyDeletions, allColl, pallet, balancer);
					final Map<Shard, Set<ShardEntity>> index = new HashMap<>();
					table.getStage().getShardsByState(ShardState.ONLINE).forEach(
							s->index.put(s, table.getStage().getDutiesByShard(pallet.getPallet(), s)));
					
					final NextTable nextTable = new NextTable(pallet.getPallet(), index, adds, removes, roadmap, table);
					balancer.balance(nextTable);
					if (!nextTable.getMigrator().isEmpty()) {
						nextTable.getMigrator().execute();
					}
				} else {
					logger.info("{}: Balancer not found ! {} set on Pallet: {} (curr size:{}) ", getClass().getSimpleName(), 
						pallet.getPallet().getMetadata().getBalancer(), pallet, Balancer.Directory.getAll().size());
				}
			} catch (Exception e) {
				logger.error("Unexpected", e);
			}
		}
		return roadmap;
	}

	protected static void registerMissing(final PartitionTable table, final Roadmap realloc,
			final Set<ShardEntity> missing) {
		for (final ShardEntity missed : missing) {
			final Shard lazy = table.getStage().getDutyLocation(missed);
			logger.info("{}: Registering {}, dangling Duty: {}", Arranger.class.getSimpleName(),
					lazy == null ? "unattached" : "from falling Shard: " + lazy, missed);
			if (lazy != null) {
				// missing duties are a confirmation per-se from the very shards,
				// so the ptable gets fixed right away without a realloc.
				missed.registerEvent(EntityEvent.REMOVE, State.CONFIRMED);
				table.getStage().confirmDutyAboutShard(missed, lazy);
			}
			missed.registerEvent(EntityEvent.CREATE, State.PREPARED);
			table.getNextStage().addCrudDuty(missed);
		}
		if (!missing.isEmpty()) {
			logger.info("{}: Registered {} dangling duties {}", Arranger.class.getSimpleName(),
					missing.size(), ShardEntity.toStringIds(missing));
		}
		// clear it or nobody will
		missing.clear();
	}

	/*
	 * check waiting duties never confirmed (for fallen shards as previous
	 * target candidates)
	 */
	protected List<ShardEntity> restoreUnfinishedBusiness(final Roadmap previousChange) {
		List<ShardEntity> unfinishedWaiting = new ArrayList<>();
		if (previousChange != null && !previousChange.isEmpty() && !previousChange.hasCurrentStepFinished()
				&& !previousChange.hasFinished()) {
			previousChange.getGroupedDeliveries().keys()
					/*
					 * .stream() no siempre la NO confirmacion sucede sobre un
					 * fallen shard .filter(i->i.getServiceState()==QUITTED)
					 * .collect(Collectors.toList())
					 */
					.forEach(i -> previousChange.getGroupedDeliveries().get(i).stream()
							.filter(j -> j.getState() == State.SENT).forEach(j -> unfinishedWaiting.add(j)));
			if (unfinishedWaiting.isEmpty()) {
				logger.info("{}: Previous change although unfinished hasnt waiting duties", getClass().getSimpleName());
			} else {
				logger.info("{}: Previous change's unfinished business saved as Dangling: {}",
						getClass().getSimpleName(), unfinishedWaiting.toString());
			}
		}
		return unfinishedWaiting;
	}

	/* by user deleted */
	private void registerDeletions(final PartitionTable table, final Roadmap roadmap,
			final Set<ShardEntity> deletions) {

		for (final ShardEntity deletion : deletions) {
			Shard shard = table.getStage().getDutyLocation(deletion);
			deletion.registerEvent(EntityEvent.DETACH, State.PREPARED);
			roadmap.ship(shard, deletion);
			logger.info("{}: Deleting from: {}, Duty: {}", getClass().getSimpleName(), shard.getShardID(),
					deletion.toBrief());
		}
	}

	protected Config getConfig() {
		return this.config;
	}

	/**
	 * A minimalist version of {@linkplain PartitionTable}  
	 * Creations and duties (table's current assigned duties) must be balanced.
	 * Note deletions are included in duties but must be ignored and removed from balancing,
	 * i.e. they must not be included in overrides and transfers.
	 */	
	public static class NextTable {
		private final Pallet<?> pallet;
		private final Map<Shard, Set<ShardEntity>> dutiesByShard;
		private final Set<ShardEntity> creations;
		private final Set<ShardEntity> deletions;
		private final Roadmap roadmap;
		private final PartitionTable partitionTable;
		
		private Migrator migra;
		private Set<ShardEntity> duties;
		
		protected NextTable(final Pallet<?> pallet, final Map<Shard, Set<ShardEntity>> dutiesByShard, 
				final Set<ShardEntity> creations, final Set<ShardEntity> deletions, 
				final Roadmap roadmap, final PartitionTable partitionTable) {
			super();
			this.pallet = pallet;
			this.dutiesByShard = Collections.unmodifiableMap(dutiesByShard);
			this.creations = Collections.unmodifiableSet(creations);
			this.deletions = Collections.unmodifiableSet(deletions);
			this.roadmap = roadmap;
			this.partitionTable = partitionTable;
		}
		public Pallet<?> getPallet() {
			return this.pallet;
		}
		/** @return an immutable map repr. the partition table's duties assigned to shards */
		public Map<Shard, Set<ShardEntity>> getIndex() {
			return this.dutiesByShard;
		}
		/** @return an immutable duty set representing all indexed contents */
		public synchronized Set<ShardEntity> getDuties() {
			if (duties == null) {
				final Set<ShardEntity> tmp = new HashSet<>();
				getIndex().values().forEach(set->tmp.addAll(set));
				duties = Collections.unmodifiableSet(tmp);
			}
			return duties;
		}
		/** @return an immutable set of new duties that must be distibuted and doesnt exist in table object */
		public Set<ShardEntity> getCreations() {
			return this.creations;
		}
		/** @return an immutable set of deletions that will cease to exist in the table (already marked)  */
		public Set<ShardEntity> getDeletions() {
			return this.deletions;
		}
		protected PartitionTable getPartitionTable() {
			return this.partitionTable;
		}
		/** @deprecated
		 * @return a roadmap inside the current table
		 * */
		public Roadmap getRoadmap() {
			return this.roadmap;
		}
		/** @return a facility to request modifications for duty assignation for the next distribution */
		public synchronized Migrator getMigrator() {
			if (migra == null) {
				this.migra = new Migrator(partitionTable, roadmap, pallet); 
			}
			return migra;
		}
	}
	
	private void logStatus(final PartitionTable table, final List<Shard> onlineShards,
			final Set<ShardEntity> dutyCreations, final Set<ShardEntity> dutyDeletions,
			final PalletCollector allCollector, final ShardEntity pallet, final Balancer balancer) {
		
		if (!logger.isInfoEnabled()) {
			return;
		}
		
		logger.info(LogUtils.titleLine(LogUtils.HYPHEN_CHAR, "Arranging Pallet: %s for %s", pallet.toBrief(), balancer.getClass().getSimpleName()));
		final StringBuilder sb = new StringBuilder();
		double clusterCapacity = 0;
		for (final Shard node: onlineShards) {
			final Capacity cap = node.getCapacities().get(pallet.getPallet());
			final double currTotal = cap == null ? 0 :  cap.getTotal();
			sb.append(node.toString()).append(": ").append(currTotal).append(", ");
			clusterCapacity += currTotal;
		}
		logger.info("{}: Cluster capacity: {}, Shard Capacities { {} }", getClass().getSimpleName(), clusterCapacity, sb.toString());
		logger.info("{}: counting #{};+{};-{} duties: {}", getClass().getSimpleName(),
			table.getStage().getAccountConfirmed(pallet.getPallet()), 
			dutyCreations.stream().filter(d->d.getDuty().getPalletId().equals(pallet.getPallet().getId())).count(),
			dutyDeletions.stream().filter(d->d.getDuty().getPalletId().equals(pallet.getPallet().getId())).count(),
			ShardEntity.toStringIds(allCollector.getDuties(pallet)));
	}
	
}
