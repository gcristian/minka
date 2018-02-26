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
import io.tilt.minka.domain.Shard.ShardState;
import io.tilt.minka.domain.ShardCapacity.Capacity;
import io.tilt.minka.domain.ShardEntity;
import io.tilt.minka.domain.EntityState;
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

	public final Plan evaluate(final PartitionTable table, final Plan previousChange) {
		final Plan plan = new Plan(
				config.getDistributor().getPlanExpirationSec(), 
				config.getDistributor().getPlanMaxRetries());
		final List<Shard> onlineShards = table.getStage().getShardsByState(ShardState.ONLINE);
		// recently fallen shards
		final Set<ShardEntity> dangling = new HashSet<>(table.getNextStage().getDutiesDangling());
		addMissingAsCrud(table, plan);
		// add previous fallen and never confirmed migrations
		dangling.addAll(restorePendings(previousChange));
		// add danglings as creations prior to migrations
		final List<ShardEntity> danglingAsCreations = new ArrayList<>();
		dangling.forEach(dang -> danglingAsCreations.add(ShardEntity.Builder.builderFrom(dang).build()));
		final Set<ShardEntity> dutyCreations = table.getNextStage().getDutiesCrud(EntityEvent.CREATE, null); //State.PREPARED); evidentemente al subir y bajar queda alguno con CREATED y STUCK ?
		dutyCreations.addAll(danglingAsCreations);
		
		final Set<ShardEntity> dutyDeletions = table.getNextStage().getDutiesCrud(EntityEvent.REMOVE, null); //State.PREPARED);
		// lets add those duties of a certain deleting pallet
		table.getNextStage().getPalletsCrud(EntityEvent.REMOVE, EntityState.PREPARED)
			.forEach(p->dutyDeletions.addAll(table.getStage().getDutiesByPallet(p.getPallet())));
		registerDeletions(table, plan, dutyDeletions);
		
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
					final Set<ShardEntity> removes = dutyDeletions.stream()
					        .filter(d->d.getDuty().getPalletId().equals(pallet.getPallet().getId()))
					        .collect(Collectors.toSet());
					final Set<ShardEntity> adds = dutyCreations.stream()
					        .filter(d->d.getDuty().getPalletId().equals(pallet.getPallet().getId()))
					        .collect(Collectors.toSet());
					
					logStatus(table, onlineShards, dutyCreations, dutyDeletions, allColl, pallet, balancer);
					final Map<Shard, Set<ShardEntity>> index = new HashMap<>();
					table.getStage().getShardsByState(ShardState.ONLINE).forEach(
							s->index.put(s, table.getStage().getDutiesByShard(pallet.getPallet(), s)));
					
					final Migrator migra = new Migrator(table, plan, pallet.getPallet());
					final NextTable nextTable = new NextTable(pallet.getPallet(), index, adds, removes, plan, migra);
					balancer.balance(nextTable);
					if (!migra.isEmpty()) {
						migra.execute();
					}
				} else {
					logger.info("{}: Balancer not found ! {} set on Pallet: {} (curr size:{}) ", getClass().getSimpleName(), 
						pallet.getPallet().getMetadata().getBalancer(), pallet, Balancer.Directory.getAll().size());
				}
			} catch (Exception e) {
				logger.error("Unexpected", e);
			}
		}
		return plan;
	}

	protected static void addMissingAsCrud(final PartitionTable table, final Plan plan) {
	    final Set<ShardEntity> missing = table.getNextStage().getDutiesMissing();
		for (final ShardEntity missed : missing) {
			final Shard lazy = table.getStage().getDutyLocation(missed);
			logger.info("{}: Registering {}, dangling Duty: {}", Arranger.class.getSimpleName(),
					lazy == null ? "unattached" : "from falling Shard: " + lazy, missed);
			if (lazy != null) {
				// missing duties are a confirmation per-se from the very shards,
				// so the ptable gets fixed right away without a realloc.
				missed.getLog().addEvent(EntityEvent.REMOVE, io.tilt.minka.domain.EntityState.CONFIRMED, 
				        lazy.getShardID(), 
				        plan.getId());
				table.getStage().writeDuty(missed, lazy);
			}
			missed.getLog().addEvent(EntityEvent.CREATE, EntityState.PREPARED,
			        null, 
                    plan.getId());
			table.getNextStage().addCrudDuty(missed);
		}
		if (!missing.isEmpty()) {
			logger.info("{}: Registered {} dangling duties {}", Arranger.class.getSimpleName(),
					missing.size(), ShardEntity.toStringIds(missing));
		}
		// clear it or nobody will
		table.getNextStage().clearAllocatedMissing();
	}

	/*
	 * check waiting duties never confirmed (for fallen shards as previous
	 * target candidates)
	 */
	protected List<ShardEntity> restorePendings(final Plan previous) {
		List<ShardEntity> pendings = new ArrayList<>();
		if (previous != null && !previous.getResult().isClosed()) {
			/*
			 * .stream() no siempre la NO confirmacion sucede sobre un
			 * fallen shard .filter(i->i.getServiceState()==QUITTED)
			 * .collect(Collectors.toList())
			 */
			pendings.addAll(previous.getAllNonConfirmedFromAllDeliveries());
			if (pendings.isEmpty()) {
				logger.info("{}: Previous change although unfinished hasnt waiting duties", getClass().getSimpleName());
			} else {
				logger.info("{}: Previous change's unfinished business saved as Dangling: {}",
						getClass().getSimpleName(), pendings.toString());
			}
		}
		return pendings;
	}

	/* by user deleted */
	private void registerDeletions(final PartitionTable table, final Plan plan,
			final Set<ShardEntity> deletions) {

		for (final ShardEntity deletion : deletions) {
			final Shard shard = table.getStage().getDutyLocation(deletion);
			deletion.getLog().addEvent(EntityEvent.DETACH, EntityState.PREPARED,
			        shard.getShardID(), 
                    plan.getId());
			plan.ship(shard, deletion);
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
		private final Plan plan;
		
		private Migrator migra;
		private Set<ShardEntity> duties;
		
		protected NextTable(final Pallet<?> pallet, final Map<Shard, Set<ShardEntity>> dutiesByShard, 
				final Set<ShardEntity> creations, final Set<ShardEntity> deletions, final Plan plan, 
				final Migrator migra) {
			super();
			this.pallet = pallet;
			this.dutiesByShard = Collections.unmodifiableMap(dutiesByShard);
			this.creations = Collections.unmodifiableSet(creations);
			this.deletions = Collections.unmodifiableSet(deletions);
			this.plan = plan;
			this.migra = migra;
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
		/** @return an immutable set of new duties that must be distibuted and doesnt exist in table object 
		 * including also recycled duties like danglings and missings which are not new */
		public Set<ShardEntity> getCreations() {
			return this.creations;
		}
		/** @return an immutable set of deletions that will cease to exist in the table (already marked)  */
		public Set<ShardEntity> getDeletions() {
			return this.deletions;
		}
		/** @deprecated
		 * @return a plan inside the current table
		 * */
		public Plan getPlan() {
			return this.plan;
		}
		/** @return a facility to request modifications for duty assignation for the next distribution */
		public synchronized Migrator getMigrator() {
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
			new PartitionTable.Stage.StageExtractor(table.getStage()).getAccountConfirmed(pallet.getPallet()), 
			dutyCreations.stream().filter(d->d.getDuty().getPalletId().equals(pallet.getPallet().getId())).count(),
			dutyDeletions.stream().filter(d->d.getDuty().getPalletId().equals(pallet.getPallet().getId())).count(),
			ShardEntity.toStringIds(allCollector.getDuties(pallet)));
	}
	
}
