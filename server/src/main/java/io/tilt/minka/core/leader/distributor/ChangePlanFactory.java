/*
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

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.tilt.minka.api.Config;
import io.tilt.minka.api.Duty;
import io.tilt.minka.api.Pallet;
import io.tilt.minka.core.leader.PartitionTable;
import io.tilt.minka.core.leader.balancer.Balancer;
import io.tilt.minka.core.leader.balancer.Balancer.NetworkLocation;
import io.tilt.minka.domain.EntityEvent;
import io.tilt.minka.domain.Shard;
import io.tilt.minka.domain.Shard.ShardState;
import io.tilt.minka.domain.ShardCapacity.Capacity;
import io.tilt.minka.domain.ShardEntity;
import io.tilt.minka.domain.EntityState;
import io.tilt.minka.utils.LogUtils;

/**
 * Organizes common fixed actions for every plan building.
 * Calls the appropriate {@linkplain Balancer} for each group of duties of the same pallet. 
 * Balancers use a {@linkplain Migrator} to plan its shippings, which after are written to the plan.
 * 
 * @author Cristian Gonzalez
 * @since Ene 4, 2015
 */
class ChangePlanFactory {

	private static final Logger logger = LoggerFactory.getLogger(ChangePlanFactory.class);

	private final Config config;

	ChangePlanFactory(final Config config) {
		this.config = config;
	}

	/** @return a plan if there're changes to apply or NULL if not */
	final ChangePlan create(final PartitionTable table, final ChangePlan previousChange) {
		
		final ChangePlan changePlan = new ChangePlan(
				config.beatToMs(config.getDistributor().getPlanExpirationBeats()), 
				config.getDistributor().getPlanMaxRetries());
		
		// recently fallen shards
		addMissingAsCrud(table, changePlan);
		final Set<ShardEntity> dutyCreations = table.getBackstage().getDutiesCrud(EntityEvent.CREATE, null);
		// add danglings as creations prior to migrations
		for (ShardEntity d: table.getBackstage().getDutiesDangling()) {
			dutyCreations.add(ShardEntity.Builder.builderFrom(d).build());
		}
		// add previous fallen and never confirmed migrations
		for (ShardEntity p: restorePendings(previousChange)) {
			dutyCreations.add(p);
		}
		
		final Set<ShardEntity> dutyDeletions = table.getBackstage().getDutiesCrud(EntityEvent.REMOVE, null);
		// lets add those duties of a certain deleting pallet
		for (ShardEntity p: table.getBackstage().getPalletsCrud(EntityEvent.REMOVE, EntityState.PREPARED)) {
			dutyDeletions.addAll(table.getScheme().getDutiesByPallet(p.getPallet()));
		}
		registerDeletions(table, changePlan, dutyDeletions);
		
		final Set<ShardEntity> ents = new HashSet<>(table.getScheme().getDutiesAttached());
		ents.addAll(dutyCreations);
		boolean changes = false;
		final Map<String, List<ShardEntity>> schemeByPallets = ents.stream()
				.collect(Collectors.groupingBy(e -> e.getDuty().getPalletId()));
		if (schemeByPallets.isEmpty()) {
			logger.warn("{}: Scheme is empty. Nothing to balance", getClass().getSimpleName());
		} else {
			final List<Shard> onlineShards = table.getScheme().getShardsByState(ShardState.ONLINE);
			for (final Map.Entry<String, List<ShardEntity>> entry : schemeByPallets.entrySet()) {
				final Pallet<?> pallet = table.getScheme().getPalletById(entry.getKey()).getPallet();
				final Balancer balancer = Balancer.Directory.getByStrategy(pallet.getMetadata().getBalancer());
				logStatus(table, onlineShards, dutyCreations, dutyDeletions, entry.getValue(), pallet, balancer);
				if (balancer != null) {
					final Migrator migra = balance(table, pallet, balancer, dutyCreations, dutyDeletions);
					changes |= migra.write(changePlan);
				} else {
					if (logger.isInfoEnabled()) {
						logger.info("{}: Balancer not found ! {} set on Pallet: {} (curr size:{}) ", getClass().getSimpleName(),
							pallet.getMetadata().getBalancer(), pallet, Balancer.Directory.getAll().size());
					}
				}
			}
		}
		if (changes) {
			return changePlan;
		} else {
			changePlan.discard();
			return null;
		}
	}

	private static final Migrator balance(
			final PartitionTable table, 
			final Pallet<?> pallet, 
			final Balancer balancer,
			final Set<ShardEntity> dutyCreations, 
			final Set<ShardEntity> dutyDeletions) {

		final Set<ShardEntity> removes = dutyDeletions.stream()
				.filter(d -> d.getDuty().getPalletId().equals(pallet.getId()))
				.collect(Collectors.toSet());
		final Set<ShardEntity> adds = dutyCreations.stream()
				.filter(d -> d.getDuty().getPalletId().equals(pallet.getId()))
				.collect(Collectors.toSet());

		final Set<ShardEntity> sourceRefs = new HashSet<>(removes.size() + adds.size());
		final Map<NetworkLocation, Set<Duty<?>>> distro = new TreeMap<>();
		for (Shard shard : table.getScheme().getShards()) {
			if (shard.getState() == ShardState.ONLINE) {
				final Set<ShardEntity> located = table.getScheme().getDutiesByShard(pallet, shard);
				distro.put(new NetworkLocation(shard), refs(located));
				sourceRefs.addAll(located);
			}
		}

		sourceRefs.addAll(removes);
		sourceRefs.addAll(adds);
		final Migrator migrator = new Migrator(table, pallet, sourceRefs);
		final Map<EntityEvent, Set<Duty<?>>> backstage = new HashMap<>();
		backstage.put(EntityEvent.CREATE, refs(adds));
		backstage.put(EntityEvent.REMOVE, refs(removes));
		balancer.balance(pallet, distro, backstage, migrator);
		return migrator;
	}

	private static final Set<Duty<?>> refs(final Set<ShardEntity> entities) {
		final Set<Duty<?>> ret = new HashSet<>(entities.size());
		entities.forEach(e -> ret.add(e.getDuty()));
		return ret;
	}
	
	private static void addMissingAsCrud(final PartitionTable table, final ChangePlan changePlan) {
	    final Set<ShardEntity> missing = table.getBackstage().getDutiesMissing();
		for (final ShardEntity missed : missing) {
			final Shard lazy = table.getScheme().getDutyLocation(missed);
			if (logger.isInfoEnabled()) {
				logger.info("{}: Registering {}, missing Duty: {}", ChangePlanFactory.class.getSimpleName(),
					lazy == null ? "unattached" : "from falling Shard: " + lazy, missed);
			}
			if (lazy != null) {
				table.getScheme().writeDuty(missed, lazy, EntityEvent.REMOVE, ()-> {
					// missing duties are a confirmation per-se from the very shards,
					// so the ptable gets fixed right away without a realloc.
					missed.getJournal().addEvent(EntityEvent.REMOVE, 
							EntityState.CONFIRMED,
							lazy.getShardID(),
							changePlan.getId());					
				});
			}
			missed.getJournal().addEvent(EntityEvent.CREATE, 
					EntityState.PREPARED,
					null,
					changePlan.getId());
			table.getBackstage().addCrudDuty(missed);
		}
		if (!missing.isEmpty()) {
			if (logger.isInfoEnabled()) {
				logger.info("{}: Registered {} dangling duties {}", ChangePlanFactory.class.getSimpleName(),
					missing.size(), ShardEntity.toStringIds(missing));
			}
		}
		// clear it or nobody will
		table.getBackstage().clearAllocatedMissing();
	}

	/*
	 * check waiting duties never confirmed (for fallen shards as previous
	 * target candidates)
	 */
	private final List<ShardEntity> restorePendings(final ChangePlan previous) {
		if (previous != null 
				&& previous.getResult().isClosed() 
				&& !previous.getResult().isSuccess()) {
			final List<ShardEntity> pendings = previous.getAllNonConfirmedFromAllDeliveries();
			if (pendings.isEmpty() && logger.isInfoEnabled()) {
				logger.info("{}: Previous change although unfinished hasnt waiting duties", getClass().getSimpleName());
			} else {
				if (logger.isInfoEnabled()) {
					logger.info("{}: Previous change's unfinished business saved as Dangling: {}",
						getClass().getSimpleName(), pendings.toString());
				}
				return pendings;
			}
		}
		return Collections.emptyList();
	}

	/* by user deleted */
	private final void registerDeletions(final PartitionTable table, final ChangePlan changePlan,
			final Set<ShardEntity> deletions) {

		for (final ShardEntity deletion : deletions) {
			final Shard shard = table.getScheme().getDutyLocation(deletion);
			deletion.getJournal().addEvent(EntityEvent.DETACH, 
					EntityState.PREPARED,
					shard.getShardID(),
					changePlan.getId());
			changePlan.ship(shard, deletion);
			if (logger.isInfoEnabled()) {
				logger.info("{}: Deleting from: {}, Duty: {}", getClass().getSimpleName(), shard.getShardID(),
					deletion.toBrief());
			}
		}
	}

	private void logStatus(
			final PartitionTable table, final List<Shard> onlineShards,
			final Set<ShardEntity> dutyCreations, 
			final Set<ShardEntity> dutyDeletions,
			final List<ShardEntity> duties,
			final Pallet<?> pallet, 
			final Balancer balancer) {
		
		if (!logger.isInfoEnabled()) {
			return;
		}
		
		logger.info(LogUtils.titleLine(LogUtils.HYPHEN_CHAR, "Building Pallet: %s for %s", pallet.getId(), balancer.getClass().getSimpleName()));
		double clusterCapacity = 0;
		for (final Shard node: onlineShards) {
			final Capacity cap = node.getCapacities().get(pallet);
			final double currTotal = cap == null ? 0 :  cap.getTotal();
			logger.info("{}: Capacity Shard {} : {}", getClass().getSimpleName(), node.toString(), currTotal);
			clusterCapacity += currTotal;
		}
		logger.info("{}: Total cluster capacity: {}", getClass().getSimpleName(), clusterCapacity);
		logger.info("{}: counting #{};+{};-{} duties: {}", getClass().getSimpleName(),
			new PartitionTable.Scheme.SchemeExtractor(table.getScheme())
				.getAccountConfirmed(pallet), 
			dutyCreations.stream()
				.filter(d->d.getDuty().getPalletId().equals(pallet.getId()))
				.count(),
			dutyDeletions.stream()
				.filter(d->d.getDuty().getPalletId().equals(pallet.getId()))
				.count(),
			ShardEntity.toStringIds(duties));
	}
	
}
