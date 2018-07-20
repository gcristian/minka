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

import static io.tilt.minka.domain.EntityEvent.ATTACH;
import static io.tilt.minka.domain.EntityEvent.CREATE;
import static io.tilt.minka.domain.EntityEvent.DETACH;
import static io.tilt.minka.domain.EntityEvent.REMOVE;
import static io.tilt.minka.domain.EntityState.CONFIRMED;
import static io.tilt.minka.domain.EntityState.PREPARED;
import static io.tilt.minka.domain.ShardEntity.toStringIds;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.tilt.minka.api.Config;
import io.tilt.minka.api.Duty;
import io.tilt.minka.api.Pallet;
import io.tilt.minka.core.leader.balancer.Balancer;
import io.tilt.minka.core.leader.balancer.NetworkLocation;
import io.tilt.minka.core.leader.data.CommitedState;
import io.tilt.minka.core.leader.data.ShardingState;
import io.tilt.minka.core.leader.data.UncommitedChanges;
import io.tilt.minka.domain.EntityEvent;
import io.tilt.minka.domain.ShardEntity;
import io.tilt.minka.shard.Capacity;
import io.tilt.minka.shard.Shard;
import io.tilt.minka.shard.ShardState;
import io.tilt.minka.utils.LogUtils;

/**
 * Factory for {@linkplain Plan} instances when neccesary.
 * Calls the configred {@linkplain Balancer} for each group of duties of the same {@linkplain Pallet} 
 * Balancers use a {@linkplain Migrator} to plan its shippings, which are later written to the plan.
 * 
 * @author Cristian Gonzalez
 * @since Ene 4, 2015
 */
class ChangePlanFactory {

	private static final Logger logger = LoggerFactory.getLogger(ChangePlanFactory.class);

	private final Config config;
	private final String name = getClass().getSimpleName();

	ChangePlanFactory(final Config config) {
		this.config = config;
	}

	/** @return a plan if there're changes to apply or NULL if not */
	final ChangePlan create(final ShardingState scheme, final ChangePlan previous) {
		
		final UncommitedChanges snapshot = scheme.getUncommited().snapshot();
		ChangePlan changePlan = new ChangePlan(
				config.beatToMs(config.getDistributor().getPlanExpiration()), 
				config.getDistributor().getPlanMaxRetries());
		
		final Set<ShardEntity> creations = collectAsCreates(scheme, previous, snapshot, changePlan);		
		final Set<ShardEntity> deletions = collectAsRemoves(scheme, previous, snapshot, changePlan, creations);
		
		final Set<ShardEntity> ents = new HashSet<>();
		scheme.getCommitedState().findDuties(ents::add);
		ents.addAll(creations);
		boolean changes = false;
		final Map<String, List<ShardEntity>> schemeByPallets = ents.stream()
				.collect(Collectors.groupingBy(e -> e.getDuty().getPalletId()));
		if (schemeByPallets.isEmpty()) {
			logger.warn("{}: CommitedState and UncommitedChanges are empty. Nothing to balance (C:{}, R:{})", 
					name, creations.size(), deletions.size());
			changePlan = null;
		} else {
			try {
				for (final Map.Entry<String, List<ShardEntity>> e : schemeByPallets.entrySet()) {
					final Pallet pallet = scheme.getCommitedState().getPalletById(e.getKey()).getPallet();
					final Balancer balancer = Balancer.Directory.getByStrategy(pallet.getMetadata().getBalancer());
					logStatus(scheme, creations, deletions, e.getValue(), pallet, balancer);
					if (balancer != null) {
						final Migrator migra = balancePallet(scheme, pallet, balancer, creations, deletions);
						changes |= migra.write(changePlan);
					} else {
						if (logger.isInfoEnabled()) {
							logger.info("{}: Balancer not found ! {} set on Pallet: {} (curr size:{}) ", name,
								pallet.getMetadata().getBalancer(), pallet, Balancer.Directory.getAll().size());
						}
					}
				}
				// only when everything went well otherwise'd be lost
				scheme.getUncommited().clearAllocatedMissing();
				scheme.getUncommited().cleanAllocatedDanglings();
				if (!changes && deletions.isEmpty()) {
					changePlan = null;
				}
		    } catch (Exception e) {
		    	changePlan = null;
		    	logger.error("{}: Cancelling ChangePlan building", name, e);
			}
		}
		scheme.getUncommited().dropSnapshot();
		return changePlan;
	}

	private Set<ShardEntity> collectAsRemoves(
			final ShardingState scheme,
			final ChangePlan previousChange,
			final UncommitedChanges snapshot,
			final ChangePlan changePlan, 
			final Set<ShardEntity> dutyCreations) {
		
		final Set<ShardEntity> dutyDeletions = new HashSet<>();
		snapshot.findDutiesCrud(REMOVE::equals, null, crud-> {
			// as a CRUD a deletion lives in stage as a mark within an Opaque ShardEntity
			// we must now search for the real one
			final ShardEntity schemed = scheme.getCommitedState().getByDuty(crud.getDuty());
			if (schemed!=null) {
				// translate the REMOVAL event
				schemed.getJournal().addEvent(
						crud.getLastEvent(), 
						crud.getLastState(), 
						scheme.getCommitedState().findDutyLocation(schemed).getShardID(), 
						-1);
				dutyDeletions.add(schemed);
				// prevail user's deletion op. over clustering restore/creation
				dutyCreations.remove(schemed);
			}
		});
		restorePendings(previousChange, dutyDeletions::add, 
				d->d.getLastEvent()==REMOVE || d.getLastEvent()==DETACH);
		
		// lets add those duties of a certain deleting pallet
		snapshot.findPalletsCrud(REMOVE::equals, PREPARED::equals, p-> {
			scheme.getCommitedState().findDutiesByPallet(p.getPallet(), dutyDeletions::add);
		});
		shipDeletions(scheme, changePlan, dutyDeletions);
		return dutyDeletions;
	}

	private Set<ShardEntity> collectAsCreates(final ShardingState scheme,
			final ChangePlan previousChange,
			final UncommitedChanges snapshot,
			final ChangePlan changePlan) {
		// recently fallen shards
		addMissingAsCrud(scheme, changePlan);
		final Set<ShardEntity> dutyCreations = new HashSet<>();
		snapshot.findDutiesCrud(CREATE::equals, null, dutyCreations::add);
		// add danglings as creations prior to migrations
		for (ShardEntity d: snapshot.getDutiesDangling()) {
			dutyCreations.add(ShardEntity.Builder.builderFrom(d).build());
		}
		
		// add previous fallen and never confirmed migrations
		restorePendings(previousChange, dutyCreations::add, 
				d->d.getLastEvent()==CREATE || d.getLastEvent()==ATTACH);
		return dutyCreations;
	}

	private static final Migrator balancePallet(
			final ShardingState partition, 
			final Pallet pallet, 
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
		final Map<NetworkLocation, Set<Duty>> scheme = new TreeMap<>();
		
		// add the currently distributed duties
		partition.getCommitedState().findShards(ShardState.ONLINE.filter(), shard-> {
			final Set<Duty> located = new HashSet<>();
			partition.getCommitedState().findDuties(shard, pallet, d-> {
				located.add(d.getDuty());
				sourceRefs.add(d);
			}); 
			scheme.put(new NetworkLocation(shard), located);
		});
		// add the currently distributed duties
		sourceRefs.addAll(removes);
		sourceRefs.addAll(adds);
		final Migrator migrator = new Migrator(partition, pallet, sourceRefs);
		final Map<EntityEvent, Set<Duty>> stage = new HashMap<>(2);
		stage.put(CREATE, refs(adds));
		stage.put(REMOVE, refs(removes));
		balancer.balance(pallet, scheme, stage, migrator);
		return migrator;
	}

	private static final Set<Duty> refs(final Set<ShardEntity> entities) {
		final Set<Duty> ret = new HashSet<>(entities.size());
		entities.forEach(e -> ret.add(e.getDuty()));
		return ret;
	}
	
	private void addMissingAsCrud(final ShardingState partition, final ChangePlan changePlan) {
	    final Collection<ShardEntity> missing = partition.getUncommited().snapshot().getDutiesMissing();
		for (final ShardEntity missed : missing) {
			final Shard lazy = partition.getCommitedState().findDutyLocation(missed);
			if (logger.isDebugEnabled()) {
				logger.debug("{}: Registering {}, missing Duty: {}", name,
					lazy == null ? "unattached" : "from falling Shard: " + lazy, missed);
			}
			if (lazy != null) {
				partition.getCommitedState().write(missed, lazy, REMOVE, ()-> {
					// missing duties are a confirmation per-se from the very shards,
					// so the ptable gets fixed right away without a realloc.
					missed.getJournal().addEvent(REMOVE, CONFIRMED,lazy.getShardID(),changePlan.getId());					
				});
			}
			missed.getJournal().addEvent(CREATE, PREPARED,null,changePlan.getId());
			partition.getUncommited().snapshot().addCrudDuty(missed);
		}
		if (!missing.isEmpty()) {
			if (logger.isInfoEnabled()) {
				logger.info("{}: Registered {} dangling duties {}", name, missing.size(), toStringIds(missing));
			}
		}
	}

	/*
	 * check waiting duties never confirmed (for fallen shards as previous
	 * target candidates)
	 */
	private void restorePendings(final ChangePlan previous, final Consumer<ShardEntity> c, final Predicate<ShardEntity> p) {
		if (previous != null 
				&& previous.getResult().isClosed() 
				&& !previous.getResult().isSuccess()) {
			int rescued = previous.findAllNonConfirmedFromAllDeliveries(d->{
				if (p.test(d)) {
					c.accept(d);
				}
			});
			if (rescued ==0 && logger.isInfoEnabled()) {
				logger.info("{}: Previous change although unfinished hasnt waiting duties", name);
			} else {
				if (logger.isInfoEnabled()) {
					logger.info("{}: Previous change's unfinished business saved as Dangling: {}", name, rescued);
				}
			}
		}
	}

	/* by user deleted */
	private final void shipDeletions(final ShardingState partition, final ChangePlan changePlan,
			final Set<ShardEntity> deletions) {

		for (final ShardEntity deletion : deletions) {
			final Shard shard = partition.getCommitedState().findDutyLocation(deletion);
			deletion.getJournal().addEvent(DETACH, 
					PREPARED,
					shard.getShardID(),
					changePlan.getId());
			changePlan.ship(shard, deletion);
			if (logger.isInfoEnabled()) {
				logger.info("{}: Shipped {} from: {}, Duty: {}", name, DETACH, shard.getShardID(),
					deletion.toBrief());
			}
		}
	}

	private void logStatus(
			final ShardingState partition, 
			final Set<ShardEntity> dutyCreations, 
			final Set<ShardEntity> dutyDeletions,
			final List<ShardEntity> duties,
			final Pallet pallet, 
			final Balancer balancer) {
		
		if (!logger.isInfoEnabled()) {
			return;
		}
		
		logger.info(LogUtils.titleLine(LogUtils.HYPHEN_CHAR, "Building Pallet: %s for %s", pallet.getId(), 
				balancer.getClass().getSimpleName()));
		final double[] clusterCapacity = new double[1];
		partition.getCommitedState().findShards(ShardState.ONLINE.filter(), node-> {
			final Capacity cap = node.getCapacities().get(pallet);
			final double currTotal = cap == null ? 0 :  cap.getTotal();
			logger.info("{}: Capacity Shard {} : {}", name, node.toString(), currTotal);
			clusterCapacity[0] += currTotal;
		});
		logger.info("{}: Total cluster capacity: {}", name, clusterCapacity);
		logger.info("{}: RECKONING #{}; +{}; -{} duties: {}", name,
			new CommitedState.SchemeExtractor(partition.getCommitedState())
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
