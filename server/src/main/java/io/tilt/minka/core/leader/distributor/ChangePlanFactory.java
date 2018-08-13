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

import static io.tilt.minka.domain.EntityEvent.CREATE;
import static io.tilt.minka.domain.EntityEvent.REMOVE;

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
import io.tilt.minka.core.leader.balancer.Balancer;
import io.tilt.minka.core.leader.balancer.Spot;
import io.tilt.minka.core.leader.data.CommitedState;
import io.tilt.minka.core.leader.data.Scheme;
import io.tilt.minka.core.leader.data.UncommitedChanges;
import io.tilt.minka.core.task.LeaderAware;
import io.tilt.minka.domain.EntityEvent;
import io.tilt.minka.domain.ShardEntity;
import io.tilt.minka.shard.ShardCapacity;
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
	private final LeaderAware aware;
	
	ChangePlanFactory(final Config config, final LeaderAware aware) {
		this.config = config;
		this.aware = aware;
	}

	/** @return a plan if there're changes to apply or NULL if not */
	ChangePlan create(final Scheme scheme, final ChangePlan previous) {
		final UncommitedChanges snapshot = scheme.getUncommited().snapshot();
		ChangePlan plan = new ChangePlan(
				config.beatToMs(config.getDistributor().getPlanExpiration()), 
				config.getDistributor().getPlanMaxRetries());
		
		// to record initial pid and detect lazy surviving followers
		if (scheme.getCurrentPlan() == null && previous == null) {
			scheme.setFirstPlanId(plan.getId());
		}

		final UncommitedCompiler compiler = new UncommitedCompiler(scheme.getCommitedState(), previous, plan, snapshot);
		
		final Set<ShardEntity> creations = compiler.collectCreations();
		final Set<ShardEntity> deletions = compiler.collectRemovals(creations);
		
		final Set<ShardEntity> ents = new HashSet<>();
		scheme.getCommitedState().findDuties(ents::add);
		ents.addAll(creations);
		final Map<String, List<ShardEntity>> schemeByPallets = ents.stream()
				.collect(Collectors.groupingBy(e -> e.getDuty().getPalletId()));
		if (schemeByPallets.isEmpty()) {
			logger.warn("{}: CommitedState and UncommitedChanges are empty. Nothing to balance (C:{}, R:{})", 
					name, creations.size(), deletions.size());
			plan = null;
		} else {
			if (!balanceByPallet(scheme, plan, creations, deletions, schemeByPallets)) {
				plan = null;
			}
		}
		scheme.getUncommited().dropSnapshot();
		return plan;
	}
	
	private boolean balanceByPallet(
			final Scheme scheme, final ChangePlan changePlan,
			final Set<ShardEntity> creations, final Set<ShardEntity> deletions, 
			final Map<String, List<ShardEntity>> schemeByPallets) {
		try {
			boolean changes = false;
			final ReplicationDispatcher replicator = new ReplicationDispatcher(scheme);
			for (final Map.Entry<String, List<ShardEntity>> e : schemeByPallets.entrySet()) {
				final Pallet pallet = scheme.getCommitedState().getPalletById(e.getKey()).getPallet();
				final Balancer balancer = Balancer.Directory.getByStrategy(pallet.getMetadata().getBalancer());
				logStatus(scheme, creations, deletions, e.getValue(), pallet, balancer);
				if (balancer != null) {
					final Migrator migra = balancePallet(scheme, pallet, balancer, creations, deletions);
					changes |= migra.write(changePlan);
					replicator.dispatchReplicas(changePlan, creations, deletions, aware.getLeaderShardId(), pallet);
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
				return false;
			}
		} catch (Exception e) {
			logger.error("{}: Cancelling ChangePlan building", name, e);
			return false;
		}
		return true;
	}

	private static final Migrator balancePallet(
			final Scheme partition, 
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
		final Map<Spot, Set<Duty>> scheme = new TreeMap<>();
		
		// add the currently distributed duties
		partition.getCommitedState().findShards(ShardState.ONLINE.filter(), shard-> {
			final Set<Duty> located = new HashSet<>();
			partition.getCommitedState().findDuties(shard, pallet, d-> {
				located.add(d.getDuty());
				sourceRefs.add(d);
			}); 
			scheme.put(new Spot(shard), located);
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
	

	private void logStatus(
			final Scheme partition, 
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
			final ShardCapacity cap = node.getCapacities().get(pallet);
			final double currTotal = cap == null ? 0 :  cap.getTotal();
			logger.info("{}: ShardCapacity Shard {} : {}", name, node.toString(), currTotal);
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
