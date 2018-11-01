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
import io.tilt.minka.core.leader.balancer.Balancer;
import io.tilt.minka.core.leader.balancer.Spot;
import io.tilt.minka.core.leader.data.CommittedState;
import io.tilt.minka.core.leader.data.DirtyState;
import io.tilt.minka.core.leader.data.Scheme;
import io.tilt.minka.core.task.LeaderAware;
import io.tilt.minka.domain.EntityEvent;
import io.tilt.minka.domain.EntityState;
import io.tilt.minka.domain.Heartbeat;
import io.tilt.minka.domain.ShardEntity;
import io.tilt.minka.model.Duty;
import io.tilt.minka.model.Pallet;
import io.tilt.minka.shard.ShardCapacity;
import io.tilt.minka.shard.ShardState;
import io.tilt.minka.utils.LogUtils;

/**
 * Factory for {@linkplain ChangePlan} instances when neccesary.
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
		final long now = System.currentTimeMillis();
		final DirtyState snapshot = scheme.getDirty().snapshot();
		ChangePlan plan = new ChangePlan(
				config.beatToMs(config.getDistributor().getPlanExpiration()), 
				config.getDistributor().getPlanMaxRetries(),
				previous==null ? 0 : previous.getVersion());
		
		// to record initial pid and detect lazy surviving followers
		if (scheme.getCurrentPlan() == null && previous == null) {
			scheme.setFirstPlanId(plan.getId());
		}
		final boolean any = detectApplyFeatures(previous, plan, snapshot, scheme.getCommitedState());

		final DirtyCompiler compiler = new DirtyCompiler(scheme.getCommitedState(), previous, plan, snapshot);
		final Set<ShardEntity> ents = new HashSet<>();
		scheme.getCommitedState().findDuties(ents::add);
		ents.addAll(compiler.getCreations());
		final Map<String, List<ShardEntity>> schemeByPallets = ents.stream()
				.collect(Collectors.groupingBy(e -> e.getDuty().getPalletId()));
		if (schemeByPallets.isEmpty()) {
			logger.warn("{}: CommittedState and DirtyState are empty. Nothing to balance (C:{}, R:{})", 
					name, compiler.getCreations().size(), compiler.getDeletions().size());
			plan = null;
		} else {
			if (!build(scheme, plan, compiler, schemeByPallets)) {
				plan = null;
			} else if (!any) {
				plan.addFeature(ChangeFeature.REBALANCE);
			}
		}
		scheme.getDirty().dropSnapshotToRunning();
		logger.info("Factory: {}", System.currentTimeMillis() - now);
		return plan;
	}
	
	/** for monitoring only - has no impact */
	private boolean detectApplyFeatures(
			final ChangePlan prev, 
			final ChangePlan plan, 
			final DirtyState snapshot, 
			final CommittedState state) {
		
		boolean[] any = {false};
		if (snapshot.commitRequestsSize()>0) {
			any[0] |= plan.addFeature(ChangeFeature.COMMIT_REQUEST);
		}
		if (snapshot.isLimitedPromotion()) {
			any[0] |= plan.addFeature(ChangeFeature.LIMITED_PROMOTION);
		}
		// care only features appeared on HBs out of last plan
		if (prev!=null) {
			final long since = prev.getCreation().toEpochMilli();
			state.findShards(null, shard-> {
				for (final Heartbeat hb: shard.getHeartbeats().values()) {
					if (hb.getReception().isAfter(since) && hb.getFeature()!=null) {
						any[0] |= plan.addFeature(hb.getFeature());
					}
				}
			});
		}
		for (ChangeFeature f: snapshot.getFeatures()) {
			any[0] |= plan.addFeature(f);
		}
		if (!snapshot.getDisturbance(EntityState.DANGLING).isEmpty()) {
			any[0] |= plan.addFeature(ChangeFeature.FIXES_DANGLING);
		}
		if (!snapshot.getDisturbance(EntityState.MISSING).isEmpty()) {
			any[0] |= plan.addFeature(ChangeFeature.FIXES_MISSING);
		}
		return any[0];
	}

	
	private boolean build(
			final Scheme scheme, final ChangePlan changePlan,
			final DirtyCompiler compiler, 
			final Map<String, List<ShardEntity>> schemeByPallets) {
		try {
			boolean changes = false;
			final Replicator replicator = new Replicator(aware.getLeaderShardId(), scheme);
			for (final Map.Entry<String, List<ShardEntity>> e : schemeByPallets.entrySet()) {
				final Pallet pallet = scheme.getCommitedState().getPalletById(e.getKey()).getPallet();
				final Balancer balancer = Balancer.Directory.getByStrategy(pallet.getMetadata().getBalancer());
				logStatus(scheme, compiler.getCreations(), compiler.getDeletions(), e.getValue(), pallet, balancer);
				if (balancer != null) {
					final Migrator migra = balance(scheme, pallet, balancer, compiler.getCreations(), compiler.getDeletions());
					changes |=migra.write(changePlan);
					if (replicator.write(changePlan, compiler, pallet)) {
						changes = true;
						changePlan.addFeature(ChangeFeature.REPLICATION_EVENTS);
					}
				} else {
					logger.warn("{}: Balancer not found ! {} set on Pallet: {} (curr size:{}) ", name,
						pallet.getMetadata().getBalancer(), pallet, Balancer.Directory.getAll().size());
				}
			}
			
			// only when everything went well otherwise'd be lost
			scheme.getDirty().cleanAllocatedDisturbance(EntityState.DANGLING, null);
			scheme.getDirty().cleanAllocatedDisturbance(EntityState.MISSING, null);
			if (!changes && compiler.getDeletions().isEmpty()) {
				return false;
			}
		} catch (Exception e) {
			logger.error("{}: Cancelling ChangePlan building", name, e);
			return false;
		}
		return true;
	}

	private static final Migrator balance(
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
			logger.info("{}: Capacity Shard {} : {}", name, node.toString(), currTotal);
			clusterCapacity[0] += currTotal;
		});
		logger.info("{}: Cluster capacity: {}", name, clusterCapacity);
		
		logger.info("{}: Living #{}", name,
			new CommittedState.SchemeExtractor(partition.getCommitedState()).getAccountConfirmed(pallet));
		
		final Set<ShardEntity> creapa = dutyCreations.stream()
				.filter(d->d.getDuty().getPalletId().equals(pallet.getId()))
				.collect(Collectors.toSet());
		if (!creapa.isEmpty() && logger.isInfoEnabled()) {
			logger.info("{}: Add +{}: {}", name, creapa.size(), ShardEntity.toStringIds(creapa));
		}
		final Set<ShardEntity> delepa = dutyDeletions.stream()
				.filter(d->d.getDuty().getPalletId().equals(pallet.getId()))
				.collect(Collectors.toSet());
		if (!delepa.isEmpty() && logger.isInfoEnabled()) {
			logger.info("{}: Rem -{}: {}", name, delepa.size(), ShardEntity.toStringIds(delepa));
		}
	}
	
}
