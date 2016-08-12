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
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.tilt.minka.api.Config;
import io.tilt.minka.core.leader.PartitionTable;
import io.tilt.minka.core.leader.distributor.Balancer.BalanceStrategy;
import io.tilt.minka.domain.EntityEvent;
import io.tilt.minka.domain.Shard;
import io.tilt.minka.domain.ShardEntity;
import io.tilt.minka.domain.ShardEntity.State;
import io.tilt.minka.utils.CircularCollection;

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

		public final Reallocation process(
				final Map<BalanceStrategy, Balancer> balancerMap, 
				final PartitionTable table, 
				final Reallocation previousChange) {
			final Reallocation realloc = new Reallocation();
			final List<Shard> onlineShards = table.getShardsByState(ONLINE);
			// recently fallen shards
			final Set<ShardEntity> dangling = table.getDutiesDangling();
			registerMissing(table, realloc, table.getDutiesMissing());
			// add previous fallen and never confirmed migrations
			dangling.addAll(restoreUnfinishedBusiness(previousChange));
			// add danglings as creations prior to migrations
			final List<ShardEntity> danglingAsCreations = new ArrayList<>();
			dangling.forEach(i -> danglingAsCreations.add(ShardEntity.copy(i)));
			
			final Set<ShardEntity> creations = table.getDutiesCrudByFilters(EntityEvent.CREATE, State.PREPARED);
			creations.addAll(danglingAsCreations);
			final Set<ShardEntity> deletions = table.getDutiesCrudByFilters(EntityEvent.DELETE, State.PREPARED);
			final int accounted = table.getAccountConfirmed();
			// 1st step: delete all
			registerDeletions(table, realloc, deletions); 
			// el unico q ponia las dangling en deletions era el EvenBalancer.. .(?) lo dejo stand-by
			// despues todos agregaban las dangling como creations... se vino para aca.
			// balance per pallet
			final PalletCollector collector = new PalletCollector(creations, table.getPallets());
			final Iterator<Set<ShardEntity>> it = collector.getIterator();
			while (it.hasNext()) {
				Set<ShardEntity> palletGroup = it.next();
				final ShardEntity pallet = collector.getPallet(palletGroup.iterator().next().getDuty().getPalletId());
				final Balancer balancer = balancerMap.get(pallet.getPallet().getBalanceStrategy());
				final Set<ShardEntity> duties = collector.getDuties(pallet);
				logger.info("{}: Calling balancer {} for Pallet: {} with Duties {}", getClass().getSimpleName(), 
						balancer.getClass().getSimpleName(), pallet, ShardEntity.toStringIds(duties));
				
				balancer.balance(pallet.getPallet(), table, realloc, onlineShards, duties, deletions, accounted);
			}
			
			
			return realloc;
		}

		protected static void registerMissing(final PartitionTable table, final Reallocation realloc,
				final Set<ShardEntity> missing) {
			for (final ShardEntity missed : missing) {
				final Shard lazy = table.getDutyLocation(missed);
				logger.info("{}: Registering from {}Shard: {}, a dangling Duty: {}", Arranger.class,
							lazy == null ? "fallen " : "", lazy, missed);
				if (lazy != null) {
						// missing duties are a confirmation per-se from the very shards,
						// so the ptable gets fixed right away without a realloc.
						missed.registerEvent(EntityEvent.DELETE, State.CONFIRMED);
						table.confirmDutyAboutShard(missed, lazy);
				}
				missed.registerEvent(EntityEvent.CREATE, State.PREPARED);
				table.addCrudDuty(missed);
			}
			if (!missing.isEmpty()) {
				logger.info("{}: Registered {} dangling duties from fallen Shard/s, {}", Arranger.class,
							missing.size(), missing);
			}
			// clear it or nobody will
			missing.clear();
		}

		/*
		 * check waiting duties never confirmed (for fallen shards as previous
		 * target candidates)
		 */
		protected List<ShardEntity> restoreUnfinishedBusiness(final Reallocation previousChange) {
			List<ShardEntity> unfinishedWaiting = new ArrayList<>();
			if (previousChange != null && !previousChange.isEmpty() && !previousChange.hasCurrentStepFinished()
						&& !previousChange.hasFinished()) {
				previousChange.getGroupedIssues().keys()
							/*
							 * .stream() no siempre la NO confirmacion sucede sobre un
							 * fallen shard .filter(i->i.getServiceState()==QUITTED)
							 * .collect(Collectors.toList())
							 */
							.forEach(i -> previousChange.getGroupedIssues().get(i).stream()
										.filter(j -> j.getState() == State.SENT).forEach(j -> unfinishedWaiting.add(j)));
				if (unfinishedWaiting.isEmpty()) {
						logger.info("{}: Previous change although unfinished hasnt waiting duties",
								getClass().getSimpleName());
				} else {
						logger.info("{}: Previous change's unfinished business saved as Dangling: {}",
								getClass().getSimpleName(), unfinishedWaiting.toString());
				}
			}
			return unfinishedWaiting;
		}

		/**
		 * put new duties into receptive shards willing to add
		 */
		protected static void registerCreations(final Set<ShardEntity> duties, final Reallocation realloc,
				CircularCollection<Shard> receptiveCircle) {

			for (ShardEntity duty : duties) {
				Shard target = receptiveCircle.next();
				realloc.addChange(target, duty);
				duty.registerEvent(EntityEvent.ASSIGN, State.PREPARED);
				logger.info("{}: Assigning to shard: {}, duty: {}", Arranger.class, target.getShardID(),
							duty.toBrief());
			}
		}

		/* by user deleted */
		private void registerDeletions(final PartitionTable table, final Reallocation realloc,
				final Set<ShardEntity> deletions) {

			for (final ShardEntity deletion : deletions) {
				Shard shard = table.getDutyLocation(deletion);
				deletion.registerEvent(EntityEvent.UNASSIGN, State.PREPARED);
				realloc.addChange(shard, deletion);
				logger.info("{}: Deleting from: {}, Duty: {}", getClass().getSimpleName(), shard.getShardID(),
							deletion.toBrief());
			}
		}

		protected Config getConfig() {
			return this.config;
		}

}
