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

import static io.tilt.minka.domain.ShardEntity.State.CONFIRMED;
import static io.tilt.minka.domain.ShardEntity.State.PREPARED;
import static io.tilt.minka.domain.ShardEntity.State.SENT;
import static io.tilt.minka.domain.ShardState.ONLINE;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.Validate;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;

import io.tilt.minka.api.Config;
import io.tilt.minka.api.DependencyPlaceholder;
import io.tilt.minka.api.Duty;
import io.tilt.minka.api.DutyBuilder;
import io.tilt.minka.api.Pallet;
import io.tilt.minka.api.Pallet.Storage;
import io.tilt.minka.broker.EventBroker;
import io.tilt.minka.core.leader.Auditor;
import io.tilt.minka.core.leader.EntityDao;
import io.tilt.minka.core.leader.PartitionTable;
import io.tilt.minka.core.leader.Status;
import io.tilt.minka.core.leader.PartitionTable.ClusterHealth;
import io.tilt.minka.core.task.LeaderShardContainer;
import io.tilt.minka.core.task.Scheduler;
import io.tilt.minka.core.task.Scheduler.Agent;
import io.tilt.minka.core.task.Scheduler.Frequency;
import io.tilt.minka.core.task.Scheduler.PriorityLock;
import io.tilt.minka.core.task.Semaphore.Action;
import io.tilt.minka.core.task.impl.ServiceImpl;
import io.tilt.minka.domain.EntityEvent;
import io.tilt.minka.domain.Shard;
import io.tilt.minka.domain.ShardEntity;
import io.tilt.minka.domain.ShardID;
import io.tilt.minka.utils.LogUtils;

/**
 * Periodically runs specified {@linkplain Balancer}'s over {@linkplain Pallet}'s
 * and drive the {@linkplain Roadmap} object if any, transporting duties.
 * 
 * @author Cristian Gonzalez
 * @since Nov 17, 2015
 */
public class Distributor extends ServiceImpl {

	private final Logger logger = LoggerFactory.getLogger(getClass());

	private final Config config;
	private final Scheduler scheduler;
	private final EventBroker eventBroker;
	private final PartitionTable partitionTable;
	private final Auditor auditor;
	private final ShardID shardId;
	private final EntityDao entityDao;
	private final DependencyPlaceholder dependencyPlaceholder;
	private final LeaderShardContainer leaderShardContainer;

	private int distributionCounter;
	private boolean initialAdding;
	private int counter;
	private Arranger arranger;

	private final Agent distributor;

	public Distributor(final Config config, final Scheduler scheduler, final EventBroker eventBroker,
			final PartitionTable partitionTable, final Auditor accounter, final ShardID shardId,
			final EntityDao dutyDao, final DependencyPlaceholder dependencyPlaceholder, 
			final LeaderShardContainer leaderShardContainer) {

		this.config = config;
		this.scheduler = scheduler;
		this.eventBroker = eventBroker;
		this.partitionTable = partitionTable;
		this.auditor = accounter;
		this.shardId = shardId;
		this.entityDao = dutyDao;
		this.leaderShardContainer = leaderShardContainer;

		if (config.getConsistency().getDutyStorage() == Storage.CLIENT_DEFINED) {
			Validate.notNull(dependencyPlaceholder, "When Minka not in Storage mode: a Partition Master is required");
		} else {
			Validate.isTrue(dependencyPlaceholder.getMaster() == null,
					"When Minka in Storage mode: a Partition Master must not be exposed");
		}

		this.dependencyPlaceholder = dependencyPlaceholder;
		this.initialAdding = true;

		this.distributor = scheduler.getAgentFactory()
				.create(Action.DISTRIBUTOR, PriorityLock.MEDIUM_BLOCKING, Frequency.PERIODIC, () -> periodicCheck())
				.delayed(config.getDistributor().getStartDelayMs()).every(config.getDistributor().getDelayMs()).build();

		this.arranger = new Arranger(config);
	}

	@Override
	public void start() {
		logger.info("{}: Starting. Scheduling constant periodic check", getName());
		scheduler.schedule(distributor);
	}

	@Override
	public void stop() {
		logger.info("{}: Stopping", getName());
		this.scheduler.stop(distributor);
	}

	private void periodicCheck() {
		try {
			FileUtils.writeStringToFile(new File("/tmp/algo-"+counter), 
					Status.toJson(partitionTable), "utf-8", false);
		} catch (JsonProcessingException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		try {
			// also if this's a de-frozening thread
			if (!leaderShardContainer.imLeader()) {
				logger.warn("{}: ({}) Posponing distribution: not leader anymore ! ", getName(), shardId);
				return;
			}
			// skip if unstable unless a roadmap in progress or expirations will occurr and dismiss
			if (auditor.getCurrentRoadmap().isEmpty()
					&& partitionTable.getVisibilityHealth() == ClusterHealth.UNSTABLE) {
				logger.warn("{}: ({}) Posponing distribution until reaching cluster stability (", getName(), shardId);
				return;
			}

			showStatus();
			final long now = System.currentTimeMillis();
			final int online = partitionTable.getStage().getShardsByState(ONLINE).size();
			final int min = config.getShepherd().getMinShardsOnlineBeforeSharding();
			if (online >= min) {
				if (checkWithStorageWhenAllOnlines()) {
					final Roadmap currentRoadmap = auditor.getCurrentRoadmap();
					if (currentRoadmap.isEmpty()) {
						buildAndDriveRoadmap(null);
					} else if (!currentRoadmap.hasFinished() && currentRoadmap.hasCurrentStepFinished()) {
						forwardRoadmap();
					} else {
						checkExpiration(now, currentRoadmap);
					}
				}
			} else {
				logger.info("{}: balancing posponed: not enough online shards (min:{}, now:{})", getName(), min,
						online);
			}
			communicateUpdates();
		} catch (Exception e) {
			logger.error("{}: Unexpected ", getName(), e);
		} finally {
			logger.info(LogUtils.END_LINE);
		}
	}
	
	private void showStatus() {
		StringBuilder title = new StringBuilder();
		title.append("Distributor (i").append(++distributionCounter)
			.append(" by Leader: ").append(shardId.toString());
		logger.info(LogUtils.titleLine(title.toString()));
		partitionTable.logStatus();
	}

	private void buildAndDriveRoadmap(final Roadmap previousChange) {
		final Roadmap roadmap = arranger.process(partitionTable, previousChange);

		auditor.addRoadmap(roadmap);
		auditor.cleanTemporaryDuties();
		if (!roadmap.isEmpty()) {
			this.partitionTable.setWorkingHealth(ClusterHealth.UNSTABLE);
			forwardRoadmap();
			logger.info("{}: Balancer generated {} issues on change: {}", getName(), roadmap.getGroupedIssues().size(),
					roadmap.getId());
		} else {
			this.partitionTable.setWorkingHealth(ClusterHealth.STABLE);
			logger.info("{}: {} Balancer without change", getName(), LogUtils.BALANCED_CHAR);
		}
	}

	private void checkExpiration(final long now, final Roadmap currentRoadmap) {
		final DateTime created = currentRoadmap.getCreation();
		final int maxSecs = config.getDistributor().getReallocationExpirationSec();
		final DateTime expiration = created.plusSeconds(maxSecs);
		if (expiration.isBefore(now)) {
			if (currentRoadmap.getRetryCount() == config.getDistributor().getReallocationMaxRetries()) {
				logger.info("{}: Abandoning change expired ! (max secs:{}) ", getName(), maxSecs);
				buildAndDriveRoadmap(currentRoadmap);
			} else {
				currentRoadmap.incrementRetry();
				logger.info("{}: ReSending change expired: Retry {} (max secs:{}) ", getName(),
						currentRoadmap.getRetryCount(), maxSecs);
				driveRoadmap();
			}
		} else {
			logger.info("{}: balancing posponed: an existing change in progress ({}'s to expire)", getName(),
					(expiration.getMillis() - now) / 1000);
		}
	}

	/* read from storage only first time */
	private boolean checkWithStorageWhenAllOnlines() {
		if (initialAdding || (config.getDistributor().isReloadDutiesFromStorage()
				&& config.getDistributor().getReloadDutiesFromStorageEachPeriods() == counter++)) {
			counter = 0;
			Set<Duty<?>> duties = reloadDutiesFromStorage();
			Set<Pallet<?>> pallets = reloadPalletsFromStorage();
			if (duties == null || duties.isEmpty() || pallets == null || pallets.isEmpty()) {
				logger.warn("{}: distribution posponed: {} hasn't return any entities (pallets = {}, duties: {})",getName(),
					config.getConsistency().getDutyStorage() == Storage.MINKA_MANAGEMENT ? "Minka storage" : "PartitionMaster",
					duties, pallets);
				return false;
			} else {
				try {
					duties.forEach(d -> DutyBuilder.validateBuiltParams(d));
				} catch (Exception e) {
					logger.error("{}: Distribution suspended - Duty Built construction problem: ", getName(), e);
					return false;
				}
				logger.info("{}: {} reported {} entities for sharding...", getName(),
					config.getConsistency().getDutyStorage() == Storage.MINKA_MANAGEMENT ? "DutyDao" : "PartitionMaster",
					duties.size());
				final List<Pallet<?>> copyP = Lists.newArrayList(pallets);
				auditor.registerPalletsFromSource(copyP);
				final List<Duty<?>> copy = Lists.newArrayList(duties);
				auditor.registerDutiesFromSource(copy);
				initialAdding = false;
			}
			if (partitionTable.getNextStage().getDutiesCrud().isEmpty()) {
				logger.warn("{}: Aborting first distribution cause of no duties !", getName());
				return false;
			}
		} else {
			checkConsistencyState();
		}
		return true;
	}

	private void checkConsistencyState() {
		if (config.getDistributor().isRunConsistencyCheck() && auditor.getCurrentRoadmap().isEmpty()) {
			// only warn in case there's no reallocation ahead
			final Set<ShardEntity> currently = partitionTable.getStage().getDutiesAllByShardState(null, null);
			final Set<ShardEntity> sortedLog = new TreeSet<>();
			reloadDutiesFromStorage().stream().filter(duty -> !currently.contains(ShardEntity.create(duty)))
					.forEach(duty -> sortedLog.add(ShardEntity.create(duty)));
			if (!sortedLog.isEmpty()) {
				logger.error("{}: Consistency check: Absent duties going as Missing [ {}]", getName(),
						ShardEntity.toStringIds(sortedLog));
				partitionTable.getNextStage().getDutiesMissing().addAll(sortedLog);
			}
		}
	}

	@SuppressWarnings("unchecked")
	private Set<Duty<?>> reloadDutiesFromStorage() {
		Set<Duty<?>> duties = null;
		try {
			if (config.getConsistency().getDutyStorage() == Storage.MINKA_MANAGEMENT) {
				duties = entityDao.loadDutySnapshot();
			} else {
				duties = dependencyPlaceholder.getMaster().loadDuties();
			}
		} catch (Exception e) {
			logger.error("{}: {} throwed an Exception", getName(),
					config.getConsistency().getDutyStorage() == Storage.MINKA_MANAGEMENT ? "DutyDao" : "PartitionMaster", e);
		}
		return duties;
	}

	@SuppressWarnings("unchecked")
	private Set<Pallet<?>> reloadPalletsFromStorage() {
		Set<Pallet<?>> pallets = null;
		try {
			if (config.getConsistency().getDutyStorage() == Storage.MINKA_MANAGEMENT) {
				pallets = entityDao.loadPalletSnapshot();
			} else {
				pallets = dependencyPlaceholder.getMaster().loadPallets();
			}
		} catch (Exception e) {
			logger.error("{}: {} throwed an Exception", getName(),
					config.getConsistency().getDutyStorage()== Storage.MINKA_MANAGEMENT ? "DutyDao" : "PartitionMaster", e);
		}
		return pallets;
	}

	private void communicateUpdates() {
		final Set<ShardEntity> updates = partitionTable.getNextStage().getDutiesCrud().stream()
				.filter(i -> i.getDutyEvent() == EntityEvent.UPDATE && i.getState() == PREPARED)
				.collect(Collectors.toCollection(HashSet::new));
		if (!updates.isEmpty()) {
			for (ShardEntity updatedDuty : updates) {
				Shard location = partitionTable.getStage().getDutyLocation(updatedDuty);
				if (transport(updatedDuty, location)) {
					updatedDuty.registerEvent(SENT);
				}
			}
		}
	}

	/* move to next issue step and send them all */
	private void forwardRoadmap() {
		auditor.getCurrentRoadmap().nextStep();
		if (auditor.getCurrentRoadmap().hasCurrentStepFinished()) {
			logger.info("{}: Skipping current empty step", getName());
			auditor.getCurrentRoadmap().nextStep();
		}
		driveRoadmap();
	}

	private void driveRoadmap() {
		final Multimap<Shard, ShardEntity> issues = auditor.getCurrentRoadmap().getGroupedIssues();
		final Iterator<Shard> it = issues.keySet().iterator();
		while (it.hasNext()) {
			final Shard shard = it.next();
			// check it's still in ptable
			if (partitionTable.getStage().getShardsByState(ONLINE).contains(shard)) {
				final Collection<ShardEntity> duties = auditor.getCurrentRoadmap().getGroupedIssues().get(shard);
				if (!duties.isEmpty()) {
					if (transport(duties, shard)) {
						// dont mark to wait for those already confirmed (from fallen shards)
						duties.forEach(duty -> duty.registerEvent((duty.getState() == PREPARED ? SENT : CONFIRMED)));
					} else {
						logger.error("{}: Couldnt transport current issues !!!", getName());
					}
				} else {
					throw new IllegalStateException("No duties grouped by shard at Reallocation !!");
				}
			} else {
				logger.error("{}: PartitionTable lost transport's target shard: {}", getName(), shard);
			}
		}
	}

	private boolean transport(final ShardEntity duty, final Shard shard) {
		logger.info("{}: Transporting (update) Duty: {} to Shard: {}", getName(), duty.toString(), shard.getShardID());
		return eventBroker.postEvent(shard.getBrokerChannel(), duty);
	}

	private boolean transport(final Collection<ShardEntity> duties, final Shard shard) {
		final Set<ShardEntity> sortedLog = new TreeSet<>(duties);
		logger.info("{}: Transporting to Shard: {} Duties ({}): {}", getName(), shard.getShardID(), duties.size(),
				ShardEntity.toStringIds(sortedLog));
		return eventBroker.postEvents(shard.getBrokerChannel(), new ArrayList<>(duties));
	}

}
