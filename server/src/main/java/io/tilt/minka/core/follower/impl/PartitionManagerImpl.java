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
package io.tilt.minka.core.follower.impl;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.tilt.minka.api.DependencyPlaceholder;
import io.tilt.minka.api.Duty;
import io.tilt.minka.api.Pallet;
import io.tilt.minka.core.follower.HeartbeatFactory;
import io.tilt.minka.core.follower.PartitionManager;
import io.tilt.minka.core.task.LeaderShardContainer;
import io.tilt.minka.core.task.Scheduler;
import io.tilt.minka.core.task.Scheduler.PriorityLock;
import io.tilt.minka.core.task.Scheduler.Synchronized;
import io.tilt.minka.core.task.Semaphore.Action;
import io.tilt.minka.domain.DomainInfo;
import io.tilt.minka.domain.ShardCommand;
import io.tilt.minka.domain.ShardEntity;
import io.tilt.minka.domain.ShardedPartition;

public class PartitionManagerImpl implements PartitionManager {

	private final Logger logger = LoggerFactory.getLogger(getClass());

	private final DependencyPlaceholder dependencyPlaceholder;
	private final ShardedPartition partition;
	private final HeartbeatFactory heartbeatFactory;
	private final Scheduler scheduler;
	private final Synchronized releaser;

	private DomainInfo domain;
	
	public PartitionManagerImpl(
			final DependencyPlaceholder dependencyPlaceholder, 
			final ShardedPartition partition, 
			final Scheduler scheduler, 
			final LeaderShardContainer leaderShardContainer, 
			final HeartbeatFactory heartbeatFactory) {
		
		super();
		this.dependencyPlaceholder = dependencyPlaceholder;
		this.partition = partition;
		this.scheduler = scheduler;
		this.heartbeatFactory = heartbeatFactory;
		this.releaser = scheduler.getFactory().build(
				Action.INSTRUCT_DELEGATE, 
				PriorityLock.MEDIUM_BLOCKING,
				() -> releaseAll());
	}

	public Void releaseAllOnPolicies() {
		//scheduler.tryBlocking(Semaphore.Action.INSTRUCT_DELEGATE, ()-> {
		scheduler.run(releaser);
		return null;
	}

	public Void releaseAll() {
		if (logger.isInfoEnabled()) {
			logger.info("{}: ({}) Instructing PartitionDelegate to RELEASE ALL", getClass().getSimpleName(), partition.getId());
		}
		dettach(partition.getDuties());
		partition.clean();
		return null;
	}

	// TODO refactory
	public Void finalized(final Collection<ShardEntity> duties) {
		for (ShardEntity duty : duties) {
			if (partition.contains(duty)) {
				if (logger.isInfoEnabled()) {
					logger.info("{}: ({}) Removing finalized Duty from Partition: {}", getClass().getSimpleName(),
						partition.getId(), duty.toBrief());
				}
				partition.remove(duty);
			} else {
				logger.error("{}: ({}) Unable to ACKNOWLEDGE for finalization a never taken Duty !: {}",
						getClass().getSimpleName(), partition.getId(), duty.toBrief());
			}
		}
		return null;
	}

	@SuppressWarnings("unchecked")
	// TODO refactory
	public Void update(final Collection<ShardEntity> duties) {
		for (ShardEntity entity : duties) {
			if (entity.getType()==ShardEntity.Type.DUTY) {
				if (partition.contains(entity)) {
					if (entity.getUserPayload() == null) {
						if (logger.isInfoEnabled()) {
							logger.info("{}: ({}) Instructing PartitionDelegate to UPDATE : {}", getClass().getSimpleName(),
								partition.getId(), entity.toBrief());
						}
						dependencyPlaceholder.getDelegate().update(entity.getDuty());
					} else {
						if (logger.isInfoEnabled()) {
							logger.info("{}: ({}) Instructing PartitionDelegate to RECEIVE: {} with Payload type {}",
								getClass().getSimpleName(), partition.getId(), entity.toBrief(),
								entity.getUserPayload().getClass().getName());
						}
						dependencyPlaceholder.getDelegate().transfer(entity.getDuty(), entity.getUserPayload());
					}
				} else {
					logger.error("{}: ({}) Unable to UPDATE a never taken Duty !: {}", getClass().getSimpleName(),
							partition.getId(), entity.toBrief());
				}
			} else if (entity.getType()==ShardEntity.Type.PALLET) {
				if (entity.getUserPayload() == null) {
					if (logger.isInfoEnabled()) {
						logger.info("{}: ({}) Instructing PartitionDelegate to UPDATE : {}", getClass().getSimpleName(),
							partition.getId(), entity.toBrief());
					}
					dependencyPlaceholder.getDelegate().update(entity.getPallet());
				} else {
					if (logger.isInfoEnabled()) {
						logger.info("{}: ({}) Instructing PartitionDelegate to RECEIVE: {} with Payload type {}",
							getClass().getSimpleName(), partition.getId(), entity.toBrief(),
							entity.getUserPayload().getClass().getName());
					}
					dependencyPlaceholder.getDelegate().transfer(entity.getDuty(), entity.getUserPayload());					
				}
			}
		}
		return null;
	}

	@SuppressWarnings("unchecked")
	public Void dettach(final Collection<ShardEntity> duties) {
		if (logger.isInfoEnabled()) {
			logger.info("{}: ({}) Instructing PartitionDelegate to RELEASE : {}", getClass().getSimpleName(),
				partition.getId(), ShardEntity.toStringBrief(duties));
		}
		
		try {
			dependencyPlaceholder.getDelegate().release(toSet(duties, duty -> {
				if (!partition.contains(duty)) {
					logger.error("{}: ({}) Unable to RELEASE a never taken Duty !: {}", getClass().getSimpleName(),
							partition.getId(), duty);
					return false;
				} else {
					return true;
				}
			}));
			duties.forEach(d->partition.remove(d));
			// remove pallets absent in duties
			final Set<ShardEntity> removing = partition.getPallets().stream()
				.filter(p->!partition.contains(p.getRelatedEntity()))
				.collect(Collectors.toSet());
			if (!removing.isEmpty()) {
				dependencyPlaceholder.getDelegate().releasePallet(removing);
			}
		} catch (Exception e) {
			logger.error("{}: ({}) Exception: {}", getClass().getSimpleName(), partition.getId(), e);
		}
		return null;
	}

	@SuppressWarnings("unchecked")
	public Void attach(final Collection<ShardEntity> duties) {
		/*
		 * if (scheduler.acquire(IdentifiedAction.build(RESERVE_DUTY,
		 * duty.getDuty().getId())) == GRANTED) {
		 */
		if (logger.isInfoEnabled()) {
			logger.info("{}: ({}) Instructing PartitionDelegate to TAKE: {}", getClass().getSimpleName(), partition.getId(),
				ShardEntity.toStringBrief(duties));
		}
		try {
			final Set<Pallet<?>> pallets = new HashSet<>();
			duties.stream().filter(d->partition.add(d))
				.forEach(d->pallets.add(d.getRelatedEntity().getPallet()));
			if (!pallets.isEmpty()) {
				dependencyPlaceholder.getDelegate().capturePallet(pallets);
			}
			dependencyPlaceholder.getDelegate().capture(toSet(duties, null));
			partition.addAllDuties(duties);
		} catch (Exception e) {
			logger.error("{}: ({}) Delegate thrown an Exception while Taking", getClass().getSimpleName(), 
					partition.getId(), e);
		}
		/*
		 * } else { logger.error(
		 * "{}: ShardID: {}, Unable to TAKE an already Locked Duty !: {}",
		 * getClass().getSimpleName(), partition.getId(), duty.toBrief()); //
		 * TODO todo mal reportar que no se puede tomar xq alguien la tiene q
		 * onda ??? }
		 */
		return null;
	}

	private Set<Duty<?>> toSet(final Collection<ShardEntity> duties, Predicate<ShardEntity> filter) {
		Set<Duty<?>> set = new HashSet<>(duties.size());
		for (ShardEntity dudty : duties) {
			if (filter == null || filter.test(dudty)) {
				set.add(dudty.getDuty());
			}
		}
		;
		return set;
	}

	public Void command(final ShardCommand op) {
		return null;
		/*
		 * if (op.getOperation() == Command.CLUSTER_CLEAN_SHUTDOWN) { stop();
		 * final Heartbeat lastGoodbye = this.heartpump.buildHeartbeat();
		 * lastGoodbye.setStateChange(ServiceState.OFFLINE);
		 * eventBroker.postEvent(eventBroker.buildToTarget(config,
		 * Channel.HEARTBEATS_TO_LEADER,
		 * leaderShardContainer.getLeaderShardId()), lastGoodbye); }
		 */
	}

	@Override
	public Void acknowledge(DomainInfo info) {
		this.domain = info;
		this.heartbeatFactory.setDomainInfo(domain);
		return null;
	}

}
