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

import java.util.List;

import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.tilt.minka.api.Config;
import io.tilt.minka.broker.EventBroker;
import io.tilt.minka.core.task.LeaderShardContainer;
import io.tilt.minka.core.task.Scheduler;
import io.tilt.minka.core.task.Scheduler.PriorityLock;
import io.tilt.minka.core.task.Semaphore.Action;
import io.tilt.minka.core.task.Service;
import io.tilt.minka.core.task.impl.ServiceImpl;
import io.tilt.minka.domain.NetworkShardID;

/**
 * An agnostic master service with the following responsibilities:
 * 1) listen to heartbeats from slaves and ensure there're no unassigned entities
 * 2) distributing and balancing sharded-entities to slaves
 * 
 * @author Cristian Gonzalez
 * @since Nov 7, 2015
 */
public class Leader extends ServiceImpl {

	private final Logger logger = LoggerFactory.getLogger(getClass());

	/* maintenance */
	private final Scheduler scheduler;
	private final NetworkShardID shardId;
	private final List<Service> services;
	private final Config config;
	private final LeaderShardContainer leaderShardContainer;
	private boolean served;

	public Leader(Config config, List<Service> services, Scheduler scheduler, NetworkShardID shardId,
			LeaderShardContainer leaderShardContainer, EventBroker eventBroker) {

		super();
		this.config = config;
		this.services = services;
		this.scheduler = scheduler;
		this.shardId = shardId;
		this.leaderShardContainer = leaderShardContainer;
	}

	public NetworkShardID getShardId() {
		return this.shardId;
	}

	@Override
	public void start() {
		try {
			//if (!locks.runOnLockRace(Names.getLeaderLockName(config.getServiceName()), ()-> {
			scheduler.run(scheduler.getFactory().build(Action.LEADERSHIP, PriorityLock.LOW_ON_PERMISSION, () -> {
				try {
					served = true;
					logger.info("{}: Registering as Leader at well after waiting {} msecs", getClass().getSimpleName(),
							super.getMillisSinceCreation());
					leaderShardContainer.setNewLeader(shardId);
					logger.info("{}: {} msec since load till leader election", getClass().getSimpleName(),
							(DateTime.now().getMillis() - config.loadTime.getMillis()));
					services.forEach(s -> s.init());
				} catch (Exception e) {
					logger.error("Unexpected error when starting service. Cannot procede", e);
				}
			}));/*
					 * { logger.error(
					 * "A blocking cluster operation is running to avoid Master start"
					 * ); //Distributed.stopCandidate(Names.getLeaderName(config.
					 * getServiceName()), false); // subscriptions never made }
					 */
		} catch (Exception e) {
			logger.error("Unexpected error when starting service. Cannot procede", e);
		} finally {

		}
	}

	@Override
	public void stop() {
		logger.info("{}: Stopping ({})", getClass().getSimpleName(), !served ? "never served" : "paid my duty");
		services.forEach(s -> s.destroy());
	}

}
