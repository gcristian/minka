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


import java.util.Date;

import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.tilt.minka.api.Config;
import io.tilt.minka.broker.EventBroker;
import io.tilt.minka.core.leader.distributor.Distributor;
import io.tilt.minka.core.task.LeaderAware;
import io.tilt.minka.core.task.Scheduler;
import io.tilt.minka.core.task.Scheduler.PriorityLock;
import io.tilt.minka.core.task.Semaphore.Action;
import io.tilt.minka.shard.NetworkShardIdentifier;
import io.tilt.minka.core.task.Service;

/**
 * An agnostic master service with the following responsibilities:
 * 1) listen to heartbeats from slaves and ensure there're no unassigned entities
 * 2) distributing and balancing sharded-entities to slaves
 * 
 * @author Cristian Gonzalez
 * @since Nov 7, 2015
 */
public class Leader implements Service {

	public final static Logger logger = LoggerFactory.getLogger(Leader.class);

	private final Config config;
	private final ClusterProctor clusterProctor;
	private final Distributor distributor;
	private final FollowerEventsHandler followerEventsHandler;
	private final ClientEventsHandler clientEventsHandler;
	private final Scheduler scheduler;
	private final NetworkShardIdentifier shardId;
	private final LeaderAware leaderAware;

	private boolean served;
	private Date start;
	private Date stop;

	Leader(
			final Config config, 
			final ClusterProctor clusterProctor,
			final Distributor distributor,
			final FollowerEventsHandler followerEventsHandler,
			final ClientEventsHandler clientEventsHandler,
			final Scheduler scheduler, 
			final NetworkShardIdentifier shardId,
			final LeaderAware leaderAware, 
			final EventBroker eventBroker) {

		super();
		this.config = config;
		this.clusterProctor = clusterProctor;
		this.distributor = distributor;
		this.followerEventsHandler = followerEventsHandler;
		this.clientEventsHandler = clientEventsHandler;
		this.scheduler = scheduler;
		this.shardId = shardId;
		this.leaderAware = leaderAware;
	}

	public NetworkShardIdentifier getShardId() {
		return this.shardId;
	}
	
	@Override
	public boolean inService() {
		return start!=null;
	}
	
	@Override
	public void start() {
		try {
			//if (!locks.runOnLockRace(Names.getLeaderLockName(config.getServiceName()), ()-> {
			scheduler.run(scheduler.getFactory().build(
					Action.LEADERSHIP, 
					PriorityLock.LOW_ON_PERMISSION, 
					() -> {
				
				try {
					served = true;
					final Date start = new Date();
					if (stop==null) {
						final long w = System.currentTimeMillis() - start.getTime();
						logger.info("{}: Registering as Leader at well after waiting {} msecs", getName(), w);
						leaderAware.setNewLeader(shardId);
						final long e = DateTime.now().getMillis() - config.loadTime.getMillis();
						logger.info("{}: {} msec since load till leader election", getName(), e);
						// start analyzing the shards and distribute duties
						clusterProctor.start();
						distributor.start();
						// start listening events from followers and clients alike
						followerEventsHandler.start();
						clientEventsHandler.start();
					} else {
						logger.warn("{}: Skipping start of an already stopped leader: {}", getName(), shardId);
					}
					
					this.start = start;
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
		if (start!=null) {
			this.stop = new Date();
			this.start = null;
			logger.info("{}: Stopping ({})", getName(), !served ? "never served" : "paid my duty");
			clusterProctor.stop();
			distributor.stop();
			followerEventsHandler.stop();
			clientEventsHandler.stop();
		}
	}

	public FollowerEventsHandler getFollowerEventsHandler() {
		return followerEventsHandler;
	}
}
