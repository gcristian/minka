/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.tilt.minka.business.leader;

import java.util.List;

import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.tilt.minka.api.Config;
import io.tilt.minka.broker.EventBroker;
import io.tilt.minka.business.Coordinator;
import io.tilt.minka.business.LeaderShardContainer;
import io.tilt.minka.business.Service;
import io.tilt.minka.business.Coordinator.PriorityLock;
import io.tilt.minka.business.Coordinator.SynchronizedFactory;
import io.tilt.minka.business.Semaphore.Action;
import io.tilt.minka.business.impl.ServiceImpl;
import io.tilt.minka.domain.NetworkShardID;

/**
 * An agnostic master service with the following responsibilities:
 *  
 * 1) receive sharded-entity events from User's using {@link ClusterEntity}
 * 2) listen to heartbeats from slaves and ensure there're no unassigned entities
 * 3) distributing and balancing sharded-entities to slaves
 * 
 * @author Cristian Gonzalez
 * @since Nov 7, 2015
 */
public class Leader extends ServiceImpl {

    private final Logger logger = LoggerFactory.getLogger(getClass());
    
    /* maintenance */
    private final Coordinator coordinator;
    private final NetworkShardID shardId;
    private final List<Service> services;
    private final Config config;
    private final LeaderShardContainer leaderShardContainer;
    private boolean served;
    
	public Leader(
	        final Config config, 
	        final List<Service> services,
	        final Coordinator coordinator,
	        final NetworkShardID shardId,
	        final LeaderShardContainer leaderShardContainer,
	        final EventBroker eventBroker) {

		super();
		this.config =config;
		this.services=services;
		this.coordinator=coordinator;
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
    		coordinator.run(SynchronizedFactory.build(Action.LEADERSHIP, PriorityLock.LOW_ON_PERMISSION, ()-> {
				try {
				    served = true;
				    logger.info("{}: Registering as Leader at well after waiting {} msecs", 
				            getClass().getSimpleName(), super.getMillisSinceCreation());
			        leaderShardContainer.setNewLeader(shardId);
				    logger.info("{}: {} msec since load till leader election", getClass().getSimpleName(), 
				            (DateTime.now().getMillis()-config.loadTime.getMillis()));
					services.forEach(s->s.init());
				} catch (Exception e) {
					logger.error("Unexpected error when starting service. Cannot procede", e);
				}
    		}));/* {
    			logger.error("A blocking cluster operation is running to avoid Master start");
    			//Distributed.stopCandidate(Names.getLeaderName(config.getServiceName()), false);
    			// subscriptions never made
    		}*/
        } catch (Exception e) {
            logger.error("Unexpected error when starting service. Cannot procede", e);
        } finally {
            
        }
	}
    

	@Override
	public void stop() {
		logger.info("{}: Stopping ({})", getClass().getSimpleName(), !served ? "never served":"paid my duty");
        services.forEach(s->s.destroy());
	}

	
}