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
package io.tilt.minka.business.follower;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.tilt.minka.api.Config;
import io.tilt.minka.broker.EventBroker;
import io.tilt.minka.business.Coordinator;
import io.tilt.minka.business.Coordinator.Frequency;
import io.tilt.minka.business.Coordinator.PriorityLock;
import io.tilt.minka.business.Coordinator.SynchronizedAgent;
import io.tilt.minka.business.Coordinator.SynchronizedAgentFactory;
import io.tilt.minka.business.Semaphore.Action;
import io.tilt.minka.business.impl.ServiceImpl;
import io.tilt.minka.domain.Clearance;
import io.tilt.minka.domain.Heartbeat;
import io.tilt.minka.domain.ShardState;

/**
 * An agnostic slave service with the following responsibilities:
 * 
 * 1) send heartbeats to the master
 * 2) translate to the partition commander the received sharded-entity dis/assignments from the master
 *   
 * @author Cristian Gonzalez
 * @param <T>
 * @since Nov 7, 2015
 *
 */
public class Follower extends ServiceImpl {

    private final Logger logger = LoggerFactory.getLogger(getClass());
    
    private boolean alive;
	
    private final DateTime creation;
    
	private final LeaderConsumer leaderConsumer;
	private final Config config;
	private final EventBroker eventBroker;
	
	private final Coordinator coordinator;
	private final Heartpump heartpump;
	private final HeartbeatBuilder builder;
	
	private final SynchronizedAgent clearancer;
	private final SynchronizedAgent cardiologyst;
	private final SynchronizedAgent pump;
	
	
	private boolean firstClearanceGot;
	
	public Follower(
	        final Config config,
			final Heartpump heartpump,
			final LeaderConsumer leaderConsumer,
			final EventBroker eventBroker, 
			final Coordinator coordinator,
			final HeartbeatBuilder builder) {
		super();
		
		this.alive = true;	
		this.heartpump = heartpump;
		this.builder = builder;
		this.leaderConsumer = leaderConsumer;
		this.config = config;
		this.eventBroker = eventBroker;
		this.creation = new DateTime(DateTimeZone.UTC);
		this.coordinator = coordinator;
		
		this.clearancer = SynchronizedAgentFactory.build(Action.FOLLOWER_POLICIES_CLEARANCE, 
		        PriorityLock.HIGH_ISOLATED, Frequency.PERIODIC, ()->certifyClearance())
                .delayed(config.getFollowerClearanceCheckStartDelayMs())
                .every(config.getFollowerClearanceCheckDelayMs());
		
		this.cardiologyst = SynchronizedAgentFactory.build(Action.STUCK_POLICY, 
		        PriorityLock.MEDIUM_BLOCKING, Frequency.PERIODIC, ()->checkHeartattackPolicy())
                .delayed(config.getFollowerHeartattackCheckStartDelayMs())
                .every(config.getFollowerHeartattackCheckDelayMs());
		
        this.pump = SynchronizedAgentFactory.build(Action.HEARTBEAT_REPORT, PriorityLock.MEDIUM_BLOCKING, 
                Frequency.PERIODIC, ()->heartpump.emit(builder.build()))
                .delayed(config.getFollowerHeartbeatStartDelayMs())
                .every(config.getFollowerHeartbeatDelayMs());
	}

	@Override
	public void start() {
		logger.info("{}: ({}) Starting services", getClass().getSimpleName(), config.getResolvedShardId());
		this.heartpump.init();
		this.leaderConsumer.init();
		alive = true;
		turnOnPolicies();
		// after partition manager initialized: set emergency shutdown
		eventBroker.setBrokerShutdownCallback(()->stop());
		coordinator.schedule(pump);
	}

	@Override
	public void stop() {
		logger.info("{}: ({}) Stopping services, on duty since: {}", getClass().getSimpleName(), 
		        config.getResolvedShardId(), creation);
		if (inService()) {
		    final Heartbeat bye = builder.build();
            bye.setStateChange(ShardState.QUITTED);
            heartpump.emit(bye);
            logger.info("{}: ({}) Stopping timer", getClass().getSimpleName(), config.getResolvedShardId());
            coordinator.stop(pump);
		    alive = false;
			turnOffPolicies();
			this.heartpump.stop();
			this.leaderConsumer.stop();
		} else {
			logger.info("{}: ({}) Follower was not longer in service", getClass().getSimpleName(), config.getResolvedShardId());
		}
	}

	private void certifyClearance() {
	    boolean lost = false;
	    final Clearance clear = leaderConsumer.getLastClearance();
	    long delta = 0;
	    int maxAbsence = config.getFollowerClearanceMaxAbsenceMs();
        if (clear != null) {
            firstClearanceGot = true;
            final DateTime now = new DateTime(DateTimeZone.UTC);
            final DateTime lastClearanceDate = clear.getCreation();
            lost = lastClearanceDate.plusMillis(maxAbsence).isBefore(now);
            delta = now.getMillis()-lastClearanceDate.getMillis();
	    }
        if (firstClearanceGot) {
    	    if (lost) {
    	        logger.error("{}: ({}) Executing Clearance policy, last: {} too old (Max: {}, Past: {} msecs)", 
                    getClass().getSimpleName(), config.getResolvedShardId(), clear!=null ? clear.getCreation() : "null", 
                            maxAbsence, delta);
                leaderConsumer.getPartitionManager().releaseAllCausePolicies();
            } else {
                logger.debug("{}: ({}) Clearence certified #{} from Leader: {}", getClass().getSimpleName(), 
                        config.getResolvedShardId(), clear.getSequenceId(), clear.getLeaderShardId());
            }
        }
    }
	
	public void turnOnPolicies() {
	    logger.info("{}: ({}) Scheduling constant policies", getClass().getSimpleName(), config.getResolvedShardId());	    
	    coordinator.schedule(clearancer);
        coordinator.schedule(cardiologyst);
	}
    
	public void turnOffPolicies() {
	    coordinator.stop(cardiologyst);
	    coordinator.stop(clearancer);
	}
	
	private void checkHeartattackPolicy() {
        if (heartpump.getLastBeat()== null) {
            return;
        } else {
            final DateTime expiracy = heartpump.getLastBeat()
                    .plus(config.getFollowerMaxHeartbeatAbsenceForReleaseMs());
            if (expiracy.isBefore(new DateTime(DateTimeZone.UTC)) && true) {
                logger.warn("{}: ({}) Executing Heartattack policy (last HB: {}): releasing delegate's held duties", 
                        getClass().getSimpleName(), config.getResolvedShardId(), expiracy);
                leaderConsumer.getPartitionManager().releaseAllCausePolicies();
            }
        }
	}
	
	@Override
	public boolean inService() {
		return alive;
	}
}
