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
package io.tilt.minka.business;

import org.apache.commons.lang.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.tilt.minka.api.Config;
import io.tilt.minka.api.ConfigValidator;
import io.tilt.minka.api.PartitionDelegate;
import io.tilt.minka.api.PartitionService;
import io.tilt.minka.broker.EventBroker;
import io.tilt.minka.business.Coordinator.Frequency;
import io.tilt.minka.business.Coordinator.PriorityLock;
import io.tilt.minka.business.Coordinator.SynchronizedAgent;
import io.tilt.minka.business.Coordinator.SynchronizedAgentFactory;
import io.tilt.minka.business.Semaphore.Action;
import io.tilt.minka.business.follower.Follower;
import io.tilt.minka.business.impl.ServiceImpl;
import io.tilt.minka.business.impl.SpectatorSupplier;
import io.tilt.minka.business.leader.Leader;
import io.tilt.minka.domain.ShardID;
import io.tilt.minka.spectator.Locks;
import io.tilt.minka.spectator.ServerCandidate;
import io.tilt.minka.utils.LogUtils;

/**
 * Bootup class  for the master-slave node
 * 
 * @author Cristian Gonzalez
 * @since Nov 5, 2015
 */
public class Bootstrap extends ServiceImpl {
	
    private static final long REPUBLISH_LEADER_CANDIDATE_AFTER_LOST_MS = 1000l;

    private static final Logger logger = LoggerFactory.getLogger(Bootstrap.class);
	
    private final Config config;
    private final ConfigValidator validator;
	private final Leader leader;
	private final Follower follower;
	@SuppressWarnings("rawtypes")
    private final PartitionDelegate partitionDelegate;
	private final Coordinator coordinator;
	private final LeaderShardContainer leaderShardContainer;
	private final ShardID shardId;
	private final EventBroker eventBroker;
	private final PartitionService partitionService;
	
	private final SpectatorSupplier spectatorSupplier;
	private Locks locks;
	private int repostulationCounter;
	
	private final SynchronizedAgent bootLeadershipCandidate;
	private final SynchronizedAgent readyAwareBooting; 
	
	/**
	 * Creates a new shard
	 * @param config 				a specific configuration  
	 * @param partitionDelegate		your delegate as point of integration
	 */
	@SuppressWarnings("rawtypes")
	public Bootstrap(
	        final Config config, 
	        final ConfigValidator validator,
	        final SpectatorSupplier spectatorSupplier,
	        final boolean autoStart,
	        final Leader leader,
	        final Follower follower,
	        final PartitionDelegate partitionDelegate, 
	        final Coordinator coordinator,
	        final LeaderShardContainer leaderShardContainer, 
	        final ShardID shardId, 
	        final EventBroker eventBroker,
	        final PartitionService partitionService) {
	    
		Validate.notNull(config, "a unique service name is required (within the ZK ensemble)");
		this.config = config;
		this.validator = validator;
		this.leader=leader;
		this.follower=follower;
		this.partitionDelegate=partitionDelegate;
		this.coordinator = coordinator;		
		this.spectatorSupplier = spectatorSupplier;
        this.repostulationCounter = 0;
        this.leaderShardContainer = leaderShardContainer;
        this.shardId = shardId;
        this.eventBroker = eventBroker;
        this.partitionService = partitionService;
        
        this.bootLeadershipCandidate = SynchronizedAgentFactory.build(
                Action.BOOTSTRAP_LEADERSHIP_CANDIDATURE, PriorityLock.HIGH_ISOLATED, Frequency.ONCE_DELAYED, 
                ()->bootLeadershipCandidate()).delayed(REPUBLISH_LEADER_CANDIDATE_AFTER_LOST_MS);
        
        this.readyAwareBooting = SynchronizedAgentFactory.build(Action.BOOTSTRAP_BULLETPROOF_START, 
                PriorityLock.HIGH_ISOLATED, Frequency.ONCE_DELAYED,
                ()->readyAwareBooting()).delayed(config.getBootstrapReadynessRetryDelayMs());
        
		if (autoStart) {
		    start();
	    }
	}
	
	@Override
	public void start() {
	    logger.info(LogUtils.getGreetings(leader.getShardId(), config.getServiceName()));
        // check configuration is valid and not unstable-prone
        validator.validate(config, partitionDelegate);
        coordinator.start();
	    // all latter services will use it
	    leaderShardContainer.start();
	    // enable the principal service
	    eventBroker.start();
        // let the client call us
        partitionDelegate.setPartitionService(partitionService);
        locks = new Locks(spectatorSupplier.get());
	    // start the real thing
	    readyAwareBooting();
	    // after booting to avoid booting's failure race condition with restart()
        locks.setConnectionLostCallback(()->restart());
	}
	
	@Override
	/* this's the system shutdown */
	public void stop() {
        if (inService()) {
            logger.info("{}: ({}) Destroying context..", getClass().getSimpleName(), shardId);
            if (config.bootstrapLeaderShardAlsoFollows()) {
                follower.destroy();
            }
            if (config.bootstrapPublishLeaderCandidature() && leader.inService()) {
                leader.destroy();
            }
            leaderShardContainer.destroy();
            eventBroker.destroy();
            
            // close the task controller at last
            coordinator.destroy();
            locks.close();
        } else {
            logger.warn("{}: ({}) Not in service", getClass().getSimpleName(), shardId);
        }
    }
    
    /**
     * when ZK connection's lost:
     * kill the leader and reboot
     * let the follower keep working as it's not compromised 
     */
    private void restart() {
        logger.warn("{}: ({}) ZK Connection's lost fallback: restarting leader and leader's shard-container", 
                getClass().getSimpleName(), shardId);
        
        /* skip this while not using global locks because 
         * will bring a loooong tail but it must be done
        coordinator.destroy();
        coordinator.init();
        */
        
        // stop current leader if on service and start the highlander booting all over again
        if (config.bootstrapPublishLeaderCandidature() &&leader.inService()) {
            leader.destroy();
        }
        // ignore container's current held refs. and start it over
        leaderShardContainer.destroy();
        // TODO me voy a esforzar no deberia necesitar reiniciar el follower ! 
        spectatorSupplier.renew();
        leaderShardContainer.init();
        bootLeadershipCandidate();
    }
    
	// check if PartitionDelegate is ready and if it must: start follower and candidate leader
    private void readyAwareBooting() {
        if (partitionDelegate.isReady()) {
    		logger.info("{}: ({}) Starting...", getClass().getSimpleName(), shardId);
    		if (config.bootstrapLeaderShardAlsoFollows()) {
    		    follower.init();
    		}
    		bootLeadershipCandidate();
	    } else {
	        logger.warn("{}: ({}) PartitionDelegate returned not ready!. Bootstrap will retry endlessly. Check: {}", 
	                getClass().getSimpleName(), shardId, partitionDelegate.getClass().getName());
	        coordinator.schedule(readyAwareBooting);
	    }
    }

    /**
     * if config allows leadership candidate: 
     * postulate and keep retrying until postulation is accepted 
     * even in case of losing leadership   
     */
    private void bootLeadershipCandidate() {
        if (config.bootstrapPublishLeaderCandidature()) {
        	logger.info("{}: ({}) Candidating Leader (lap: {})", getClass().getSimpleName(), shardId, 
        	        repostulationCounter);
        	if (!locks.runWhenLeader(getElectionName(), new ServerCandidate() {
        		@Override
        		public void start() {
        			Bootstrap.logger.info("Bootstrap: {} Elected as Leader", leader.getShardId());
        			leader.init();
        			// let's getting hoppin
        			/*coordinator.run(SynchronizedAgentFactory.build(Action.DISTRIBUTE_ENTITY, 
        			     PriorityLock.HIGH_ISOLATED, Frequency.ONCE_DELAYED, ()-> {
        			         logger.info("---->TESTING THE HOP HOP !");
        			         locks.stopCandidateOrLeader(getElectionName(), true);        			         
    			         }).delayed(30000));*/
        		}
        		@Override
        		public void stop() {
        		    leader.destroy();
        		    if (inService()) {
        		        Bootstrap.logger.info("Bootstrap: {} Stopping Leader: now Candidate", leader.getShardId());        		    
        		        coordinator.schedule(bootLeadershipCandidate);
        		    } else {
        		        Bootstrap.logger.info("Bootstrap: {} Stopping Leader at shutdown", leader.getShardId());
        		    }
        		}
        	})) {
        	    // may be ZK's down: so retry
        	    logger.error("{}: ({}) Leader membership rejected, retrying", getClass().getSimpleName(), shardId);
        	    coordinator.schedule(bootLeadershipCandidate); 
        	} else {
        	    repostulationCounter++;
        	}
        } else {
            logger.info("{}: ({}) Avoiding leader candidate according Config", getClass().getSimpleName(), shardId);
        }
    }

    private String getElectionName() {
        final String electionName = "minka/" + config.getServiceName() + "/leader-latch";
        return electionName;
    }

}