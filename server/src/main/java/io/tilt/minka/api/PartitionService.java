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
package io.tilt.minka.api;

import java.io.Serializable;

import org.apache.commons.lang.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.tilt.minka.broker.EventBroker;
import io.tilt.minka.broker.EventBroker.Channel;
import io.tilt.minka.business.LeaderShardContainer;
import io.tilt.minka.business.Semaphore;
import io.tilt.minka.business.Semaphore.Action;
import io.tilt.minka.business.follower.Follower;
import io.tilt.minka.business.impl.ZookeeperLeaderShardContainer;
import io.tilt.minka.business.leader.ClientMediator;
import io.tilt.minka.business.leader.Leader;
import io.tilt.minka.domain.DutyEvent;
import io.tilt.minka.domain.Shard;
import io.tilt.minka.domain.ShardCommand;
import io.tilt.minka.domain.ShardDuty;
import io.tilt.minka.domain.ShardID;
import io.tilt.minka.domain.ShardState;
import io.tilt.minka.domain.Workload;
import io.tilt.minka.domain.ShardDuty.State;

/**
 * Client's point of integration.
 * Facility to CRUD {@linkplain ShardDuty} and SysAdmin a {@linkplain Shard} 
 * 
 * So a {@link Leader} service whoever it is, will receive the event and distribute it
 * to a selected {@link Follower} according their {@link ShardState} and {@link Workload}
 * 
 * From any {@linkplain Shard} (leader or follower) the client can use this class.
 * Coordination operations are driven thru {@linkplain EventBroker} to reach the leader shard
 * or locally executed in case the current shard is the leader
 * 
 * @author Cristian Gonzalez
 * @since Nov 7, 2015
 */
public class PartitionService {

	private static final Logger logger = LoggerFactory.getLogger(PartitionService.class);
	
	public enum Command {
	    /**
	     * Cleanly stop the cluster, avoiding further election of leaders
	     * and stopping all the followers in service.
	     */
	    CLUSTER_CLEAN_SHUTDOWN (Semaphore.Action.CLUSTER_COMPLETE_SHUTDOWN),
	    /**
	     * Performa an entity balance on unbalanced followers
	     */
	    CLUSTER_BALANCE (Semaphore.Action.DISTRIBUTOR_PERIODIC),
	    /**
	     * Perform a reelection of a leader, avoiding the current leader present as candidate
	     */
	    CLUSTER_LEADER_REELECTION (Semaphore.Action.LEADERSHIP),
	    /**
	     * Take a follower out of the cluster, causing its taken entities to be resharded.
	     */
	    FOLLOWER_DECOMISSION (Semaphore.Action.PARTITION_TABLE_UPDATE),
	    /**
	     * Take a follower out of the cluster, holding its entities without rebalance
	     */
	    FOLLOWER_DEACTIVATE (Action.PARTITION_TABLE_UPDATE),
	    /**
	     * Take a follower into the cluster
	     */
	    FOLLOWER_ACTIVATE (Action.PARTITION_TABLE_UPDATE),
	    /**
	     * Take all entities to a certain follower
	     */
	    FOLLOWER_HOARD (Action.PARTITION_TABLE_UPDATE);
	    
	    final Action action;
	    Command(Action action) {
	        this.action = action;
	    }
        public Action getAction() {
            return this.action;
        }
	    
	}
	
	private final Leader leader;
	private final EventBroker eventBroker;
	private final ClientMediator clientMediator;
	private final ShardID shardId; 
	private final LeaderShardContainer leaderShardContainer;
	
	public PartitionService(
	        final Leader leader, 
	        final EventBroker eventBroker, 
	        final ClientMediator mediator, 
	        final ShardID shardId, 
	        final ZookeeperLeaderShardContainer leaderShardContainer) {
	    
	    this.leader = leader;
	    this.eventBroker = eventBroker;
	    this.clientMediator = mediator;
	    this.shardId = shardId;
	    this.leaderShardContainer = leaderShardContainer;
	}
	
	/**
	 * Remove duties already running/distributed by Minka
	 * This causes the duty to be stopped at Minkas's Follower context.
	 * So expect a call at {@linkplain PartitionDelegate.release}.
	 * 
	 * Pre-conditions:
	 *     1) use after {@linkplain PartitionMaster}'s source has been updated, or within same TX. 
	 * Post-Conditions:
	 *     1) {@linkplain PartitionDelegate} must not report it still taken: or it will be flagged "dangling"
	 *     2) {@linkplain PartitionMaster} must not report it: or it will be re-distributed for execution  
	 * 
	 * @param service	a unique name within a ZK ensemble to identify the shards
	 * @param duty	    a duty sharded or to be sharded in the cluster
	 */
	public boolean remove(final String service, final Duty<?> duty) {
        return send(service, duty, DutyEvent.DELETE, null);
    }
	
	/**
	 * Used to notify Minka about the self-finalization of a Duty by natural cause.
	 * The {@linkplain PartitionDelegate} wont be instructed to release the duty.
	 *  
	 * Post-conditions
	 *     1) {@linkplain PartitionDelegate} must not report it taken 
	 *     2) {@linkplain PartitionMaster} must not report it present
	 *      
	 * @param service
	 * @param duty
	 * @return
	 */
	public boolean finalized(final String service, final Duty<?> duty) {
        return send(service, duty, DutyEvent.FINALIZED, null);
    }
	
	/**
	 * Enter a new duty to Minka so it can distribute it to proper shards. 
	 * If Minka persists duties @see {@linkplain Config} this's the only way to enter duties.
	 * Otherwise client uses a {@linkplain PartitionMaster} and this's intended for events after bootstraping.
	 * Post-conditions:
	 *     1) use after {@linkplain PartitionMaster}'s source has been updated, or within same TX.
	 *     2) These duties must be also present when Minka uses {@linkplain PartitionMaster} at leader's promotion 
	 * 
     * @param service   a unique name within a ZK ensemble to identify the shards
     * @param duty      a duty sharded or to be sharded in the cluster
	 */
	public boolean add(final String service, final Duty<?> duty) {
        return send(service, duty, DutyEvent.CREATE, null);
    }
	
	/**
	 * Notify Minka of an updated duty so it can notify {@linkplain PartitionDelegate} about it
     * @param service   a unique name within a ZK ensemble to identify the shards
     * @param duty      a duty sharded or to be sharded in the cluster
	 */
	public boolean update(final String service, final Duty<?> duty) {
        return send(service, duty, DutyEvent.UPDATE, null);
    }
	
	/**
	 * Enter a notification event for a {@linkplain Duty}
	 * @see PartitionDelegate method receive()
     * @see {@linkplain update} but with a payload  
     */
	public boolean notify(
	        final String service, 
	        final Duty<?> duty, 
	        final Serializable userPayload) {
	    
        return send(service, duty, DutyEvent.UPDATE, userPayload);
    }
    
	private boolean send(
	        final String service, 
	        final Duty<?> raw, 
	        final DutyEvent event, 
	        final Serializable userPayload) {
	    
		Validate.notNull(service, "a service name is required to send the entity");
		Validate.notNull(raw, "an entity is required");
		
		boolean sent = true;
		final ShardDuty duty = ShardDuty.create(raw);
		duty.registerEvent(event, State.PREPARED);
		if (userPayload !=null) {
		    duty.setUserPayload(userPayload);
		}
		if (leader.inService()) {
		    logger.info("{}: Recurring to local leader !", getClass().getSimpleName());
		    clientMediator.mediateOnDuty(duty);
		} else {
		    logger.info("{}: Sending Duty: {} with Event: {} to leader in service", 
		            getClass().getSimpleName(), raw, event);
		    sent = eventBroker.postEvent(eventBroker.buildToTarget(service, Channel.CLIENT_TO_LEADER, 
		            leaderShardContainer.getLeaderShardId()), duty);
		}
		return sent;
	}
	
	public String getShardIdentity() {
	    return this.shardId.getStringID();
	}
	
	/**
	 * Client should not need to use this method unless is misunderstanding Minka.
	 * Any CRUD operation over a service inside Minka, must use the crud methods.
	 * But in order to handle "other" dependencies you may need to know where 
	 * leader is temporarily, as it will change without you being noticed.
	 *   
	 * @return    whether or not the current partition has taken leadership
	 */
	public boolean isCurrentLeader() {
	    return leader.inService();
	}
	
	/**
	 * Send an operation to be executed at the leader
	 * 
	 * @param service	a unique name within a ZK ensemble to identify the shards
	 * @param op		an {@link Command} to be sent to the master's duty queue
	 * @return
	 */
	public boolean execute(final String service, final ShardCommand op) {
		boolean done = false;
		if (leader.inService()) {
		    logger.info("{}: Execute: recurring to local leader in service", getClass().getSimpleName());
			done = clientMediator.clusterOperation(op);			
		} else {
		    done = eventBroker.postEvent(eventBroker.buildToTarget(service, Channel.CLIENT_TO_LEADER, 
		            leaderShardContainer.getLeaderShardId()), op);
		}
		return done;
	}

}