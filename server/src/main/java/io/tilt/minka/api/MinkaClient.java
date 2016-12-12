/*
<12 * Licensed to the Apache Software Foundation (ASF) under one or more
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
package io.tilt.minka.api;

import java.io.Serializable;

import org.apache.commons.lang.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.tilt.minka.broker.EventBroker;
import io.tilt.minka.broker.EventBroker.Channel;
import io.tilt.minka.core.follower.Follower;
import io.tilt.minka.core.leader.ClientEventsHandler;
import io.tilt.minka.core.leader.Leader;
import io.tilt.minka.core.leader.PartitionTable;
import io.tilt.minka.core.leader.Status;
import io.tilt.minka.core.task.LeaderShardContainer;
import io.tilt.minka.core.task.impl.ZookeeperLeaderShardContainer;
import io.tilt.minka.domain.EntityEvent;
import io.tilt.minka.domain.Shard;
import io.tilt.minka.domain.ShardEntity;
import io.tilt.minka.domain.ShardEntity.State;
import io.tilt.minka.domain.ShardID;
import io.tilt.minka.domain.ShardState;

/**
 * Facility to CRUD {@linkplain ShardEntity}
 * 
 * So a {@link Leader} service whoever it is, will receive the event and distribute it
 * to a selected {@link Follower} according their {@link ShardState}
 * 
 * From any {@linkplain Shard} (leader or follower) the client can use this class.
 * Coordination operations are driven thru {@linkplain EventBroker} to reach the leader shard
 * or locally executed in case the current shard is the leader
 * 
 * @author Cristian Gonzalez
 * @since Nov 7, 2015
 */
public class MinkaClient<D extends Serializable, P extends Serializable> {

	private static final Logger logger = LoggerFactory.getLogger(MinkaClient.class);

	private final Leader leader;
	private final EventBroker eventBroker;
	private final ClientEventsHandler clientMediator;
	private final ShardID shardId;
	private final Config config;
	private final LeaderShardContainer leaderShardContainer;
	private final PartitionTable table;

	protected MinkaClient(final Config config, final Leader leader, final EventBroker eventBroker,
			final ClientEventsHandler mediator, final ShardID shardId, 
			final ZookeeperLeaderShardContainer leaderShardContainer, final PartitionTable table) {
		this.config = config;
		this.leader = leader;
		this.eventBroker = eventBroker;
		this.clientMediator = mediator;
		this.shardId = shardId;
		this.leaderShardContainer = leaderShardContainer;
		this.table = table;
	}

	/**
	 * A representation Status of Minka's domain objects
	 * @return a nonempty Status only when the curent shard is the Leader 
	 */
	public Status getStatus() {
		return Status.build(table);
	}
	/**
	* Remove duties already running/distributed by Minka
	* This causes the duty to be stopped at Minkas's Follower context.
	* So expect a call at PartitionDelegate.release
	* 
	* Pre-conditions:
	*     1) use after {@linkplain PartitionMaster}'s source has been updated, or within same TX. 
	* Post-Conditions:
	*     1) {@linkplain PartitionDelegate} must not report it still taken: or it will be flagged "dangling"
	*     2) {@linkplain PartitionMaster} must not report it: or it will be re-distributed for execution  
	* 
	* @param duty	    a duty sharded or to be sharded in the cluster
	* @return whether or not the operation succeed
	*/
	public boolean remove(final Duty<D> duty) {
		return send(duty, EntityEvent.REMOVE, null);
	}
	public boolean remove(final Pallet<P> pallet) {
		return send(pallet, EntityEvent.REMOVE, null);
	}

	/**
	* Used to notify Minka about the self-finalization of a Duty by natural cause.
	* The {@linkplain PartitionDelegate} wont be instructed to release the duty.
	*  
	* Post-conditions
	*     1) {@linkplain PartitionDelegate} must not report it taken 
	*     2) {@linkplain PartitionMaster} must not report it present
	*      
	* @param duty	the duty to finalize
	* @return whether or not the operation succeed
	*/
	public boolean finalized(final Duty<D> duty) {
		return send(duty, EntityEvent.FINALIZED, null);
	}

	/**
	* Enter a new duty to Minka so it can distribute it to proper shards. 
	* If Minka persists duties @see {@linkplain Config} this's the only way to enter duties.
	* Otherwise client uses a {@linkplain PartitionMaster} and this's intended for events after bootstraping.
	* Post-conditions:
	*     1) use after {@linkplain PartitionMaster}'s source has been updated, or within same TX.
	*     2) These duties must be also present when Minka uses {@linkplain PartitionMaster} at leader's promotion 
	* 
	* @param duty      a duty sharded or to be sharded in the cluster
	* @return whether or not the operation succeed
	*/
	public boolean add(final Duty<D> duty) {
		return send(duty, EntityEvent.CREATE, null);
	}

	public boolean add(final Pallet<P> pallet) {
		return send(pallet, EntityEvent.CREATE, null);
	}

	/**
	* Enter a notification event for a {@linkplain Duty}
	* @param duty the duty to update
	* @return whether or not the operation succeed
	*/
	public boolean update(final Duty<D> duty) {
		return send(duty, EntityEvent.UPDATE, null);
	}
	public boolean update(final Pallet<P> pallet) {
		return send(pallet, EntityEvent.UPDATE, null);
	}
	public boolean transfer(final Duty<D> duty, final EntityPayload userPayload) {
		return send(duty, EntityEvent.TRANSFER, userPayload);
	}
	public boolean transfer(final Pallet<P> pallet, final EntityPayload userPayload) {
		return send(pallet, EntityEvent.TRANSFER, userPayload);
	}
	
	@SuppressWarnings("unchecked")
	private boolean send(final Entity<?> raw, final EntityEvent event, final EntityPayload userPayload) {
		Validate.notNull(raw, "an entity is required");
		boolean sent = true;
		final ShardEntity entity;
		if (raw instanceof Duty) {
			entity = ShardEntity.create((Duty<D>)raw);
		} else {
			entity = ShardEntity.create((Pallet<P>)raw);
		}
		entity.registerEvent(event, State.PREPARED);
		if (userPayload != null) {
			entity.setUserPayload(userPayload);
		}
		if (leader.inService()) {
			logger.info("{}: Recurring to local leader !", getClass().getSimpleName());
			clientMediator.mediateOnEntity(entity);
		} else {
			logger.info("{}: Sending {}: {} with Event: {} to leader in service", getClass().getSimpleName(), 
					entity.getType(), raw, event);
			sent = eventBroker.postEvent(eventBroker.buildToTarget(config.getBootstrap().getServiceName(), Channel.FROM_CLIENT,
					leaderShardContainer.getLeaderShardId()), entity);
		}
		return sent;
	}

	public String getShardIdentity() {
		return this.shardId.getStringIdentity();
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

}
