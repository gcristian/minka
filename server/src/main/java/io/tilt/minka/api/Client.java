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
import java.util.Map;

import org.apache.commons.lang.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.tilt.minka.broker.EventBroker;
import io.tilt.minka.broker.EventBroker.Channel;
import io.tilt.minka.core.leader.ClientEventsHandler;
import io.tilt.minka.core.leader.Leader;
import io.tilt.minka.core.leader.PartitionScheme;
import io.tilt.minka.core.leader.SchemeViews;
import io.tilt.minka.core.leader.distributor.ChangePlan;
import io.tilt.minka.core.task.LeaderShardContainer;
import io.tilt.minka.core.task.impl.ZookeeperLeaderShardContainer;
import io.tilt.minka.domain.EntityEvent;
import io.tilt.minka.domain.ShardEntity;
import io.tilt.minka.domain.ShardIdentifier;
import io.tilt.minka.domain.EntityState;
import io.tilt.minka.domain.PartitionDelegate;
import io.tilt.minka.domain.PartitionMaster;

/**
 * Facility to execute CRUD ops. to {@linkplain Duty} on the cluster.<br> 
 * All ops. are forwarded thru the network broker to the leader, and then routed to its final target shard.<br>  
 * Updates and Transfers are executed without a distributor's balance calculation.<br>
 * In case the leader runs within the same follower's shard, no network communication is needed.<br>
 *<br><br>
 * Remember no CRUD survives leader-reelection (by now), and all ops must be ACID with the<br> 
 * client's supplier callback of duties. (see  {@linkplain Server.onLoad(..))}  <br>
 * As long as Minka lacks of a CAP storage facility.<br>
 * <br><br>
 * @author Cristian Gonzalez
 * @since Nov 7, 2015
 */
public class Client<D extends Serializable, P extends Serializable> {

	private static final Logger logger = LoggerFactory.getLogger(Client.class);

	private final Leader leader;
	private final EventBroker eventBroker;
	private final ClientEventsHandler clientMediator;
	private final ShardIdentifier shardId;
	private final Config config;
	private final LeaderShardContainer leaderShardContainer;
	private final PartitionScheme table;
	private final SchemeViews views;

	protected Client(
			final Config config, 
			final Leader leader, 
			final EventBroker eventBroker,
			final ClientEventsHandler mediator, 
			final ShardIdentifier shardId, 
			final ZookeeperLeaderShardContainer leaderShardContainer, 
			final PartitionScheme table,
			final SchemeViews views) {
		this.config = config;
		this.leader = leader;
		this.eventBroker = eventBroker;
		this.clientMediator = mediator;
		this.shardId = shardId;
		this.leaderShardContainer = leaderShardContainer;
		this.table = table;
		this.views = views;
	}

	/**
	 * A representation Status of Minka's domain objects
	 * @return a nonempty Status only when the curent shard is the Leader 
	 */
	public Map<String, Object> getStatus() {
		return views.buildDistribution(table);
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
		return push(duty, EntityEvent.REMOVE, null);
	}
	public boolean remove(final Pallet<P> pallet) {
		return push(pallet, EntityEvent.REMOVE, null);
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
		return push(duty, EntityEvent.CREATE, null);
	}

	public boolean add(final Pallet<P> pallet) {
		return push(pallet, EntityEvent.CREATE, null);
	}

	/**
	* Enter a notification event for a {@linkplain Duty}
	* @param duty the duty to update
	* @return whether or not the operation succeed
	*/
	public boolean update(final Duty<D> duty) {
		return push(duty, EntityEvent.UPDATE, null);
	}
	public boolean update(final Pallet<P> pallet) {
		return push(pallet, EntityEvent.UPDATE, null);
	}
	public boolean transfer(final Duty<D> duty, final EntityPayload userPayload) {
		return push(duty, EntityEvent.TRANSFER, userPayload);
	}
	public boolean transfer(final Pallet<P> pallet, final EntityPayload userPayload) {
		return push(pallet, EntityEvent.TRANSFER, userPayload);
	}
	
	private boolean push(final Entity<?> raw, final EntityEvent event, final EntityPayload userPayload) {
		Validate.notNull(raw, "an entity is required");
		boolean sent = false;
		final ShardEntity.Builder builder = ShardEntity.Builder.builder(raw);
		if (userPayload != null) {
			builder.withPayload(userPayload);
		}
		final ShardEntity entity = builder.build();
		entity.getJournal().addEvent(event, EntityState.PREPARED, 
				this.shardId,
				ChangePlan.PLAN_WITHOUT);
		if (leader.inService()) {
			if (logger.isInfoEnabled()) {
				logger.info("{}: Recurring to local leader !", getClass().getSimpleName());
			}
			clientMediator.mediateOnEntity(entity);
		} else {
			if (logger.isInfoEnabled()) {
				logger.info("{}: Sending {}: {} with Event: {} to leader in service", 
						getClass().getSimpleName(), entity.getType(), raw, event);
			}
			sent = eventBroker.send(
					eventBroker.buildToTarget(
							config, 
							Channel.FROM_CLIENT,
							leaderShardContainer.getLeaderShardId()), 
					entity);
		}
		return sent;
	}

	public String getShardIdentity() {
		return this.shardId.getId();
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