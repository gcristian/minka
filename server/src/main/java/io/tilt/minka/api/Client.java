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

import static java.util.Collections.singletonList;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import org.apache.commons.lang.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.tilt.minka.api.inspect.SystemState;
import io.tilt.minka.broker.EventBroker;
import io.tilt.minka.broker.EventBroker.BrokerChannel;
import io.tilt.minka.broker.EventBroker.Channel;
import io.tilt.minka.core.leader.ClientEventsHandler;
import io.tilt.minka.core.leader.Leader;
import io.tilt.minka.core.leader.distributor.ChangePlan;
import io.tilt.minka.core.task.LeaderShardContainer;
import io.tilt.minka.core.task.impl.ZookeeperLeaderShardContainer;
import io.tilt.minka.domain.EntityEvent;
import io.tilt.minka.domain.EntityState;
import io.tilt.minka.domain.ShardEntity;
import io.tilt.minka.domain.ShardIdentifier;
import io.tilt.minka.domain.ShardedPartition;

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
	private final SystemState state;
	private final ShardedPartition partition;

	protected Client(
			final Config config, 
			final Leader leader, 
			final EventBroker eventBroker,
			final ClientEventsHandler mediator, 
			final ShardIdentifier shardId, 
			final ZookeeperLeaderShardContainer leaderShardContainer, 
			final SystemState state,
			final ShardedPartition partition) {
		this.config = config;
		this.leader = leader;
		this.eventBroker = eventBroker;
		this.clientMediator = mediator;
		this.shardId = shardId;
		this.leaderShardContainer = leaderShardContainer;
		this.state = state;
		this.partition = partition;
	}

	/**
	 * A representation Status of Minka's domain objects
	 * @return a nonempty Status only when the curent shard is the Leader 
	 */
	public Map<String, Object> getStatus() {
		return state.buildDistribution();
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
	public Reply remove(final Duty<D> duty) {
		return push(singletonList(duty), EntityEvent.REMOVE, null, null);
	}
	public void removeAll(final Collection<Entity<?>> coll, final Consumer<Reply> callback) {
		push(coll, EntityEvent.REMOVE, null, callback);
	}

	public Reply remove(final Pallet<P> pallet) {
		return push(singletonList(pallet), EntityEvent.REMOVE, null, null);
	}

	/**
	 * <p>
	 * A list of duties currently captured by the server shard, 
	 * Contents will differ since the call of this method, if new distributions occurr,
	 * i.e. calling this method twice may not return the same contents.
	 * 
	 * @return			a list of captured duties
	 */
	public List<Duty<D>> captured() {
		return this.partition.getDuties().stream()
				.map(d->(Duty<D>)d.getDuty())
				.collect(Collectors.toList());
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
	public Reply add(final Duty<D> duty) {
		return push(singletonList(duty), EntityEvent.CREATE, null, null);
	}
	public void addAll(final Collection<Entity<?>> duty, final Consumer<Reply> callback) {
		push(duty, EntityEvent.CREATE, null, callback); 
	}

	public Reply add(final Pallet<P> pallet) {
		return push(singletonList(pallet), EntityEvent.CREATE, null, null);
	}

	/**
	* Enter a notification event for a {@linkplain Duty}
	* @param duty the duty to update
	* @return whether or not the operation succeed
	*/
	public Reply update(final Duty<D> duty) {
		return push(singletonList(duty), EntityEvent.UPDATE, null, null);
	}
	public Reply update(final Pallet<P> pallet) {
		return push(singletonList(pallet), EntityEvent.UPDATE, null, null);
	}
	public Reply transfer(final Duty<D> duty, final EntityPayload userPayload) {
		return push(singletonList(duty), EntityEvent.TRANSFER, userPayload, null);
	}
	public Reply transfer(final Pallet<P> pallet, final EntityPayload userPayload) {
		return push(singletonList(pallet), EntityEvent.TRANSFER, userPayload, null);
	}
	
	private Reply push(final Collection<Entity<?>> raws, 
			final EntityEvent event, 
			final EntityPayload userPayload, 
			final Consumer<Reply> callback) {
		
		Validate.notNull(raws, "an entity is required");
		// only not null when raws.size > 1 
		final Reply[] r = {null};
		
		final List<ShardEntity> entities = toEntities(raws, event, userPayload);
		
		if (leader.inService()) {
			if (logger.isDebugEnabled()) {
				logger.debug("{}: Recurring to local leader !", getClass().getSimpleName());
			}
			if (callback==null) {
				clientMediator.mediateOnEntity(entities, reply->r[0]=reply);
			} else {
				clientMediator.mediateOnEntity(entities, callback);
			}
		} else {
			if (logger.isDebugEnabled()) {
				logger.debug("{}: Sending event: {} to leader in service", 
					getClass().getSimpleName(), event, raws, event);
			}
			r[0] = sendAndReply(event, callback, r, entities);
		}
		return r[0];
	}

	private Reply sendAndReply(
			final EntityEvent event, 
			final Consumer<Reply> callback, 
			final Reply[] r,
			final List<ShardEntity> tmp) {
		int tries = 10;
		boolean[] sent = {false};
		while (!sent[0] && tries-->0) {
			if (leaderShardContainer.getLeaderShardId()!=null) {
				final BrokerChannel channel = eventBroker.buildToTarget(config, 
						Channel.CLITOLEAD,
						leaderShardContainer.getLeaderShardId());
				sent[0] = eventBroker.send(channel, (List)tmp);
			} else {
				try {
					Thread.sleep(config.beatToMs(10));
				} catch (InterruptedException ie) {
					break;
				}
			}
		}
		if (callback==null) {
			return new Reply(sent[0] ? ReplyValue.SUCCESS_SENT : ReplyValue.FAILURE_NOT_SENT, 
					tmp.get(0).getEntity(), null, event, null);
		} else {
			for (ShardEntity e: tmp) {
				try {
					callback.accept(new Reply(
							sent[0] ? ReplyValue.SUCCESS_SENT : ReplyValue.FAILURE_NOT_SENT, 
							e.getEntity(), 
							null, 
							event, 
							null));	
				} catch (Exception e2) {
					logger.warn("{}: reply callback throwed exception: {}", getClass().getSimpleName(), e2.getMessage());
				}
			}
		}
		return null;
	}

	private List<ShardEntity> toEntities(
			final Collection<Entity<?>> raws, 
			final EntityEvent event,
			final EntityPayload userPayload) {
		
		final List<ShardEntity> tmp = new ArrayList<>();
		for (final Entity<?> e: raws) {
			final ShardEntity.Builder builder = ShardEntity.Builder.builder(e);
			if (userPayload != null) {
				builder.withPayload(userPayload);
			}
			final ShardEntity tmpp = builder.build();
			tmpp.getJournal().addEvent(
					event, 
					EntityState.PREPARED,  
					this.shardId, 
					ChangePlan.PLAN_WITHOUT);
			tmp.add(tmpp);
		}
		return tmp;
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
