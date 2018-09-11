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
package io.tilt.minka.api;

import static java.util.Collections.singletonList;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

import io.tilt.minka.core.leader.LeaderBootstrap;
import io.tilt.minka.core.monitor.DistroJSONBuilder;
import io.tilt.minka.domain.EntityEvent;
import io.tilt.minka.domain.ShardedPartition;
import io.tilt.minka.shard.ShardIdentifier;

/**
 * Facility to execute CRUD ops. to {@linkplain Duty} on the cluster.<br> 
 * All ops. are forwarded thru the network broker to the leaderBootstrap, and then routed to its final target shard.<br>  
 * Updates and Transfers are executed without a distributor's balance calculation.<br>
 * In case the leaderBootstrap runs within the same follower's shard, no network communication is needed.<br>
 *<br><br>
 * Remember no CRUD survives leaderBootstrap-reelection (by now), and all ops must be ACID with the<br> 
 * client's supplier callback of duties. (see  {@linkplain Server.onLoad(..))}  <br>
 * As long as Minka lacks of a CAP storage facility.<br>
 * <br><br>
 * @author Cristian Gonzalez
 * @since Nov 7, 2015
 */
public class Client {

	private final CrudExecutor crudExec;
	private final LeaderBootstrap leaderBootstrap;
	private final ShardIdentifier shardId;
	private final DistroJSONBuilder distroJSONBuilder;
	private final ShardedPartition partition;
	
	private EventMapper eventMapper;
	private long futureMaxWaitMs = ParkingThreads.NO_EXPIRATION;
	
	protected Client(
			final CrudExecutor crudExec,
			final LeaderBootstrap leaderBootstrap, 
			final ShardIdentifier shardId, 
			final DistroJSONBuilder distroJSONBuilder,
			final ShardedPartition partition) {
		this.crudExec = crudExec;
		this.leaderBootstrap = leaderBootstrap;
		this.shardId = shardId;
		this.distroJSONBuilder = distroJSONBuilder;
		this.partition = partition;
	}

	/**
	 * A representation Status of Minka's domain objects
	 * @return a nonempty Status only when the curent shard is the LeaderBootstrap 
	 */
	public Map<String, Object> getStatus() {
		return distroJSONBuilder.buildDistribution();
	}

	/**
	* Remove duties already running/distributed by Minka
	* This causes the duty to be stopped at Minkas's FollowerBootstrap context.
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
	public Future<Collection<Reply>> remove(final Duty duty) {
		return crudExec.execute(futureMaxWaitMs, singletonList(duty), EntityEvent.REMOVE, null);
	}
	public Future<Collection<Reply>> removeAll(final Collection<Entity> coll) {
		return crudExec.execute(futureMaxWaitMs, coll, EntityEvent.REMOVE, null);
	}

	public Future<Collection<Reply>> remove(final Pallet pallet) {
		return crudExec.execute(futureMaxWaitMs, singletonList(pallet), EntityEvent.REMOVE, null);
	}

	/**
	 * <p>
	 * A list of duties currently captured by the server shard, 
	 * Contents will differ since the call of this method, if new distributions occurr,
	 * i.e. calling this method twice may not return the same contents.
	 * Although difference may not exist if:
	 * 	1) no CRUD operations occurr
	 *  2) no shard falls down
	 * 
	 * @return			a list of captured duties
	 */
	public List<Duty> captured() {
		return this.partition.getDuties().stream()
				.map(d->(Duty)d.getDuty())
				.collect(Collectors.toList());
	}
	
	/**
	* Enter a new duty to Minka so it can distribute it to proper shards. 
	* If Minka persists duties @see {@linkplain Config} this's the only way to enter duties.
	* Otherwise client uses a {@linkplain PartitionMaster} and this's intended for events after bootstraping.
	* Post-conditions:
	*     1) use after {@linkplain PartitionMaster}'s source has been updated, or within same TX.
	*     2) These duties must be also present when Minka uses {@linkplain PartitionMaster} at leaderBootstrap's promotion 
	* 
	* @param duty      a duty sharded or to be sharded in the cluster
	* @return whether or not the operation succeed
	*/
	public Future<Collection<Reply>> add(final Duty duty) {
		return crudExec.execute(futureMaxWaitMs, singletonList(duty), EntityEvent.CREATE, null);
	}
	public Future<Collection<Reply>> addAll(final Collection<? extends Entity> duty) {
		return crudExec.execute(futureMaxWaitMs, duty, EntityEvent.CREATE, null); 
	}

	public Future<Collection<Reply>> add(final Pallet pallet) {
		return crudExec.execute(futureMaxWaitMs, singletonList(pallet), EntityEvent.CREATE, null);
	}

	/**
	* Enter a notification event for a {@linkplain Duty}
	* @param duty the duty to update
	* @return whether or not the operation succeed
	*/
	public Future<Collection<Reply>> update(final Duty duty) {
		return crudExec.execute(futureMaxWaitMs, singletonList(duty), EntityEvent.UPDATE, null);
	}
	public Future<Collection<Reply>> update(final Pallet pallet) {
		return crudExec.execute(futureMaxWaitMs, singletonList(pallet), EntityEvent.UPDATE, null);
	}
	public Future<Collection<Reply>> transfer(final Duty duty, final EntityPayload userPayload) {
		return crudExec.execute(futureMaxWaitMs, singletonList(duty), EntityEvent.TRANSFER, userPayload);
	}
	public Future<Collection<Reply>> transfer(final Pallet pallet, final EntityPayload userPayload) {
		return crudExec.execute(futureMaxWaitMs, singletonList(pallet), EntityEvent.TRANSFER, userPayload);
	}
	
	public void setFutureMaxWaitMs(long futureMaxWaitMs) {
		this.futureMaxWaitMs = futureMaxWaitMs;
	}
	
	public String getShardIdentity() {
		return this.shardId.getId();
	}
	
	public EventMapper getEventMapper() {
		return this.eventMapper;
	}
	final void setEventMapper(EventMapper eventMapper) {
		this.eventMapper = eventMapper;
	}
	

	/**
	* Client should not need to use this method unless is misunderstanding Minka.
	* Any CRUD operation over a service inside Minka, must use the crud methods.
	* But in order to handle "other" dependencies you may need to know where 
	* leaderBootstrap is temporarily, as it will change without you being noticed.
	*   
	* @return    whether or not the current partition has taken leadership
	*/
	public boolean isCurrentLeader() {
		return leaderBootstrap.inService();
	}

}
