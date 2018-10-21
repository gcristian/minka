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
package io.tilt.minka.api.crud;

import static java.util.Collections.singletonList;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

import io.tilt.minka.api.EventMapper;
import io.tilt.minka.core.leader.LeaderBootstrap;
import io.tilt.minka.core.leader.data.CommitState;
import io.tilt.minka.core.monitor.DistroJSONBuilder;
import io.tilt.minka.domain.EntityEvent;
import io.tilt.minka.domain.ShardedPartition;
import io.tilt.minka.model.Duty;
import io.tilt.minka.model.EntityPayload;
import io.tilt.minka.model.Pallet;
import io.tilt.minka.shard.ShardIdentifier;

/**
 * Facility to execute CRUD ops. to {@linkplain Duty} on the cluster.<br> 
 * All ops. are forwarded thru the network broker to the Leader shard, and then routed to its final target shard.<br>  
 * Updates and Transfers are executed without a distributor's balance calculation.<br>
 * In case the Leader runs within Client's shard, no network communication is needed.<br>
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
	private NonBlockingAPI nonblocking;
	private FireAndForgetAPI fireAndForget;
	
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

	public String getShardIdentity() {
		return this.shardId.getId();
	}
	
	public EventMapper getEventMapper() {
		return this.eventMapper;
	}
	public final void setEventMapper(EventMapper eventMapper) {
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

	/**
	 * <p>
	 * A list of duties currently captured by the server shard. 
	 * Calling this method twice may not return the same contents.
	 * Although difference may not exist if:
	 * 	1) no CRUD operations occurr
	 *  2) no shard falls down or changes health state
	 *  3) no balancer's migrations occurr
	 *  
	 * @return			a list of captured duties
	 */
	public List<Duty> captured() {
		return this.partition.getDuties().stream()
				.map(d->(Duty)d.getDuty())
				.collect(Collectors.toList());
	}

	public void setFutureMaxWaitMs(int futureMaxWaitMs) {
		this.crudExec.setFutureMaxWaitMs(futureMaxWaitMs);
	}
	
	/**
	* Enter a new duty to Minka so it can distribute it to proper shards. 
	* This causes the duty to be attached at Minka's Follower context.
	* So expect a call at PartitionDelegate.capture 
	* @param duty      a duty sharded or to be sharded in the cluster
	* @return whether or not the operation succeed
	*/
	public Reply add(final Duty duty) {
		return crudExec.executeBlockingWithResponse(
					singletonList(duty), 
					EntityEvent.CREATE, 
					null)
				.iterator().next();
	}
	
	public Collection<Reply> addAll(final Collection<Duty> duty) {
		return crudExec.executeBlockingWithResponse(duty, EntityEvent.CREATE, null); 
	}

	public Reply add(final Pallet pallet) {
		return crudExec.executeBlockingWithResponse(
					singletonList(pallet), 
					EntityEvent.CREATE, 
					null)
				.iterator().next();
	}

	/**
	* Remove duties already running/distributed by Minka
	* This causes the duty to be stopped at Minkas's FollowerBootstrap context.
	* So expect a call at PartitionDelegate.release
	* @param duty	    a duty sharded or to be sharded in the cluster
	* @return whether or not the operation succeed
	*/
	public Reply remove(final Duty duty) {
		return crudExec.executeBlockingWithResponse(
					singletonList(duty), 
					EntityEvent.REMOVE, null)
				.iterator().next();
	}
	public Collection<Reply> removeAll(final Collection<Duty> coll) {
		return crudExec.executeBlockingWithResponse(coll, EntityEvent.REMOVE, null);
	}

	public Reply remove(final Pallet pallet) {
		return crudExec.executeBlockingWithResponse(
					singletonList(pallet), 
					EntityEvent.REMOVE, 
					null)
				.iterator().next();
	}
	
	/**
	 * Obtain a facility to dispatch operations asynchronously.
	 * It will still add tasks to FJP waiting for leader's response replies 
	 * and then (for successful replies) to receive distribution's status as {@linkplain CommitState}
	 * For that matter all methods return Futures. 
	 */
	public synchronized NonBlockingAPI nonBlocking() {
		if (this.nonblocking == null) {
			this.nonblocking = new NonBlockingAPI(this.crudExec);
		}
		return nonblocking;
	}
	
	public static class NonBlockingAPI {

		private final CrudExecutor crudExec;

		public NonBlockingAPI(CrudExecutor crudExec) {
			this.crudExec = crudExec;
		}

		/**
		* Enter a new duty to Minka so it can distribute it to proper shards.
		* This causes the duty to be attached at Minka's Follower context.
		* So expect a call at PartitionDelegate.capture 
		* @param duty      a duty sharded or to be sharded in the cluster
		* @return whether or not the operation succeed
		*/
		public Future<Collection<Reply>> addAll(final Collection<Duty> duty) {
			return crudExec.executeWithResponse(duty, EntityEvent.CREATE, null); 
		}

		public Future<Reply> add(final Duty duty) {
			return crudExec.executeWithResponseSingle(duty, EntityEvent.CREATE, null);
		}
		
		public Future<Reply> add(final Pallet pallet) {
			return crudExec.executeWithResponseSingle(pallet, EntityEvent.CREATE, null);
		}

		/**
		* Remove duties already running/distributed by Minka
		* This causes the duty to be stopped at Minkas's Follower context.
		* So expect a call at PartitionDelegate.release
		* @param duty	    a duty sharded or to be sharded in the cluster
		* @return whether or not the operation succeed
		*/
		public Future<Collection<Reply>> removeAll(final Collection<Duty> coll) {
			return crudExec.executeWithResponse(coll, EntityEvent.REMOVE, null);
		}
		public Future<Reply> remove(final Duty duty) {
			return crudExec.executeWithResponseSingle(duty, EntityEvent.REMOVE, null);
		}
		
		public Future<Reply> remove(final Pallet pallet) {
			return crudExec.executeWithResponseSingle(pallet, EntityEvent.REMOVE, null);
		}
	}
	
	/**
	 * A way to execute operations without saving resources neither locally waiting for the leader reply,
	 * nor the leader shard responding back the result of the crud operation, and it's later distribution status.
	 * 
	 * In case the local shard hosts the Leader shard, replies will have some detail on {@linkplain ReplyValue}
	 * about the consistency or legality of the operation.
	 * In case the local is a follower shard, reply will have a sent_failed or sent_success flag.
	 * 
	 * Operation still runs in the caller's thread but there's no blocking step internally,
	 * 
	 * @return	an instance with the same capabilities but with much lighter resource usage, better for performance.
	 */
	public synchronized FireAndForgetAPI fireAndForget() {
		if (this.fireAndForget==null) { 
			this.fireAndForget = new FireAndForgetAPI(this.crudExec);
		}
		return fireAndForget;
	}
	
	public static class FireAndForgetAPI {
		
		private final CrudExecutor crudExec;
		
		public FireAndForgetAPI(final CrudExecutor crudExec) {
			this.crudExec = crudExec;
		}

		public Reply add(final Pallet pallet) {
			return crudExec.fireAndForget(singletonList(pallet), EntityEvent.CREATE, null).iterator().next();
		}
		public Reply add(final Duty duty) {
			return addAll(singletonList(duty)).iterator().next();
		}
		public Collection<Reply> addAll(final Collection<Duty> duty) {
			return crudExec.fireAndForget(duty, EntityEvent.CREATE, null);
		}
		
		public Reply remove(final Duty duty) {
			return removeAll(singletonList(duty)).iterator().next();
		}
		public Collection<Reply> removeAll(final Collection<Duty> coll) {
			return crudExec.fireAndForget(coll, EntityEvent.REMOVE, null);
		}
		public Reply remove(final Pallet pallet) {
			return crudExec.fireAndForget(singletonList(pallet), EntityEvent.REMOVE, null).iterator().next();
		}
		
		public Reply update(final Duty duty) {
			return crudExec.fireAndForget(singletonList(duty), EntityEvent.UPDATE, null).iterator().next();
		}
		public Reply update(final Pallet pallet) {
			return crudExec.fireAndForget(singletonList(pallet), EntityEvent.UPDATE, null).iterator().next();
		}
		public Reply transfer(final Duty duty, final EntityPayload userPayload) {
			return crudExec.fireAndForget(singletonList(duty), EntityEvent.TRANSFER, userPayload).iterator().next();
		}
		public Reply transfer(final Pallet pallet, final EntityPayload userPayload) {
			return crudExec.fireAndForget(singletonList(pallet), EntityEvent.TRANSFER, userPayload).iterator().next();
		}
	}
	
	
}
