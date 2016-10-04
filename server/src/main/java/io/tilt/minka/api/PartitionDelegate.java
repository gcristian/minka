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

import java.io.Serializable;
import java.util.List;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.tilt.minka.core.task.Bootstrap;
import io.tilt.minka.domain.EntityEvent;
import io.tilt.minka.domain.Shard;
import io.tilt.minka.domain.ShardEntity;
import io.tilt.minka.domain.ShardState;

/**
 * Client's point of integration with Minka.
 * Minka will be heavily calling this methods to control shards.
 * 
 * Starting the {@linkplain Bootstrap} with this delegate will make Minka to maintain its own 
 * storage for {@linkplain ShardEntity}
 * 
 * Client is compelled to call {@linkplain MinkaClient.add/remove} to enter and remove duties to the shards
 *    
 * @author Cristian Gonzalez
 * @since Nov 7, 2015
 */
public interface PartitionDelegate<E extends Serializable, P extends Serializable> {

		Logger logger = LoggerFactory.getLogger(PartitionDelegate.class);

		/**
		 * Is the service ready for sharding ?
		 * Sometimes a Shard serer needs of external or non propietary events to start collaborating
		 * 
		 * @return  Leader and Follower will not command until this returns true
		 */
		default boolean isReady() {
			logger.info("{}: this PartitionDelegate has not implemented the readyness question (default: true)",
						getClass().getSimpleName());
			return true;
		}

		
		long getMaxWeight(Pallet<P> pallet);
		
		/**
		* Instruct the Follower shard to take management responsibilities on these duties
		*/
		void take(Set<Duty<E>> duties);

		default void takePallet(Set<Pallet<P>> pallets) {
		}

		/**
		* Instruct the Follower shard to release management 
		* responsibi﻿lities on these duties.
		* Not doing so will make Minka apply rules set in {@linkplain Config} about {@linkplain ShardState}
		*/
		void release(Set<Duty<E>> duties);

		default void releasePallet(Set<Duty<E>> duties) {
		}

		/**
		 * Instruct the Follower shard to acknowledge an update ocurred on these duties
		 * @param duties
		 */
		default void update(Set<Duty<E>> duties) {
			logger.error("{}: this PartitionDelegate has not implemented the duty update event",
						getClass().getSimpleName());
		}

		/**
		 * Instruct te Follower shard to get a client payload event for a particular duty 
		 * @param duties
		 */
		default void receive(Set<Duty<E>> duty, Serializable clientPayload) {
			logger.error("{}: this PartitionDelegate has not implemented the payload reception event",
						getClass().getSimpleName());
		}
		
		/**
		 * Report shard's maximum workload capacity for a certain pallet
		 * @return
		 */
		default long getShardMaxloadByPallet(Pallet<?> pallet) {
			return -1;
		}

		/**
		* Notify the Leader shard of an event over these duties.
		* Hook facility for the User to know about {@linkplain ShardEntity} CRUD from other {@linkplain Shard}.
		* 
		* i.e. shards are workers, they dont know if an assignation is from a recent CRUD event,
		* or if the duty belonged to a recent fallen shard.
		* So may be the leader shard needs to have the big picture, because shards are shards.   
		*  
		* @param duties
		*/
		default void notify(List<Duty<E>> duties, EntityEvent event) {
			logger.error("{}: this PartitionDelegate has not implemented the duty notification event",
						getClass().getSimpleName());
		}

		/**
		* Report the effectively sharded duties being handled.
		* The Leader cannot trust that a delegated duty is really held by a shard,
		* So the shard has the last word and must reports so at any given time.
		* 
		* @return	a list of handled duties
		*/
		Set<Duty<E>> reportTaken();

		/**
		* Continue performing actions on already taken duties.
		* No special calls to take() will be done
		*/
		default void play() {
			throw new RuntimeException("this delegate does not implements play: " + getClass().getSimpleName());
		}

		/**
		* Prepare for taking duties
		*/
		default void activate() {
			logger.error("{}: this PartitionDelegate has not implemented the activation call", getClass().getSimpleName());
		}

		/**
		* Pause performing actions on taken duties without forgetting them.
		* Taken duties are not called for release.
		*/
		default void pause() {
			throw new RuntimeException("this delegate does not implements pause: " + getClass().getSimpleName());
		}

		/**
		* Stop performing actions on taken duties.
		* Release any taken resources
		*/
		default void deactivate() {
			logger.error("{}: this PartitionDelegate has not implemented the de-activation call",
						getClass().getSimpleName());
		}

		default void setPartitionService(final MinkaClient service) {
			logger.error("{}: this PartitionDelegate has not implemented the setPartitionDelegate(..) method",
						getClass().getSimpleName());
		}

}
