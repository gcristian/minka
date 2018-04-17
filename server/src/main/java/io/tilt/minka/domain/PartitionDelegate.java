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
package io.tilt.minka.domain;

import java.io.Serializable;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.tilt.minka.api.Duty;
import io.tilt.minka.api.Pallet;

/**
 * Traditional repr. of the client implementation model's contract. 
 * May be used instead of using {@linkplain Minka} class more functional approach 
 * which saves suppliers and consumers at {@linkplain ConsumerDelegate}
 *    
 * @author Cristian Gonzalez
 * @since Nov 7, 2015
 */
public interface PartitionDelegate<D extends Serializable, P extends Serializable> {

	public static final String METHOD_NOT_IMPLEMENTED = "{}: this PartitionDelegate has not implemented event: {}";
	Logger logger = LoggerFactory.getLogger(PartitionDelegate.class);

	/*
	* Instruct the Follower shard to take management responsibilities on these duties
	*/
	void capture(Set<Duty<D>> duties);
	void capturePallet(Set<Pallet<P>> pallets);

	/*
	* Instruct the Follower shard to release management 
	* responsibi﻿lities on these duties.
	* Not doing so will make Minka apply rules set in {@linkplain Config} about {@linkplain ShardState}
	*/
	void release(Set<Duty<D>> duties);
	void releasePallet(Set<Pallet<P>> pallets);

	/*
	 * Instruct the Follower shard to acknowledge an update ocurred on a duty's payload
	 */
	default void update(Duty<D> duties) {
		logger.error(METHOD_NOT_IMPLEMENTED, getClass().getSimpleName(), "update");
	}
	default void update(Pallet<P> pallets) {
		logger.error(METHOD_NOT_IMPLEMENTED, getClass().getSimpleName(), "update");
	}
	/*
	 * Instruct te Follower shard to get a client payload event for a particular duty 
	 */
	default void transfer(Duty<D> duty, Serializable clientPayload) {
		logger.error(METHOD_NOT_IMPLEMENTED, getClass().getSimpleName(), "transfer");
	}
	default void transfer(Pallet<P> duty, Serializable clientPayload) {
		logger.error(METHOD_NOT_IMPLEMENTED, getClass().getSimpleName(), "transfer");
	}

	/*
	 * Report shard's maximum workload capacity for a certain pallet
	 * @return	a unit in the same measure unit than duty weights reported 
	 */
	default double getTotalCapacity(Pallet<P> pallet) {
		return -1;
	}
	
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
		logger.warn(METHOD_NOT_IMPLEMENTED, getClass().getSimpleName(), "activation");
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
		logger.warn(METHOD_NOT_IMPLEMENTED, getClass().getSimpleName(), "deactivation");
	}

	/**
	 * Is the service ready for sharding ?
	 * Sometimes a Shard serer needs of external or non propietary events to start collaborating
	 * 
	 * @return  Leader and Follower will not command until this returns true
	 */
	default boolean isReady() {
		logger.warn(METHOD_NOT_IMPLEMENTED, getClass().getSimpleName(), "readyness");
		return true;
	}


}
