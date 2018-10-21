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
package io.tilt.minka.model;

import java.util.Set;

import io.tilt.minka.api.Config;
import io.tilt.minka.api.crud.Client;
import io.tilt.minka.core.follower.FollowerBootstrap;
import io.tilt.minka.core.leader.LeaderBootstrap;
import io.tilt.minka.core.task.Bootstrap;
import io.tilt.minka.domain.ShardEntity;

/**
 * Starting the {@linkplain Bootstrap} with this delegate will avoid Minka from 
 * maintaining storage for {@linkplain ShardEntity}
 * 
 * Minka will depend entirely on this to obtain the initial duties to distribute  
 * 
 * Adds and Removes from {@linkplain Client} are yet properly functional.
 * Adds and Removes should be ONLY used to enter new Duties to Minka after bootstrap.
 * In case of LeaderBootstrap reelection/termination: Minka recalls this implementation 
 * and forgets previously added duties thru add/remove from {@linkplain Client}
 *    
 * @author Cristian Gonzalez
 * @since Nov 7, 2015
 */
public interface PartitionMaster extends PartitionDelegate {

	/**
	* This is a hint for the {@link LeaderBootstrap} to know in forehand the size
	* of duties and do an initially massive {@link FollowerBootstrap} assignment.
	* 
	* In case of {@linkplain Config} parameter distributorRunConsistencyCheck is TRUE
	* this will be called profusely only to check there's no duty unnassigned or lost.
	* @return	a master (complete) list of duties from the storage
	*/
	Set<Duty> loadDuties();

	Set<Pallet> loadPallets();

}
