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
package io.tilt.minka.core.task;

import java.time.Instant;
import java.util.List;
import java.util.function.Consumer;

import io.tilt.minka.domain.NetworkShardIdentifier;

/**
 * Value object with responsibility of always knowing who the leader is 
 * and a service of Observer pattern adhoc implementation (jdk is bad bad bad)
 * 
 * @author Cristian Gonzalez
 * @since Feb 2, 2016
 *
 */
public interface LeaderShardContainer extends Service {

	void setNewLeader(NetworkShardIdentifier newLeader);

	/**
	 * Add consumer of new leader when is first time or already elected, 
	 * and for latter elections as well    
	 * @param consumerOfNewLeader	a consumer
	 */
	void observeForChange(Consumer<NetworkShardIdentifier> consumerOfNewLeader);

	NetworkShardIdentifier getLeaderShardId();

	NetworkShardIdentifier getPreviousLeaderShardId();

	/* Auditing matters */
	List<NetworkShardIdentifier> getAllPreviousLeaders();

	/* whether the current shard is also the leader */
	boolean imLeader();

	/** @return the last moment a new leader was reelected */
	Instant getLastLeaderChange();

}
