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
package io.tilt.minka.core.follower;

import java.io.InputStream;
import java.util.Collection;

import io.tilt.minka.domain.ShardEntity;
import io.tilt.minka.shard.DomainInfo;

/**
 * Upon leader's reception of instructions: execute them
 * 
 * @author Cristian Gonzalez
 * @since Nov 17, 2015
 */
public interface PartitionManager {

	/** drop all duties caused by a safety policy */
	Void releaseAllOnPolicies();

	/** drop all duties */
	Void releaseAll();

	/** drop duties from follower caused by proper finalization */
	Void finalized(Collection<ShardEntity> duty);

	/** contact follower to update duties payloads */
	Void update(Collection<ShardEntity> duty, InputStream stream);

	/** drop duties from follower, that is stopping them */
	boolean dettach(Collection<ShardEntity> duty);

	/** make the follower responsible of duties */
	boolean attach(Collection<ShardEntity> duty, InputStream stream);

	/** make the follower know leader's domain information */
	Void acknowledge(DomainInfo info);

}
