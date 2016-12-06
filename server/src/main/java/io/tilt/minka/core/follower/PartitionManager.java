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

import java.util.Collection;

import io.tilt.minka.domain.DomainInfo;
import io.tilt.minka.domain.ShardEntity;

/**
 * Pretty obvious mission 
 * 
 * @author Cristian Gonzalez
 * @since Nov 17, 2015
 */
public interface PartitionManager {

	Void releaseAllOnPolicies();

	Void releaseAll();

	Void finalized(Collection<ShardEntity> duty);

	Void update(Collection<ShardEntity> duty);

	Void dettach(Collection<ShardEntity> duty);

	Void attach(Collection<ShardEntity> duty);

	Void acknowledge(DomainInfo info);

}
