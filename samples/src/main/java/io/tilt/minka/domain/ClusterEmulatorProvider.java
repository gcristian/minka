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

import java.util.Set;

import io.tilt.minka.api.Duty;
import io.tilt.minka.api.Pallet;

/** 
 * a basic cluster emulator that provides constant contents (the same entities in every shard)
 * every time a different shard takes leadership role, and reports specific capacities for every shard configured. 
 */
public interface ClusterEmulatorProvider {
	
    Set<Duty<String>> loadDuties();

    Set<Pallet<String>> loadPallets();
    
    double loadShardCapacity(
    		Pallet<String> pallet, 
    		Set<Duty<String>> allDuties, 
    		String shardIdentifier);	    
}