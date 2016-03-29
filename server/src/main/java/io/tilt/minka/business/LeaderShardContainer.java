/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.tilt.minka.business;

import java.util.List;
import java.util.function.Consumer;

import io.tilt.minka.domain.NetworkShardID;

/**
 * Value object with responsibility of always knowing who the leader is 
 * and a service of Observer pattern adhoc implementation (jdk is bad bad bad)
 * 
 * @author Cristian Gonzalez
 * @since Feb 2, 2016
 *
 */
public interface LeaderShardContainer extends Service {

    void setNewLeader(NetworkShardID newLeader);
    
    /**
     * Add consumer of new leader when is first time or already elected, 
     * and for latter elections as well    
     * @param consumerOfNewLeader
     */
    void observeForChange(Consumer<NetworkShardID> consumerOfNewLeader);
    
    NetworkShardID getLeaderShardId();
            
    NetworkShardID getPreviousLeaderShardId();    

    /* Auditing matters */
    List<NetworkShardID> getAllPreviousLeaders();
    
    /* whether the current shard is also the leader */
    boolean imLeader();
    
}
