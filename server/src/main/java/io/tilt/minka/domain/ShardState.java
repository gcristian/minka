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
package io.tilt.minka.domain;

/**
 * State of life for a cluster node calculated by {@link Sheperd} 
 * 
 * @author Cristian Gonzalez
 * @since Nov 7, 2015
 */
public enum ShardState {
	
    /**
     * all nodes START in this state
     * while becoming Online after a Quarantine period
     */
    JOINING,
	/** 
	 * the node has been continuously online for a long time
	 * so it can trustworthly receive work 
	 */
	ONLINE,
	/**
	 * the node interrupted heartbeats time enough to be
	 * considered not healthly online.
	 * in this state all nodes tend to rapidly go ONLINE or fall GONE 
	 */
	QUARANTINE,
	
	/** 
	 * the node emited a last heartbeat announcing offline mode
	 * either being manually stopped or cleanly shuting down
	 * so its ignored by the master
	 */
	QUITTED,
	
	/**
	 * the server discontinued heartbeats and cannot longer
	 * be considered alive, recover its reserved duties
	 */
	GONE
	
	;
    
    public boolean isAlive() {
        return this == ONLINE || this == QUARANTINE || this == JOINING;
    }
    
    public enum Reason {
        
        INITIALIZING,
	    /*
	     * the shard persistently ignores commands from the Follower shard
	     */
	    REBELL,
	    /* "inconsistent behaviour measured in short lapses"
	     *
	     * not trustworthly shard
	     */
	    FLAPPING,
	    /* "no recent HBs from the shard, long enough" 
	     * 
	     * the node has ceased to send heartbeats for time enough
	     * to be considered gone and unrecoverable
	     * so it is ignored by the master
	     */
	    LOST;
	}
}


