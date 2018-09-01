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

import io.tilt.minka.domain.Heartbeat;
import io.tilt.minka.shard.DomainInfo;

/**
 * Knows how to build a HB 
 * 
 * @author Cristian Gonzalez
 * @since Nov 17, 2015
 */
public interface HeartbeatFactory {

	/**
	 * Gather follower's status data and build conclusive info for Leader decisions. 
	 * @param forceFullReport	force factory to build a complete detail even when not obliged to.  
	 * @return	a beat with duties taken, differences, consistency details.
	 */
	Heartbeat create(boolean forceFullReport);
	
	/**
	 * Helps the factory to add more information about the shard.
	 * @param domain
	 */
	void setDomainInfo(DomainInfo domain);	
}
