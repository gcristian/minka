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

import io.tilt.minka.domain.Workload;

/**
 * An abstract entity that the host application uses to represent anything able to balance and distribute.  
 * the user MUST guarantee to TAKE and RELEASE responsibilities when the user's {@link PartitionDelegate} 
 * receives events:  {@link ShardDutyEvent.ASSIGNMENT} or {@link ShardDutyEvent.DISASSIGNMENT} respectively.
 * Wrapped to keep generics matters isolated
 * 
 * Conditions:
 *  1)  implement hashCode() and equals() for consistency with your {@linkplain PartitionDelegate} 
 *  2)  If you handle storage: you must avoid collissions
 *   
 * @author Cristian Gonzalez
 * @since Dec 3, 2015
 * 
 * @param <T>
 */
public interface Duty<T extends Serializable> extends Entity<T> {
		/**
		* Required to maintain a fairly load balancing  
		* @return
		*/
		Workload getWeight();

		/**
		* The pallet ID to which this duty must be grouped into.
		* @return
		*/
		String getPalletId();

}