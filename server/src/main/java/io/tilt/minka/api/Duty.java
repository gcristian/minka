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
import java.util.Comparator;

import io.tilt.minka.domain.ShardEntity;

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
	* @return a representation in the same measure unit than the delegate's pallet capacity 
	* Required to maintain a fairly load balancing  */
	double getWeight();

	/** @return the pallet id to which this duty must be grouped into. */		
	String getPalletId();
	
	/** @return not mandatory only for Client usage */ 
	Pallet<?> getPallet();
	
	/**
	 * @return whether or not the absence this duty in the delegate's report
	 * must be interpreted as a normal finalization. 
	 * Otherwise it will be missing and schedule for reattachment
	 */
	boolean isLazyFinalized();

	/** 
	 * @return whether or not this duty can migrate for balancing purposes.
	 * Generally true if the Client doesn't link to local machine resources and
	 * the duty can be paused and restarted at a different machine, 
	 * for which it may require a distributed savepoints facility if depends of state
	 * 
	 * Minka doesnt yet support in-memory snapshot of the Duty representation for continuos state recording.
	 * Such a feature depends on stronger partitiontable replication flows and wider CRUD consistency issues.  
	 */
	boolean isIdempotent();

	/** @return whether of not this Duty cannot be balanced and it must cohabitat all Minka shards */
	boolean isSynthetic();

	public static class WeightComparer implements Comparator<ShardEntity>, Serializable {

		private static final long serialVersionUID = 2191475545082914908L;

		@Override
		public int compare(final ShardEntity o1, final ShardEntity o2) {
			return Double.compare(o1.getDuty().getWeight(), o2.getDuty().getWeight());
		}

	}

}