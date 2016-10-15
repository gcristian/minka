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
public abstract interface Entity<T extends Serializable> extends Comparable<Entity<T>>, Comparator<Entity<T>> {

	Class<T> getClassType();

	/**
	 * Duties have a payload as Pallets do.
	 * 
	 * Pallets may have their own payload, to be accesible and shared by their conforming duties.  
	 * They will be carried to shards only the first time they appear in the shard's life-cycle. 
	 * Unless they change thru {@linkplain MinkaClient}, ending up {@linkplain PartitionDelegate} being called.
	 * 
	 * In case of large objects, you should only build the return object when the method is call.
	 * It will be kept useless in memory until you use it again, 
	 * as Minka only calls this method but at the time of shard transportation.   
	 * 
	 * @return  any data that can be transported thru TCP-IP
	 */
	T get();

	/**
	* Type erasure bans the chance to call a useful equals() on the impl.
	* @return
	*/
	String getId();

	@Override
	default public int compareTo(Entity<T> o) {
		return compare(this, o);
	}

	@Override
	default public int compare(Entity<T> o1, Entity<T> o2) {
		return o1.getId().compareTo(o2.getId());
	}

}