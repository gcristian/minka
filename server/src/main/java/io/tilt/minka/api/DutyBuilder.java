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

import org.apache.commons.lang.Validate;
import org.apache.commons.lang.builder.HashCodeBuilder;

import io.tilt.minka.domain.Workload;

/**
 * A plain simple duty that can be stored in maps, sets, compared to others, etc.
 * Instance identity is upon ID param.
 * 
 * This builder does not support large binary payloads as duties are loaded into memory and transported 
 *  
 * @author Cristian Gonzalez
 * @since Jan 5, 2016
 * 
 * @param <T>
 */
//@SuppressWarnings({ "unchecked", "rawtypes" })
public class DutyBuilder<T extends Serializable> implements Duty<T>, EntityPayload {

		private static final long serialVersionUID = 4976043821619752116L;

		private final String id;
		private final String palletId;
		private final long load;
		private final long maxLoad;
		private final T value;
		private Class<T> type;

		/**
		 * Create a simple Duty representing an ID without payload or content
		 * Duties with same ID will be handled as equals
		 * @param id    must be a unique ID within your domain
		 * @param load  the work load associated to this duty at execution time
		 * @return
		 */
		public static <T extends Serializable> DutyBuilder<T> build(Class<T> clas, final String id, final String palletId, final long load) {
			return new DutyBuilder<T>(clas, (T)id, id, load, load, palletId);
		}

		/**
		 * Create a simple Duty representing an ID without payload or content
		 * Duties with same ID will be handled as equals
		 * @param id    must be a unique ID within your domain
		 * @return
		 */
		@SuppressWarnings("unchecked")
		public static <T extends Serializable> DutyBuilder<T> build(Class<T> clas, final String id, final String palletId) {
			return new DutyBuilder<T>(clas, (T)id, id, 1l, 1l, palletId);
		}


		/**
		 * Create a simple Duty with a payload.
		 * Such payload is not read by Minka Only de/serialized. 
		 * @param type          class object for the payload
		 * @param value         the payload traveling from Leader to Follower
		 * @param id            must be a unique ID within your domain
		 * @param load          the work load associated to this duty at execution time
		 * @return
		 */
		public static <T extends Serializable> DutyBuilder<T> build(Class<T> type, final T value, final String id, final String palletId,
				final long load) {
			return new DutyBuilder<T>(type, value, id, load, load, palletId);
		}

		/**
		 * Create a simple Duty with a payload.
		 * Such payload is not read by Minka Only de/serialized. 
		 * @param type          class object for the payload
		 * @param value         the payload traveling from Leader to Follower
		 * @param id            must be a unique ID within your domain
		 * @param load          the work load associated to this duty at execution time
		 * @param maxLoad       the maximum work load (limit) that your domains recognizes for a duty
		 * @return
		 */
		public static <T extends Serializable> DutyBuilder<T> build(final Class<T> type, final T value, final String id, final String palletId,
				final long load, final long maxLoad) {
			return new DutyBuilder<T>(type, value, id, load, maxLoad, palletId);
		}

		private DutyBuilder(final Class<T> class1, final T payload, final String id, final long load, final long maxLoad,
				final String palletId) {
			this.id = id;
			this.palletId = palletId;
			this.load = load;
			this.value = payload;
			this.type = class1;
			this.maxLoad = maxLoad;
			validateBuiltParams(this);
		}
		
		public static void validateBuiltParams(final Duty<?> duty) {
			Validate.notNull(duty.getId(), "A non null ID is required");
			final String id = "Duty:" + duty.getId() + " ";
			Validate.notNull(duty.getPalletId(), id + "a Pallet ID is mandatory");
			Validate.notNull(duty.getClassType(), id + "You must specify param's class or use overload builder");
			Validate.notNull(duty.get(), id + "You must specify payload param or use overload builder");
			Validate.isTrue(duty.getWeight().getLoad() > 0, id + "A number greater than 0 expected for workload representing the duty");
			Validate.notNull(duty.getWeight().getMaxLoad(), id + "A number greater than 0 expected for the maximum workload (limit) in your domain");
		}

		@Override
		public boolean equals(Object obj) {
			if (obj != null && obj instanceof Entity) {
				@SuppressWarnings("unchecked")
				Entity<T> entity = (Entity<T>) obj;
				return getId().equals(entity.getId());
			} else {
				return false;
			}
		}

		@Override
		public int hashCode() {
			return new HashCodeBuilder().append(getId()).toHashCode();
		}

		@Override
		public String toString() {
			return getId();
		}

		@Override
		public Class<T> getClassType() {
			return type;
		}

		@Override
		public T get() {
			return value;
		}

		@Override
		public Workload getWeight() {
			return new Workload(load, maxLoad);
		}
		
		@Override
		public String getId() {
			return id;
		}

		@Override
		public String getPalletId() {
			return palletId;
		}

}