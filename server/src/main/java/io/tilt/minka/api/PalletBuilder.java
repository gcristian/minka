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
import java.util.List;

import org.apache.commons.lang.builder.HashCodeBuilder;

import io.tilt.minka.core.leader.distributor.Balancer.BalanceStrategy;
import io.tilt.minka.domain.Shard;

/**
 * Representation of a {@linkplain Duty} selected for an action in a {@linkplain Shard}  
 * 
 * @author Cristian Gonzalez
 * @since Nov 5, 2015
 */
public class PalletBuilder<P extends Serializable> implements Pallet<P>, Serializable {

		private static final long serialVersionUID = 4519763920222729635L;

		private final BalanceStrategy balanceStrategy;
		private final Storage storage;
		private final List<DutyAttribute> attribute;
		private final P value;
		private final Class<P> type;
		private final String id;

		private PalletBuilder(String id, Class<P> clas, BalanceStrategy balanceStrategy, Pallet.Storage storage,
				List<DutyAttribute> attribute, P payload) {
			super();
			this.balanceStrategy = balanceStrategy;
			this.storage = storage;
			this.attribute = attribute;
			this.value = payload;
			this.id = id;
			this.type = clas;
		}

		public static <P extends Serializable> PalletBuilder<P> build(String id, Class<P> clas) {
			return new PalletBuilder<P>(id, clas, null, null, null, null);
		}

		public static <P extends Serializable> PalletBuilder<P> build(String id, Class<P> clas, P payload) {
			return new PalletBuilder<P>(id, clas, null, null, null, payload);
		}

		public static <P extends Serializable> PalletBuilder<P> build(String id, Class<P> clas, BalanceStrategy strategy, 
				Storage storage, P payload) {
			return new PalletBuilder<P>(id, clas, strategy, storage, null, payload);
		}

		@Override
		public Storage getStorage() {
			return storage;
		}

		@Override
		public BalanceStrategy getBalanceStrategy() {
			return balanceStrategy;
		}

		@Override
		public List<DutyAttribute> getAttributes() {
			return attribute;
		}

		@Override
		public String getId() {
			return id;
		}
		

		@Override
		public int hashCode() {
			return new HashCodeBuilder().append(getId()).toHashCode();
		}
		
		@Override
		public boolean equals(Object obj) {
			if (obj != null && obj instanceof Entity) {
				Entity<P> entity = (Entity) obj;
				return getId().equals(entity.getId());
			} else {
				return false;
			}
		}

		@Override
		public Class<P> getClassType() {
			return type;
		}

		@Override
		public P get() {
			return value;
		}
		
		public Class<P> getType() {
			return this.type;
		}

		@Override
		public String toString() {
			return id;
		}

}
