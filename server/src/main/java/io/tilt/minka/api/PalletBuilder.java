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

import io.tilt.minka.api.Pallet.Storage;
import io.tilt.minka.core.leader.distributor.Balancer;
import io.tilt.minka.core.leader.distributor.Balancer.BalancerMetadata;

/**
 * @author Cristian Gonzalez
 * @since Nov 5, 2015
 */
public class PalletBuilder<P extends Serializable> {

	private final String id;

	private BalancerMetadata meta;
	private Storage storage;
	private Class<?> clazz;
	private P payload;

	private PalletBuilder(final String id) {
		super();
		Validate.notNull(id);
		Validate.isTrue(id.length() < 512);
		this.id = id;
	}

	public static <P extends Serializable> PalletBuilder<P> builder(final String id) {
		return new PalletBuilder<>(id);
	}

	public PalletBuilder<P> with(final BalancerMetadata meta) {
		Validate.notNull(meta);
		this.meta = meta;
		return this;
	}

	public PalletBuilder<P> with(final P payload) {
		Validate.notNull(payload);
		Validate.notNull(clazz);
		this.clazz = payload.getClass();
		this.payload = payload;
		return this;
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	public Pallet<P> build() {
		return new Group(id, meta == null ? Balancer.Strategy.EVEN_SIZE.getBalancerMetadata() : meta,
				storage == null ? Storage.CLIENT_DEFINED : storage, clazz == null ? String.class : clazz, id);
	}

	public static class Group<P extends Serializable> implements Pallet<P>, Serializable {

		private static final long serialVersionUID = 4519763920222729635L;

		private final BalancerMetadata meta;
		private final Storage storage;
		private final P value;
		private final Class<P> type;
		private final String id;

		private Group(final String id, final BalancerMetadata meta, final Pallet.Storage storage, Class<P> clazz,
				final P payload) {
			super();
			Validate.notNull(id);
			Validate.notNull(storage);
			this.meta = meta;
			this.storage = storage;
			this.value = payload;
			this.id = id;
			this.type = clazz;
		}

		@Override
		public int hashCode() {
			return new HashCodeBuilder().append(getId()).toHashCode();
		}
		@Override
		public boolean equals(Object obj) {
			if (obj != null && obj instanceof Entity) {
				if (obj == this) {
					return true;
				} else {
					Entity<P> entity = (Entity) obj;
					return getId().equals(entity.getId());
				}
			} else {
				return false;
			}
		}

		@Override
		public Storage getStorage() {
			return storage;
		}

		@Override
		public BalancerMetadata getMetadata() {
			return meta;
		}

		@Override
		public String getId() {
			return id;
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

}
