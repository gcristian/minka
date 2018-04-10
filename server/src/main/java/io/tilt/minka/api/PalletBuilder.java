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
import io.tilt.minka.core.leader.balancer.Balancer;
import io.tilt.minka.core.leader.balancer.Balancer.BalancerMetadata;

/**
 * 
 * @author Cristian Gonzalez
 * @since Nov 5, 2015
 * 
 * @param <P>	A bytes serializable payload to carry along the wire from the intake point 
 * at <code> MinkaClient.getInstance().add(...) </code> to the Shard where the host app will process it 
 */
public class PalletBuilder<P extends Serializable> {

	private final String id;

	private BalancerMetadata meta;
	private Storage storage;
	private P payload;

	private PalletBuilder(final String id) {
		super();
		Validate.notNull(id);
		Validate.isTrue(id.length() < 512);
		this.id = id;
	}

	/**
	 * <code>PalletBuilder.&lt;String&gt;builder("uniqueIdForPallet")</code>
	 * @param <P> 	a bytes serializable payload to carry along the wire from the intake point to the processing shard 
	 * @param palletId	must be unique across the user's domain
	 * @return		Builder to enter duties to Minka
	 */
	public static <P extends Serializable> PalletBuilder<P> builder(final String palletId) {
		return new PalletBuilder<>(palletId);
	}

	/** 
	 * @param meta a configuration for a {@linkplain Balancer} strategy
	 * @return the builder to keep building  
	 * */
	public PalletBuilder<P> with(final BalancerMetadata meta) {
		Validate.notNull(meta);
		this.meta = meta;
		return this;
	}

	/** 
	 * @param payload	 a bytes seriaizable payload 
	 * @return the builder to keep building
	 * */
	public PalletBuilder<P> with(final P payload) {
		Validate.notNull(payload);
		this.payload = payload;
		return this;
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	public Pallet<P> build() {
		return new Group(id, meta == null ? Balancer.Strategy.EVEN_SIZE.getBalancerMetadata() : meta,
				storage == null ? Storage.CLIENT_DEFINED : storage, 
				payload == null ? id : payload);
	}

	public static class Group<P extends Serializable> implements Pallet<P>, Serializable {

		private static final long serialVersionUID = 4519763920222729635L;

		private final BalancerMetadata meta;
		private final Storage storage;
		private final P value;
		private final Class<P> type;
		private final String id;

		@SuppressWarnings("unchecked")
		private Group(final String id, final BalancerMetadata meta, final Pallet.Storage storage, final P payload) {
			super();
			Validate.notNull(id);
			Validate.notNull(storage);
			this.meta = meta;
			this.storage = storage;
			this.value = payload;
			this.id = id;
			this.type = (Class<P>) payload.getClass();
		}

		@Override
		public int hashCode() {
			return new HashCodeBuilder().append(getId()).toHashCode();
		}
		@Override
		@SuppressWarnings({ "unchecked", "rawtypes" })
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

		@Override
		public String toString() {
			return id;
		}
	}

}
