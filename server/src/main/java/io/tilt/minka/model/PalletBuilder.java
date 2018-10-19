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
package io.tilt.minka.model;

import java.io.InputStream;
import java.io.Serializable;
import java.util.function.Supplier;

import org.apache.commons.lang.Validate;

import io.tilt.minka.core.leader.balancer.Balancer;
import io.tilt.minka.core.leader.balancer.Balancer.BalancerMetadata;

/**
 * 
 * @author Cristian Gonzalez
 * @since Nov 5, 2015
 * 
 * @param <P>	A bytes serializable payload to carry along the wire from the intake point 
 * at <code> Client.getInstance().add(...) </code> to the Shard where the host app will process it 
 */
public class PalletBuilder {

	private final String id;

	private BalancerMetadata meta;
	private Supplier<InputStream> payload;

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
	protected static PalletBuilder builder(final String palletId) {
		return new PalletBuilder(palletId);
	}

	/** 
	 * @param meta a configuration for a {@linkplain Balancer} strategy
	 * @return the builder to keep building  
	 * */
	public PalletBuilder with(final BalancerMetadata meta) {
		Validate.notNull(meta);
		this.meta = meta;
		return this;
	}

	/** 
	 * @param payload	 a bytes seriaizable payload 
	 * @return the builder to keep building
	 * */
	public PalletBuilder with(final Supplier<InputStream> payload) {
		Validate.notNull(payload);
		this.payload = payload;
		return this;
	}

	public Pallet build() {
		return new Group(id, meta == null ? Balancer.Strategy.EVEN_SIZE.getBalancerMetadata() : meta, payload);
	}

	public static class Group implements Pallet, Serializable {

		private static final long serialVersionUID = 4519763920222729635L;

		private final BalancerMetadata meta;
		private final Supplier<InputStream> value;
		private final String id;

		private Group(
		        final String id, 
		        final BalancerMetadata meta, 
		        final Supplier<InputStream> payload) {
			super();
			Validate.notNull(id);
			this.meta = meta;
			this.value = payload;
			this.id = id;
		}

		@Override
		public int hashCode() {
			final int prime = 31;
			int res = 1;
			res *= prime + (id==null ? 1 : id.hashCode());
			return res;
		}
		
		@Override
		public boolean equals(Object obj) {
			if (obj != null && obj instanceof Entity) {
				if (obj == this) {
					return true;
				} else {
					Entity entity = (Entity) obj;
					return getId().equals(entity.getId());
				}
			} else {
				return false;
			}
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
		public String toString() {
			return id;
		}
	}

}
