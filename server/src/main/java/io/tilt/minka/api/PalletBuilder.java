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

import org.apache.commons.lang.builder.HashCodeBuilder;

import io.tilt.minka.core.leader.distributor.Balancer;
import io.tilt.minka.core.leader.distributor.Balancer.Strategy;
import io.tilt.minka.domain.Shard;

/**
 * Representation of a {@linkplain Duty} selected for an action in a {@linkplain Shard}  
 * 
 * @author Cristian Gonzalez
 * @since Nov 5, 2015
 */
public class PalletBuilder<P extends Serializable> implements Pallet<P>, Serializable {

	private static final long serialVersionUID = 4519763920222729635L;

	private final Class<? extends Balancer> balancer;
	private final Storage storage;
	private final P value;
	private final Class<P> type;
	private final String id;

	private PalletBuilder(final String id, Class<P> clas, final Class<? extends Balancer> balancer, 
			final Pallet.Storage storage, final P payload) {
		super();
		this.balancer = balancer;
		this.storage = storage;
		this.value = payload;
		this.id = id;
		this.type = clas;
	}

	public static <P extends Serializable> PalletBuilder<P> build(final String id, final Class<P> clas) {
		return new PalletBuilder<P>(id, clas, null, null, null);
	}

	public static <P extends Serializable> PalletBuilder<P> build(final String id, final Class<P> clas, 
			final P payload) {
		return new PalletBuilder<P>(id, clas, null, null, payload);
	}

	public static <P extends Serializable> PalletBuilder<P> build(final String id, final Class<P> clas, 
			final Strategy strategy, final Storage storage, final P payload) {
		return new PalletBuilder<P>(id, clas, strategy.getBalancer(), storage, payload);
	}

	@Override
	public Storage getStorage() {
		return storage;
	}

	@Override
	public Class<? extends Balancer> getStrategy() {
		return balancer;
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
			if (obj == this ) {
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
