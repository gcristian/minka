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
package io.tilt.minka.domain;

import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

import org.apache.commons.lang.Validate;

import com.google.common.collect.Lists;

import io.tilt.minka.api.Duty;
import io.tilt.minka.api.Pallet;

/**
 * An effectively assigned, running and continuously confirmed 
 * set of {@linkplain ShardEntity} in a given {@linkplain Shard}
 *  
 * @author Cristian Gonzalez
 * @since Dec 13, 2015
 *
 */
public class AttachedPartition {

	private final NetworkShardID id;
	private Set<ShardEntity> duties;
	private Set<ShardEntity> pallets;

	public static AttachedPartition partitionForFollower(final NetworkShardID shardId) {
		return new AttachedPartition(shardId);
	}

	public AttachedPartition(final NetworkShardID shardId) {
		this.id = shardId;
		init();
	}

	/**
	 * @return  the sum of all weights present in these duties
	 */
	public double getWeight() {
		return getWeight_(null);
	}
	public double getWeight(final Pallet<?> pallet) {
		Validate.notNull(pallet);
		return getWeight_(pallet);
	}
	private double getWeight_(final Pallet<?> p) {
		double weight = 0;
		for (final ShardEntity duty : duties) {
			if (p==null || p.getId().equals(duty.getDuty().getPalletId())) {
				weight += duty.getDuty().getWeight();
			}
		}
		return weight;
	}

	private void init() {
		this.duties = new TreeSet<>();
		this.pallets = new TreeSet<>();
	}

	public NetworkShardID getId() {
		return id;
	}

	public void clean() {
		init();
	}

	@Override
	public String toString() {
		final StringBuilder sb = new StringBuilder();
		final List<ShardEntity> sorted = Lists.newArrayList(duties);
		if (!sorted.isEmpty()) {
			sorted.sort(sorted.get(0));
		}
		sorted.forEach(i -> sb.append(i.toBrief()).append(", "));
		return sb.toString();
	}

	public ShardEntity forDuty(final Duty<?> t) {
		for (ShardEntity shardDuty : duties) {
			if (shardDuty.getDuty().getId().equals(t.getId())) {
				return shardDuty;
			}
		}
		return null;
	}
	
	public Set<ShardEntity> getDuties(final Pallet<?> pallet) {
		return this.duties.stream().filter(d->d.getDuty().getPalletId().equals(pallet.getId()))
				.collect(Collectors.toSet());
	}
	
	public boolean remove(final ShardEntity duty) {
		return this.duties.remove(duty);
	}

	public boolean containsDuty(final ShardEntity duty) {
		return this.duties.contains(duty);
	} 	

	public Set<ShardEntity> getDuties() {
		return duties;
	}
	
	public Set<ShardEntity> getPallets() {
		return this.pallets;
	}

}
