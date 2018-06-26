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

import static java.util.Collections.unmodifiableCollection;

import java.util.Collection;
import java.util.Map;
import java.util.TreeMap;

import org.apache.commons.lang.Validate;

import io.tilt.minka.api.Duty;
import io.tilt.minka.api.Pallet;

/**
 * At leader, the ChangeDetector read-writes to maintain the knowledge about where is what. 
 * At follower, feeds the HeartbeatFactory agent. 
 * An effectively assigned, running and continuously confirmed 
 * set of {@linkplain ShardEntity} in a given {@linkplain Shard}
 *  
 * @author Cristian Gonzalez
 * @since Dec 13, 2015
 *
 */
public class ShardedPartition {

	// the where
	private final NetworkShardIdentifier id;
	// the what
	private Map<Duty, ShardEntity> duties;
	private Map<Pallet, ShardEntity> pallets;
	private long lastUpdateTimestamp;
	private long recentUpdateThreshold = 10 *1000l;

	public static ShardedPartition partitionForFollower(final NetworkShardIdentifier shardId) {
		return new ShardedPartition(shardId);
	}

	public ShardedPartition(final NetworkShardIdentifier shardId) {
		this.id = shardId;
		init();
	}

	/**
	 * @return  the sum of all weights present in these duties
	 */
	public double getWeight() {
		return getWeight_(null);
	}
	public double getWeight(final Pallet pallet) {
		Validate.notNull(pallet);
		return getWeight_(pallet);
	}
	private double getWeight_(final Pallet p) {
		double weight = 0;
		for (final Duty duty : duties.keySet()) {
			if (p==null || p.getId().equals(duty.getPalletId())) {
				weight += duty.getWeight();
			}
		}
		return weight;
	}

	private void init() {
		this.duties = new TreeMap<>();
		this.pallets = new TreeMap<>();
	}

	public NetworkShardIdentifier getId() {
		return id;
	}

	public void clean() {
		init();
	}

	@Override
	public String toString() {
		final StringBuilder sb = new StringBuilder();
		duties.values().forEach(i -> sb.append(i.toBrief()).append(", "));
		return sb.toString();
	}

	public ShardEntity getFromRawDuty(final Duty t) {
		for (Duty d : duties.keySet()) {
			if (d.getId().equals(t.getId())) {
				return duties.get(d);
			}
		}
		return null;
	}
	
	public ShardEntity getByDuty(final Duty duty) {
		return this.duties.get(duty);
	}
	
	public Collection<ShardEntity> getDuties() {
		return unmodifiableCollection(duties.values());
	}

	public Collection<ShardEntity> getPallets() {
		return unmodifiableCollection(pallets.values());
	}
	public long getDutiesSize(final Pallet pallet) {
		return this.duties.keySet().stream()
				.filter(d->d.getPalletId().equals(pallet.getId()))
				.count();
	}
	public void addAllPallets(final Collection<ShardEntity> all) {
		all.forEach(p->pallets.put(p.getPallet(), p));
		updateLastChange();
	}
	public void addAllDuties(final Collection<ShardEntity> all) {		
		all.forEach(d->duties.put(d.getDuty(), d));
		updateLastChange();
	}
	public boolean add(final ShardEntity entity) {
		updateLastChange();
		if (entity.getType() == ShardEntity.Type.DUTY) {
			return duties.put(entity.getDuty(), entity) == null;
		} else if (entity.getType() == ShardEntity.Type.PALLET) {
			return pallets.put(entity.getPallet(), entity) == null;
		}
		return false;
	}
	public boolean remove(final ShardEntity entity) {
		updateLastChange();
		if (entity.getType() == ShardEntity.Type.DUTY) {
			return duties.remove(entity.getDuty()) !=null;
		} else if (entity.getType() == ShardEntity.Type.PALLET) {
			return pallets.remove(entity.getPallet()) !=null;
		}
		return false;
	}
	public void removeAll(final ShardEntity.Type type) {
		updateLastChange();
		(type==ShardEntity.Type.DUTY ? duties : pallets).clear();
	}
	public boolean contains(final ShardEntity entity) {
		if (entity.getType() == ShardEntity.Type.DUTY) {
			return duties.containsKey(entity.getDuty());
		} else if (entity.getType() == ShardEntity.Type.PALLET) {
			return pallets.containsKey(entity.getPallet());
		}
		return false;
	}

	public boolean wasRecentlyUpdated() {
		return (System.currentTimeMillis() - lastUpdateTimestamp) < recentUpdateThreshold;
	}
	private void updateLastChange() {
		this.lastUpdateTimestamp = System.currentTimeMillis();
	}

}
