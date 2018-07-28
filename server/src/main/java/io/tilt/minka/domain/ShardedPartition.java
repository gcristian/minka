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
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import org.apache.commons.lang.Validate;

import io.tilt.minka.api.Duty;
import io.tilt.minka.api.Pallet;
import io.tilt.minka.shard.NetworkShardIdentifier;
import io.tilt.minka.shard.Shard;

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

	// the follower's shard
	private final NetworkShardIdentifier id;
	// effectively sharded (attached) on the follower's shard
	private Map<Duty, ShardEntity> duties;
	// kept as knowledge to inform at leader reelection 
	// not attached at follower's shard BUT at leader's shard
	private Set<ShardEntity> stock;
	private Map<Pallet, ShardEntity> pallets;
	private long lastUpdateTimestamp;
	private long recentUpdateThreshold = 10 *1000l;

	public static ShardedPartition partitionForFollower(final NetworkShardIdentifier shardId) {
		return new ShardedPartition(shardId);
	}

	ShardedPartition(final NetworkShardIdentifier shardId) {
		this.id = shardId;
		init();
	}

	/**
	 * @return  the sum of all weights present in these duties
	 */
	double getWeight() {
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
		this.stock = new TreeSet<>();
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
	// add to partition because it's currently attached: captured
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
	
	//////////////////////////////////////////////////////////////////////////////////

	public Collection<ShardEntity> getStock() {
		return this.stock;
	}
	// add to domain duties: not attached
	public boolean stock(final ShardEntity entity) {
		return stock.add(entity);
	}
	public boolean stockAll(final Collection<ShardEntity> entities) {
		return stock.addAll(entities);
	}
	public boolean drop(final ShardEntity entity) {
		return stock.remove(entity);
	}
	public boolean dropAll(final Collection<ShardEntity> entities) {
		return stock.removeAll(entities);
	}
	public void dropAll() {
		stock.clear();
	}

	
}
