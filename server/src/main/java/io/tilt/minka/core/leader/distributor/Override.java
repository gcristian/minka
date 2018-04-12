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

package io.tilt.minka.core.leader.distributor;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import io.tilt.minka.api.Pallet;
import io.tilt.minka.core.leader.PartitionTable;
import io.tilt.minka.domain.EntityEvent;
import io.tilt.minka.domain.Shard;
import io.tilt.minka.domain.ShardEntity;
import io.tilt.minka.domain.EntityState;

/**
 * A migration strategy in the override style which replaces a shard's content with given duties.
 * it computes to a delta change considering currently already assigned duties on the target shard.
 */
public class Override {

	private final Pallet<?> pallet;
	private final Shard shard;
	private final Set<ShardEntity> entities;
	private final double remainingCap;
	
	Override(final Pallet<?> pallet, final Shard shard, final Set<ShardEntity> entities, final double remainingCap) {
		super();
		this.pallet = pallet;
		this.shard = shard;
		this.entities = entities;
		this.remainingCap = remainingCap;;
	}
	Shard getShard() {
		return this.shard;
	}
	Set<ShardEntity> getEntities() {
		return this.entities;
	}
	double getRemainingCap() {
		return this.remainingCap;
	}

	boolean apply(final ChangePlan changePlan, final PartitionTable table) {
		boolean anyChange = false;
		final Set<ShardEntity> current = table.getScheme().getDutiesByShard(pallet, getShard());
		if (Migrator.log.isDebugEnabled()) {
			Migrator.log.debug("{}: cluster built {}", getClass().getSimpleName(), getEntities());
			Migrator.log.debug("{}: currents at shard {} ", getClass().getSimpleName(), current);
		}
		anyChange |= dettachDelta(changePlan, getEntities(), getShard(), current);
		anyChange |= attachDelta(changePlan, getEntities(), getShard(), current);
		if (!anyChange) {
			Migrator.log.info("{}: Shard: {}, unchanged", getClass().getSimpleName(), shard);
		}
		return anyChange;
	}
	 
		/* dettach anything living in the shard outside what's coming
	    * null or empty cluster translates to: dettach all existing */
	private final boolean dettachDelta(
			final ChangePlan changePlan, 
			final Set<ShardEntity> clusterSet, 
			final Shard shard, 
			final Set<ShardEntity> currents) {

		List<ShardEntity> detaching = clusterSet == null ? new ArrayList<>(currents) : currents.stream()
				.filter(i -> !clusterSet.contains(i))
				.collect(Collectors.toList());

		if (!detaching.isEmpty()) {
			final StringBuilder logg = new StringBuilder(detaching.size() * 16);
			for (ShardEntity detach : detaching) {
				detach.getJournal().addEvent(EntityEvent.DETACH,
						EntityState.PREPARED,
						shard.getShardID(),
						changePlan.getId());
				changePlan.ship(shard, detach);
				logg.append(detach.getEntity().getId()).append(", ");
			}
			Migrator.log.info("{}: Shipping dettaches from: {}, duties: (#{}) {}",
					getClass().getSimpleName(),
					shard.getShardID(),
					detaching.size(),
					logg.toString());
			return true;
		}
		return false;
	}

	/* attach what's not already living in that shard */
	private final boolean attachDelta(
			final ChangePlan changePlan, 
			final Set<ShardEntity> clusterSet, 
			final Shard shard, 
			final Set<ShardEntity> currents) {

		StringBuilder logg;
		if (clusterSet != null) {
			final List<ShardEntity> attaching = clusterSet.stream()
					.filter(i -> !currents.contains(i))
					.collect(Collectors.toList());
			if (!attaching.isEmpty()) {
				logg = new StringBuilder(attaching.size() * 16);
				for (ShardEntity attach : attaching) {
					attach.getJournal().addEvent(EntityEvent.ATTACH,
							EntityState.PREPARED,
							shard.getShardID(),
							changePlan.getId());
					changePlan.ship(shard, attach);
					logg.append(attach.getEntity().getId()).append(", ");
				}
				Migrator.log.info("{}: Shipping attaches shard: {}, duty: (#{}) {}",
						getClass().getSimpleName(),
						shard.getShardID(),
						attaching.size(),
						logg.toString());
				return true;
			}
		}
		return false;
	}

	public int hashCode() {
		final int prime = 31;
		int res = 1;
		res *= prime + ((shard == null ) ? 0 : shard.hashCode());
		res *= prime + ((entities== null ) ? 0 : entities.hashCode());
		return res;
	}

	@java.lang.Override
	public boolean equals(final Object obj) {
		if (obj == null || !(obj instanceof Override)) {
			return false;
		} else if (obj == this) {
			return true;
		} else {
			final Override o = (Override) obj;
			return o.getShard().equals(shard) && o.getEntities().equals(entities);

		}
	}

	@java.lang.Override
	public String toString() {
		final StringBuilder sb = new StringBuilder("override shard:");
		sb.append(shard).append(", entities:");
		for (ShardEntity se : entities) {
			sb.append(se.toBrief()).append(',');
		}
		return sb.toString();
	}

}