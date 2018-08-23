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

import static io.tilt.minka.core.leader.distributor.Migrator.log;

import java.util.Set;

import io.tilt.minka.api.Pallet;
import io.tilt.minka.core.leader.data.Scheme;
import io.tilt.minka.domain.EntityEvent;
import io.tilt.minka.domain.EntityState;
import io.tilt.minka.domain.ShardEntity;
import io.tilt.minka.shard.Shard;

/**
 * Strategy of Replacement.
 * A migration strategy in the override style which replaces a shard's content with given duties.
 */
public class Override {

	private final Pallet pallet;
	private final Shard shard;
	private final Set<ShardEntity> entities;
	private final double remainingCap;
	
	Override(final Pallet pallet, final Shard shard, final Set<ShardEntity> entities, final double remainingCap) {
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

	boolean apply(final ChangePlan changePlan, final Scheme scheme) {
		boolean anyChange = false;
		
		if (log.isDebugEnabled()) {
			log.debug("{}: cluster built {}", getClass().getSimpleName(), entities);
			final StringBuilder tmp = new StringBuilder();
			scheme.getCommitedState().findDuties(getShard(), pallet, d->tmp.append(d).append(", "));
			log.debug("{}: currents at shard {} ", getClass().getSimpleName(), tmp);
		}
		anyChange |= dettachDelta(changePlan, scheme);
		anyChange |= attachDelta(changePlan, scheme);
		if (!anyChange && log.isInfoEnabled()) {
			log.info("{}: Shard: {}, unchanged", getClass().getSimpleName(), shard);
		}
		return anyChange;
	}
	 
		/* dettach anything living in the shard outside what's coming
	    * null or empty cluster translates to: dettach all existing */
	private final boolean dettachDelta(
			final ChangePlan changePlan, 
			final Scheme scheme) {

		final StringBuilder logg = new StringBuilder(16*10);
		final int[] count = new int[1];
		scheme.getCommitedState().findDuties(getShard(), pallet, detach-> {
			if (entities == null || !entities.contains(detach)) {
				detach.getCommitTree().addEvent(EntityEvent.DETACH,
						EntityState.PREPARED,
						shard.getShardID(),
						changePlan.getId());
				changePlan.dispatch(shard, detach);
				logg.append(detach.getEntity().getId()).append(", ");
				count[0]++;
			}
		});
		
		if (count[0]>0 && log.isInfoEnabled()) {
			log.info("{}: Shard: {} shipping {} (#{}) duties: {}",
				getClass().getSimpleName(), shard.getShardID(), EntityEvent.DETACH, count[0], logg.toString());
		}
		return count[0]>0;
	}

	/* attach what's not already living in that shard */
	private final boolean attachDelta(
			final ChangePlan changePlan, 
			final Scheme scheme) {

		final StringBuilder logg = new StringBuilder(10 * 16); 
		int count = 0;
		if (entities != null) {
			for (final ShardEntity attach: entities) {
				if (!scheme.getCommitedState().dutyExistsAt(attach, getShard())) {
					count++;
					attach.getCommitTree().addEvent(EntityEvent.ATTACH,
							EntityState.PREPARED,
							shard.getShardID(),
							changePlan.getId());
					changePlan.dispatch(shard, attach);
					logg.append(attach.getEntity().getId()).append(", ");
				}
			}
		}
		if (count>0 && log.isInfoEnabled()) {
			Migrator.log.info("{}: Shard: {} shipping {} (#{}) duties: {}",
				getClass().getSimpleName(), shard.getShardID(), EntityEvent.ATTACH, count, logg.toString());
		}
		return count>0;
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
			return o.getShard().equals(shard) 
				&& o.getEntities().equals(entities);

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