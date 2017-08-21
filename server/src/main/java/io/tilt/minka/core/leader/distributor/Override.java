/**
 *  Copyright (c) 2011-2015 Zauber S.A.  -- All rights reserved
 */

package io.tilt.minka.core.leader.distributor;

import static io.tilt.minka.domain.ShardEntity.State.PREPARED;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import io.tilt.minka.api.Pallet;
import io.tilt.minka.core.leader.PartitionTable;
import io.tilt.minka.domain.EntityEvent;
import io.tilt.minka.domain.Shard;
import io.tilt.minka.domain.ShardEntity;

/**
 * A migration strategy in the override style which replaces a shard's content with given duties.
 * it computes to a delta change considering currently already assigned duties on the target shard.
 */
class Override {
    
	private final Pallet<?> pallet;
	private final Shard shard;
	private final Set<ShardEntity> entities;
	private final double remainingCap;
	
	Override(final Pallet<?> pallet, Shard shard, final Set<ShardEntity> entities, final double remainingCap) {
		super();
		this.pallet = pallet;
		this.shard = shard;
		this.entities = entities;
		this.remainingCap = remainingCap;;
	}
	public Shard getShard() {
		return this.shard;
	}
	public Set<ShardEntity> getEntities() {
		return this.entities;
	}
	public double getRemainingCap() {
		return this.remainingCap;
	}

    boolean compute(final Plan plan, final PartitionTable table) {
        boolean anyChange = false;
        final Set<ShardEntity> current = table.getStage().getDutiesByShard(pallet, getShard());
        if (Migrator.log.isDebugEnabled()) {
            Migrator.log.debug("{}: cluster built {}", getClass().getSimpleName(), getEntities());
            Migrator.log.debug("{}: currents at shard {} ", getClass().getSimpleName(), current);
        }
        anyChange|=dettachDelta(plan, table, getEntities(), getShard(), current);
        anyChange|=attachDelta(plan, table, getEntities(), getShard(), current);
        if (!anyChange) {
            Migrator.log.info("{}: Shard: {}, unchanged", getClass().getSimpleName(), shard);
        }
        return anyChange;
    }
    
	/* dettach anything living in the shard outside what's coming
    * null or empty cluster translates to: dettach all existing */
    private final boolean dettachDelta(
            final Plan plan, final 
            PartitionTable table, 
            final Set<ShardEntity> clusterSet, 
            final Shard shard, 
            final Set<ShardEntity> currents) {
        
        List<ShardEntity> detaching = clusterSet==null ? 
                new ArrayList<>(currents) :
                currents.stream()
                    .filter(i -> !clusterSet.contains(i))
                    .collect(Collectors.toList());
                
        if (!detaching.isEmpty()) {
            StringBuilder logg = new StringBuilder(detaching.size() * 16);
            for (ShardEntity detach : detaching) {
                // copy because in latter cycles this will be assigned
                // so they're traveling different places
                final ShardEntity copy = ShardEntity.Builder.builderFrom(detach).build();
                copy.addEvent(EntityEvent.DETACH, 
                        PREPARED, 
                        shard.getShardID().getStringIdentity(), 
                        plan.getId());
                // testing: con buen soporte de eventg log ahora puedo usar la misma entidad
                // ademas se confirman en serie no en paralelo, no hay problema
                plan.ship(shard, copy);
                logg.append(copy.getEntity().getId()).append(", ");
            }
            Migrator.log.info("{}: Shipping dettaches from: {}, duties: (#{}) {}", getClass().getSimpleName(), shard.getShardID(),
                detaching.size(), logg.toString());
            return true;
        }
        return false;
    }
    
    /* attach what's not already living in that shard */
    private final boolean attachDelta(
            final Plan plan, 
            final PartitionTable table, 
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
                    // copy because in latter cycles this will be assigned
                    // so they're traveling different places
                    final ShardEntity copy = ShardEntity.Builder.builderFrom(attach).build();
                    copy.addEvent(EntityEvent.ATTACH, 
                            PREPARED,
                            shard.getShardID().getStringIdentity(), 
                            plan.getId());
                    plan.ship(shard, copy);
                    logg.append(copy.getEntity().getId()).append(", ");
                }
                Migrator.log.info("{}: Shipping attaches shard: {}, duty: (#{}) {}", getClass().getSimpleName(), shard.getShardID(),
                    attaching.size(), logg.toString());
                return true;
            }
        }
        return false;
    }
}