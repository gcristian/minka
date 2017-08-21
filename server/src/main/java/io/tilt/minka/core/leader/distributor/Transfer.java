/**
 *  Copyright (c) 2011-2015 Zauber S.A.  -- All rights reserved
 */

package io.tilt.minka.core.leader.distributor;

import static io.tilt.minka.domain.ShardEntity.State.PREPARED;

import io.tilt.minka.core.leader.PartitionTable;
import io.tilt.minka.domain.EntityEvent;
import io.tilt.minka.domain.Shard;
import io.tilt.minka.domain.ShardEntity;

/**
 * A specific duty movement considering localized source and target shards   
 */
class Transfer {
    
	private final Shard source;
	private final Shard target;
	private final ShardEntity entity;
	
	Transfer(final Shard source, final Shard target, final ShardEntity entity) {
		super();
		this.source = source;
		this.target = target;
		this.entity = entity;
	}

	ShardEntity getEntity() {
		return this.entity;
	}
	
	public String toString() {
		return entity.toBrief() + source.toString() + " ==> " + target.toString(); 
	}

    /* dettach in prev. source, attach to next target */
    boolean compute(final Plan plan, final PartitionTable table) {
        final ShardEntity entity = getEntity();
        final Shard location = table.getStage().getDutyLocation(entity);
        if (location!=null && location.equals(target)) {
            Migrator.log.info("{}: Transfers mean no change for Duty: {}", getClass().getSimpleName(), toString());
            return false;
        }
        if (source!=null) {
            getEntity().addEvent(EntityEvent.DETACH, 
                    PREPARED, 
                    location.getShardID().getStringIdentity(), 
                    plan.getId());
            plan.ship(source, entity);
        }
        final ShardEntity assign = ShardEntity.Builder.builderFrom(entity).build();
        assign.addEvent(EntityEvent.ATTACH, 
                PREPARED,
                target.getShardID().getStringIdentity(), 
                plan.getId());
        plan.ship(target, assign);
        
        Migrator.log.info("{}: Shipping transfer from: {} to: {}, Duty: {}", getClass().getSimpleName(),
                source!=null ? source.getShardID() : "[new]", target.getShardID(), assign.toString());
        return true;
    }

}