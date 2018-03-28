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


import io.tilt.minka.core.leader.PartitionTable;
import io.tilt.minka.domain.EntityEvent;
import io.tilt.minka.domain.EntityState;
import io.tilt.minka.domain.Shard;
import io.tilt.minka.domain.ShardEntity;

/**
 * A specific duty movement considering localized source and target shards   
 */
class Transfer {
    
	private final Shard source;
	private final Shard target;
	private final ShardEntity entity;
	
	protected Transfer(final Shard source, final Shard target, final ShardEntity entity) {
		super();
		this.source = source;
		this.target = target;
		this.entity = entity;
	}

	ShardEntity getEntity() {
		return this.entity;
	}
	
	Shard getSource() {
		return source;
	}
	
	Shard getTarget() {
		return target;
	}
	
    /* dettach in prev. source, attach to next target */
    boolean apply(final Plan plan, final PartitionTable table) {
        final ShardEntity entity = getEntity();
        final Shard location = table.getStage().getDutyLocation(entity);
        if (location!=null && location.equals(target)) {
            Migrator.log.info("{}: Transfers mean no change for Duty: {}", getClass().getSimpleName(), toString());
            return false;
        }
        if (source!=null) {
            getEntity().getJournal().addEvent(EntityEvent.DETACH, 
                    EntityState.PREPARED, 
                    location.getShardID(), 
                    plan.getId());
            plan.ship(source, entity);
        }
        
        // TODO this's not longer neccesary: previously there was not a LogList object  
        
        final ShardEntity assign = ShardEntity.Builder.builderFrom(entity).build();
        assign.getJournal().addEvent(EntityEvent.ATTACH, 
                EntityState.PREPARED,
                target.getShardID(), 
                plan.getId());
        plan.ship(target, assign);
        
        Migrator.log.info("{}: Shipping transfer from: {} to: {}, Duty: {}", getClass().getSimpleName(),
                source!=null ? source.getShardID() : "[new]", target.getShardID(), assign.toString());
        return true;
    }
    

	public int hashCode() {
		final int prime = 31;
		int res = 1;
		for (final Object o: new Object[] {source, target, entity}) {
			res *= prime + ((o == null ) ? 0 : o.hashCode());
		}
		return res;
	}

    @java.lang.Override
    public boolean equals(final Object obj) {
    	if (obj==null || !(obj instanceof Transfer)) {
    		return false;
    	} else if (obj == this) {
    		return true;
    	} else {
    		final Transfer o = (Transfer)obj;
    		return o.getEntity().equals(entity)
    				&& o.getSource().equals(source)
    				&& o.getTarget().equals(target);
    		
    	}
    }
    
    @java.lang.Override
    public String toString() {
    	final StringBuilder sb = new StringBuilder("");
    	sb.append("transfer from:").append(source);
    	sb.append(", to:").append(target);
    	sb.append(", entity:").append(entity);
    	return sb.toString();
    }

    

}