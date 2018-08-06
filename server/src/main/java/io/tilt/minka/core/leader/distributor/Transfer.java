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


import io.tilt.minka.core.leader.data.Scheme;
import io.tilt.minka.domain.EntityEvent;
import io.tilt.minka.domain.EntityState;
import io.tilt.minka.domain.ShardEntity;
import io.tilt.minka.shard.Shard;

/**
 * Strategy of Movement.
 * When source and target shards are at hand.
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
	
	Shard getSource() {
		return source;
	}
	
	Shard getTarget() {
		return target;
	}
	
	/* dettach in prev. source, attach to next target */
	boolean apply(final ChangePlan changePlan, final Scheme scheme) {
		final Shard location = scheme.getCommitedState().findDutyLocation(entity);
		if (location != null && location.equals(target)) {
			Migrator.log.info("{}: Transfers mean no change for Duty: {}", getClass().getSimpleName(), toString());
			return false;
		}

		for (Shard shard: new Shard[] {source, target}) {
			if (shard!=null) {
				entity.getCommitTree().addEvent(
						shard==source ? EntityEvent.DETACH : EntityEvent.ATTACH,
						EntityState.PREPARED,
						shard.getShardID(),
						changePlan.getId());
				changePlan.ship(shard, entity);				
			}
		}
		
		if (Migrator.log.isInfoEnabled()) {
			Migrator.log.info("{}: Shipping from: {} to: {}, Duty: {}",
				getClass().getSimpleName(),
				source != null ? source.getShardID() : "[new]",
				target.getShardID(),
				entity.toString());
		}
		return true;
	}

	public int hashCode() {
		final int prime = 31;
		int res = 1;
		res *= prime + ((entity == null ) ? 0 : entity.hashCode());
		res *= prime + ((source == null ) ? 0 : source.hashCode());
		res *= prime + ((target == null ) ? 0 : target.hashCode());
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