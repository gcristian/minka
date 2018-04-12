/*
 * Licensed to the Apache S oftware Foundation (ASF) under one or more
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

import static java.util.Objects.requireNonNull;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;

import io.tilt.minka.domain.EntityEvent;
import io.tilt.minka.domain.EntityJournal.Log;
import io.tilt.minka.domain.Shard;
import io.tilt.minka.domain.ShardEntity;
import io.tilt.minka.domain.EntityState;
import io.tilt.minka.utils.CollectionUtils;

/**
 * A single {@linkplain EntityEvent} over many {@linkplain ShardEntity} 
 * intended on a single {@linkplain Shard}
 * Isolated from other deliveries. Created by the {@linkplain Plan} 
 * Coordinated by the {@linkplain Distributor}
 * @author Cristian Gonzalez
 * @since Aug 11, 2017
 */
@JsonPropertyOrder({"order", "shard", "event", "state"})
public class Delivery {
    
	private final List<ShardEntity> duties;
	private final Shard shard;
	private final EntityEvent event;
	private final int order;
	private final long planId;

	private Step step;

	protected Delivery(
	        final List<ShardEntity> duties, 
	        final Shard shard, 
	        final EntityEvent event, 
	        final int order, 
	        final long planId) {
		super();
		this.duties = requireNonNull(duties);
		this.shard = requireNonNull(shard);
		this.event = requireNonNull(event);
		this.order = order;
		this.planId = planId;
		this.step = Step.ENQUEUED;
	}
	public int getOrder() {
        return this.order;
    }
	@JsonIgnore
	public List<ShardEntity> getDuties() {
		return this.duties;
	}
	@JsonIgnore
	public Shard getShard() {
		return this.shard;
	}
	public EntityEvent getEvent() {
		return event;
	}
	public long getPlanId() {
		return this.planId;
	}
	
	public static enum Step {
	    // waiting for it's turn to be delivered
	    ENQUEUED,
	    // sent, waiting for shard confirmation
	    PENDING,
	    // delivered 
	    DONE,
	}
	
	protected Map<ShardEntity, Log> getByState() {
		return getByState_(null);
	}
	
	protected Map<ShardEntity, Log> getByState(final EntityState state) {
		return getByState_(state);
	}
	
	private Map<ShardEntity, Log> getByState_(final EntityState state) {
		final Map<ShardEntity, Log> ret = new HashMap<>(duties.size());
	    for (ShardEntity duty: duties) {
	    	for (Log log: duty.getJournal().getLogs()) {
	    		if (log.matches(getEvent(), shard.getShardID().getId(), getPlanId())
	    				&& (state == null || log.getLastState()==state)) {
	    			ret.put(duty, log);
	    			break;
	    		}
	    	}
	    }
	    return ret;
	}
	
	public Step getStep() {
		return this.step;
	}
	
	public void checkState() {
	    if (step==Step.ENQUEUED) {
	        if (duties.isEmpty()) {
	            throw new IllegalStateException("delivery without duties cannot go to pending !");
	        } else {
	            step = Step.PENDING;
	        }
	    } else if (step==Step.PENDING){
        	// for all duties grouped together with same Event to the same Shard..
            boolean noneLeft = true;
    		for (final ShardEntity duty : duties) {
    			// look up confirmation for the specific logged event matching this delivery
    			final Log found = duty.getJournal().find(getPlanId(), shard.getShardID(), getEvent());
    			if (found!=null) {
    				if (found.getLastState()!=EntityState.CONFIRMED) {
					    noneLeft = false;
					    // TODO get Partition TAble and check if Shard has long fell offline
						if (Plan.logger.isInfoEnabled()) {
							Plan.logger.info("{}: waiting Shard: {} for {} still in {}, at least Duty: {}", 
                                getClass().getSimpleName(), shard, duty.getEntity().getId(), 
                                found.getEvent(), found.getLastState());
						}
						return;
					}
    			}
    		}
    		if (noneLeft) {
    		    this.step = Step.DONE;
    		}
	    }
	}
		
	@java.lang.Override
	public String toString() {
	    return event + "-" + shard + "-" + duties.size();
	}
	
	@JsonProperty("shard")
	private String getShard_() {
	    return shard.toString();
	}

	@JsonProperty("state")
	/* only for serialization */
	final Map<EntityState, StringBuilder> getState() {
		final Map<EntityState, StringBuilder> ret = new HashMap<>();
		for (final ShardEntity duty: duties) {
		    CollectionUtils.getOrPut(ret, duty.getLastState(), ()->new StringBuilder())
		        .append(duty.getDuty().getId()).append(", ");
		}
		
		return ret;
	}

}