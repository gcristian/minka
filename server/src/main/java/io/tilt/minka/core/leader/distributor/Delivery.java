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
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;

import io.tilt.minka.domain.EntityEvent;
import io.tilt.minka.domain.EventTrack.Track;
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
    
	private EntityEvent event;
	private final Shard shard;
	private final List<ShardEntity> duties;
	private final int order;
	private Step step;
	private final long planId;

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
	@JsonProperty("shard")
	private String getShard_() {
	    return shard.toString();
	}
	public EntityEvent getEvent() {
		return event;
	}
	public long getPlanId() {
		return this.planId;
	}

	@JsonProperty("state")
	/* only for serialization */
	final Map<EntityState, StringBuilder> getState() {
		final Map<EntityState, StringBuilder> ret = new HashMap<>();
		duties.forEach(duty-> CollectionUtils.getOrPut(ret, duty.getLastState(), ()->new StringBuilder())
		        .append(duty.getDuty().getId()).append(", "));
		return ret;
	}
	
	public static enum Step {
	    // waiting for it's turn to be delivered
	    ENQUEUED,
	    // sent, waiting for shard confirmation
	    PENDING,
	    // delivered 
	    DONE,
	}
	
	protected Map<ShardEntity, Track> getByState() {
		return getByState_(null);
	}
	
	protected Map<ShardEntity, Track> getByState(final EntityState state) {
		return getByState_(state);
	}
	
	private Map<ShardEntity, Track> getByState_(final EntityState state) {
		final Map<ShardEntity, Track> ret = new HashMap<>(duties.size());
	    for (ShardEntity duty: duties) {
	    	for (Track track: duty.getEventTrack().getTracks()) {
	    		if (track.getEvent()==getEvent()
	    				&& track.getPlanId()==getPlanId()
	    				&& track.getTargetId().equals(shard.getShardID().getStringIdentity())
	    				&& (state == null || track.getLastState()==state)) {
	    			ret.put(duty, track);
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
            int pending = duties.size();
    		for (final ShardEntity duty : duties) {
    			// look up confirmation for the specific logged event matching this delivery
    			for (Iterator<Track> elit = duty.getEventTrack().getDescendingIterator(); elit.hasNext();) {
    				final Track log = elit.next();
    				if (log.getTargetId().isEmpty()) {
    				    throw new IllegalStateException("log target id is invalid (empty)");
    				}
    				if (log.getTargetId().equals(shard.getShardID().getStringIdentity())
    						&& log.getEvent() == getEvent()
        					&& log.getPlanId() == getPlanId()) {
    					if (log.getLastState()==EntityState.CONFIRMED) {
    					    pending--;
    					} else {
    					    // TODO get Partition TAble and check if Shard has long fell offline
                            Plan.logger.info("{}: waiting Shard: {} for at least Duty: {} for: {} still in: {}", 
                                    getClass().getSimpleName(), shard, duty, log.getEvent(), log.getLastState());
                            if (log.getLastState()==EntityState.PENDING) {
                                return;
                            }
    					}
                        break;
    				} else if (getPlanId() > log.getPlanId()) {
    					// avoid phantom events
    					break;
    				}
    			}
    		}
    		if (pending==0) {
    		    this.step = Step.DONE;
    		}
	    }
	}
		
	@java.lang.Override
	public String toString() {
	    return event + "-" + shard + "-" + duties.size();
	}
}