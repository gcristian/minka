/**
 *  Copyright (c) 2011-2015 Zauber S.A.  -- All rights reserved
 */

package io.tilt.minka.core.leader.distributor;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;

import io.tilt.minka.domain.EntityEvent;
import io.tilt.minka.domain.Shard;
import io.tilt.minka.domain.ShardEntity;
import io.tilt.minka.domain.ShardEntity.State;

/**
 * A single Event over many Entities intended on a single Shard.
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
	private Status status;

	public Delivery(final List<ShardEntity> duties, final Shard shard, final EntityEvent event, final int order) {
		super();
		this.duties = duties;
		this.shard = shard;
		this.event = event;
		this.order = order;
		this.status = Status.ENQUEUED;
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
	public String getShard_() {
	    return shard.toString();
	}
	public EntityEvent getEvent() {
		return event;
	}

	public void markPending() {
	    this.status = Status.PENDING;
	}
	
	@JsonProperty("state")
	/* only for serialization */
	final Map<ShardEntity.State, StringBuilder> getState() {
		final Map<ShardEntity.State, StringBuilder> ret = new HashMap<>();
		duties.forEach(duty-> Plan.getOrPut(ret, duty.getState(), ()->new StringBuilder())
				.append(duty.getDuty().getId()).append(", "));
		return ret;
	}
	
	public static enum Status {
	    // waiting for it's turn to be delivered
	    ENQUEUED,
	    // sent, waiting for shard confirmation
	    PENDING,
	    // delivered 
	    CONFIRMED,
	}
	
	public List<ShardEntity> getByState(final ShardEntity.State state) {
	    return state == null ? duties :
	        duties.stream()
	            .filter(d->d.getState()==state)
	            .collect(Collectors.toList());
	}
	
	public Status checkStatus() {
	    if (status == Status.PENDING) {
    		for (final ShardEntity duty : duties) {
    			if (duty.getState() != State.CONFIRMED) {
    				// TODO get Partition TAble and check if Shard has long fell offline
    				Plan.logger.info("{}: waiting Shard: {} for at least Duties: {}", getClass().getSimpleName(), shard,
    						duty);
    				return status;
    			}
    		}
    		this.status = Status.CONFIRMED;
	    }
	    return this.status;
	}
	@java.lang.Override
	public String toString() {
	    return event + "-" + shard + "-" + duties.size();
	}
}