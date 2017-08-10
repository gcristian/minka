/**
 *  Copyright (c) 2011-2015 Zauber S.A.  -- All rights reserved
 */

package io.tilt.minka.core.leader.distributor;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;

import io.tilt.minka.domain.EntityEvent;
import io.tilt.minka.domain.Shard;
import io.tilt.minka.domain.ShardEntity;
import io.tilt.minka.domain.ShardEntity.State;

/* plainer of the work map  */
@JsonPropertyOrder({"order", "shard", "event", "state"})
public class Delivery {
    
	private EntityEvent event;
	private final Shard shard;
	private final List<ShardEntity> duties;
	private final int order;
	
	public Delivery(final List<ShardEntity> duties, final Shard shard, final EntityEvent event, final int order) {
		super();
		this.duties = duties;
		this.shard = shard;
		this.event = event;
		this.order = order;
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

	@JsonProperty("state")
	/* only for serialization */
	final Map<ShardEntity.State, StringBuilder> getState() {
		final Map<ShardEntity.State, StringBuilder> ret = new HashMap<>();
		duties.forEach(duty-> Plan.getOrPut(ret, duty.getState(), ()->new StringBuilder())
				.append(duty.getDuty().getId()).append(", "));
		return ret;
	}
	public boolean allConfirmed() {
		final Set<ShardEntity> sortedLog = new TreeSet<>();
		for (final ShardEntity duty : duties) {
			if (duty.getState() != State.CONFIRMED) {
				// TODO get Partition TAble and check if Shard has long fell offline
				sortedLog.add(duty);
				Plan.logger.info("{}: waiting Shard: {} for at least Duties: {}", getClass().getSimpleName(), shard,
						ShardEntity.toStringIds(sortedLog));
				return false;
			}
		}
		return true;
	}
	@Override
	public String toString() {
	    return event + "-" + shard + "-" + duties.size();
	}
}