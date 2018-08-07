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
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;

import io.tilt.minka.domain.EntityEvent;
import io.tilt.minka.domain.CommitTree.Log;
import io.tilt.minka.shard.Shard;
import io.tilt.minka.domain.EntityState;
import io.tilt.minka.domain.ShardEntity;
import io.tilt.minka.utils.CollectionUtils;

/**
 * A single {@linkplain EntityEvent} over many {@linkplain ShardEntity} 
 * intended on a single {@linkplain Shard}
 * Isolated from other deliveries. Created by the {@linkplain ChangePlan} 
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
	@JsonIgnore
	private boolean sent;
	
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
	@JsonIgnore
	int getOrder() {
		return this.order;
	}
	
	void markSent() {
		this.sent = true;
	}

	@JsonIgnore
	public
	List<ShardEntity> getDuties() {
		return this.duties;
	}
	@JsonIgnore
	Shard getShard() {
		return this.shard;
	}
	@JsonIgnore
	EntityEvent getEvent() {
		return event;
	}
	@JsonIgnore
	long getPlanId() {
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
	
	int contentsByState(final BiConsumer<ShardEntity, Log> bicons) {
		return contentsByState_(null, bicons);
	}
	
	int contentsByState(final EntityState state, final BiConsumer<ShardEntity, Log> bicons) {
		return contentsByState_(state, bicons);
	}
	
	private int contentsByState_(final EntityState state, final BiConsumer<ShardEntity, Log> bicons) {
		int count = 0;
		for (ShardEntity duty : duties) {
			for (Log log : duty.getCommitTree().filterLogs(getPlanId(), shard.getShardID().getId(), getEvent())) {
				if (state == null || log.getLastState() == state) {
					bicons.accept(duty, log);
					count++;
					break;
				}
			}
		}
		return count;
	}
	
	public Step getStep() {
		return this.step;
	}
	
	/* recalculates state: DONE when all duties COMMITED */
	public void calculateState(final Consumer<String> c) {
		if (step == Step.ENQUEUED) {
			if (duties.isEmpty()) {
				throw new IllegalStateException("delivery without duties cannot go to pending !");
			} else if (sent) {
				step = Step.PENDING;
			}
		} else if (step == Step.PENDING) {
			// for all duties grouped together with same Event to the same Shard..
			boolean noneLeft = true;
			for (final ShardEntity duty : duties) {
				// look up confirmation for the specific logged event matching this delivery
				final Log found = duty.getCommitTree().findOne(getPlanId(), shard.getShardID(), getEvent());
				if (found != null && found.getLastState() != EntityState.COMMITED) {
					noneLeft = false;
					if (c!=null) {
						c.accept(String.format("%s: waiting Shard %s for %s still %s for %s",
								getClass().getSimpleName(),
								found.getTargetId(),
								duty.getEntity().getId(),
								found.getLastState(),
								found.getEvent()));
					}
					return;
				}
			}
			if (noneLeft) {
				this.step = Step.DONE;
			}
		}
	}
		
	@java.lang.Override
	public String toString() {
		return new StringBuilder()
				.append(event)
				.append("-")
				.append(shard)
				.append("-")
				.append(duties.size())
				.toString();
	}
	
	@JsonProperty("event")
	private String getEventShard() {
		return new StringBuilder()
				.append(event).append(' ')
				.append(shard.toString())
				.toString();
	}

	@JsonProperty("state")
	/* only for serialization */
	Map<EntityState, StringBuilder> getState() {
		final Map<EntityState, StringBuilder> ret = new HashMap<>(3); // any of given prepared, pending, confirmed
		for (final ShardEntity duty: duties) {
			ChangePlan.getOrPut(ret, 
					duty.getCommitTree().findOne(this.planId, this.shard.getShardID(), this.event).getLastState(), 
					() -> new StringBuilder())
					.append(duty.getDuty().getId())
					.append(", ");
		}
		
		return ret;
	}

}