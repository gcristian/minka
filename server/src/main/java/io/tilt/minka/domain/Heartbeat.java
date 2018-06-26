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
package io.tilt.minka.domain;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.Validate;
import org.apache.commons.lang.builder.EqualsBuilder;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

import io.tilt.minka.api.Pallet;
import io.tilt.minka.core.follower.Follower;
import io.tilt.minka.core.leader.Leader;

/**
 * A heartbeat is a sign that the node must be considered alive.
 * built by {@link Follower}s holding statistics for allowing the {@link Leader} to balance work.  
 *  
 * @author Cristian Gonzalez
 * @since Nov 5, 2015
 */
public class Heartbeat implements Serializable, Comparable<Heartbeat> {

	private static final long serialVersionUID = 4828220405145911529L;

	private List<EntityRecord> reportedCapturedDuties;
	private Map<Pallet, Capacity> capacities;
	private final NetworkShardIdentifier shardId;
	private final DateTime creation;
	private DateTime reception;
	@JsonIgnore
	private final boolean warning;
	private final long sequenceId;
	private final boolean reportsDuties;
	
	/* only set when change is owned by follower */
	private Shard.Change stateChange;

	public static Builder builder(final long sequenceId, final NetworkShardIdentifier shardId) {
		Validate.notNull(shardId);
		return new Builder(sequenceId, shardId);
	}

	public static class Builder {
		private List<EntityRecord> entities;
		private boolean warning;
		
		private boolean reportsCapturedDuties;		
		private final long sequenceId;
		private final DateTime creation;
		private final NetworkShardIdentifier shardId;
		private final Map<Pallet, Capacity> capacities = new HashMap<>();

		private Builder(final long sequenceId, final NetworkShardIdentifier shardId) {
			this.shardId = shardId;
			this.sequenceId = sequenceId;
			this.creation = new DateTime(DateTimeZone.UTC);
		}
		public Builder addReportedCapturedDuty(final EntityRecord duty) {
			Validate.notNull(duty);
			if (this.entities ==null) {
				this.entities = new ArrayList<>();
			}
			this.entities.add(duty);
			return this;
		}
		public Builder reportsDuties() {
			this.reportsCapturedDuties = true;
			return this;
		}
		public Builder addCapacity(final Pallet pallet, final Capacity capacity) {
			Validate.notNull(pallet);
			Validate.notNull(capacity);
			this.capacities.put(pallet, capacity); 
			return this;
		}
		public Builder withWarning() {
			this.warning = true;
			return this;
		}
		public Heartbeat build() {
			return new Heartbeat(entities, warning, shardId, sequenceId, capacities, 
					reportsCapturedDuties, this.creation);
		}
	}

	public enum AbsenseType {
		REPORTED_UNKNOWN,
		SHARDED_UNREPORTED
		;
	}
	
	private Heartbeat(
			final List<EntityRecord> duties, 
			final boolean warning, 
			final NetworkShardIdentifier id,
			final long sequenceId, 
			final Map<Pallet, Capacity> capacities, 
			final boolean reportsDuties, 
			final DateTime creation) {
		this.reportedCapturedDuties = duties == null ? Collections.emptyList() : duties;
		this.warning = warning;
		this.shardId = id;
		this.creation = creation;
		this.sequenceId = sequenceId;
		this.capacities = capacities;
		this.reportsDuties = reportsDuties;
	}

	@JsonProperty(index=7, value="reports-duties")
	/** @return if reported duties content is able to be analyzed */
	public boolean reportsDuties(){
		return this.reportsDuties;
	}
	
	@JsonIgnore
	public DateTime getCreation() {
		return this.creation;
	}

	@JsonProperty(index=1, value="creation")
	private String getCreation_() {
		return this.creation.toString();
	}

	@JsonProperty(index=3, value="reception-delay")
	public long getReceptionDelay() {
		return reception.getMillis() - creation.getMillis();
	}
	
	@JsonIgnore
	public DateTime getReception() {
		return this.reception;
	}
	
	public void setReception(DateTime reception) {
		this.reception = reception;
	}

	@JsonIgnore
	public NetworkShardIdentifier getShardId() {
		return this.shardId;
	}

	@JsonProperty(index=8, value="state-change")
	public Shard.Change getShardChange() {
		return this.stateChange;
	}

	public void setShardChange(final Shard.Change change) {
		this.stateChange = change;
	}

	@JsonIgnore
	public List<EntityRecord> getReportedCapturedDuties() {
		return this.reportedCapturedDuties;
	}
	
	/** dereferences inner collections */
	public void clear() {
		this.reportedCapturedDuties.clear();
		this.capacities.clear();
	}

	@JsonProperty(index=5, value="reported-duties")
	private int getDutySize() {
        return this.reportedCapturedDuties!=null ? this.reportedCapturedDuties.size() : 0;
    }

	@JsonProperty(index=4, value="has-warning")
	public boolean hasWarning() {
		return warning;
	}

	@JsonProperty(index=2, value="sequence-id")
	public long getSequenceId() {
		return this.sequenceId;
	}

	public int hashCode() {
		final int prime = 31;
		int res = 1;
		res *= prime + ((shardId == null ) ? 0 : shardId.hashCode());
		res *= prime + ((creation== null ) ? 0 : creation.hashCode());
		res *= prime + sequenceId;
		return res;
	}

	@Override
	public boolean equals(Object obj) {
		if (obj instanceof Heartbeat) {
			Heartbeat other = (Heartbeat) obj;
			return new EqualsBuilder()
					.append(getShardId(), other.getShardId())
					.append(getCreation(), other.getCreation())
					.append(getSequenceId(), other.getSequenceId())
					.isEquals();
		} else {
			return false;
		}
	}

	@Override
	public String toString() {
		final StringBuilder sb = new StringBuilder()
			.append(" Sequence: ").append(sequenceId)
			.append(" - Created: ").append(getCreation())
			.append(" - ShardID: ").append(getShardId())
			;
		if (getShardChange()!=null) {
			sb.append(" - w/StateChange ");
		}
		if (hasWarning()) {
			sb.append(" - w/Warn ");
		}
		sb.append(" - Duties: ")
			.append(reportsDuties() ? getReportedCapturedDuties().size() : "<single>")
		;
		return sb.toString();
	}

	@Override
	public int compareTo(Heartbeat o) {
		return getCreation().compareTo(o.getCreation());
	}

	@JsonIgnore
	public Map<Pallet, Capacity> getCapacities() {
		return this.capacities;
	}
}
