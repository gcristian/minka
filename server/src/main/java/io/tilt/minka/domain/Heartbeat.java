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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.Validate;
import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

import io.tilt.minka.api.Pallet;
import io.tilt.minka.core.follower.Follower;
import io.tilt.minka.core.leader.Leader;
import io.tilt.minka.domain.ShardCapacity.Capacity;
import io.tilt.minka.spectator.NodeCacheable.Identifiable;

/**
 * A heartbeat is a sign that the node must be considered alive.
 * built by {@link Follower}s holding statistics for allowing the {@link Leader} to balance work.  
 *  
 * @author Cristian Gonzalez
 * @since Nov 5, 2015
 */
public class Heartbeat implements Serializable, Comparable<Heartbeat>, Identifiable {

	private static final long serialVersionUID = 4828220405145911529L;

	private List<ShardEntity> reportedCapturedDuties;
	private Map<Pallet<?>, Capacity> capacities;
	private final NetworkShardIdentifier shardId;
	private final DateTime creation;
	private DateTime reception;
	private final boolean warning;
	private final long sequenceId;
	private final boolean isReportedCapturedDuties;
	private List<DutyDiff> differences;
	
	/* only set when change is owned by follower */
	private ShardState stateChange;

	public static Builder builder(final long sequenceId, final NetworkShardIdentifier shardId) {
		Validate.notNull(shardId);
		return new Builder(sequenceId, shardId);
	}
	
	public static Builder builderFrom(final Heartbeat hb) {
		Validate.notNull(hb);
		final Builder b = new Builder(hb.sequenceId, hb.shardId);
		b.from = hb;
		return b;
	}
	public static class Builder {
		private Heartbeat from;
		private List<ShardEntity> entities;
		private boolean warning;
		private List<DutyDiff> differences;
		
		private boolean reportsCapturedDuties;		
		private final long sequenceId;
		private final DateTime creation;
		private final NetworkShardIdentifier shardId;
		private final Map<Pallet<?>, ShardCapacity.Capacity> capacities = new HashMap<>();

		private Builder(final long sequenceId, final NetworkShardIdentifier shardId) {
			this.shardId = shardId;
			this.sequenceId = sequenceId;
			this.creation = new DateTime(DateTimeZone.UTC);
		}
		public Builder addDifference(final DutyDiff dutyDifference) {
			Validate.notNull(dutyDifference);
			if (this.differences==null) {
				this.differences = new ArrayList<>();
			}
			this.differences.add(dutyDifference);
			return this;
		}
		public Builder addReportedCapturedDuty(final ShardEntity reportedCapturedDuty) {
			Validate.notNull(reportedCapturedDuty);
			if (this.entities ==null) {
				this.entities = new ArrayList<>();
			}
			this.entities.add(reportedCapturedDuty);
			this.reportsCapturedDuties = true;
			return this;
		}
		public Builder addCapacity(final Pallet<?> pallet, Capacity capacity) {
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
			if (from == null) {
				return new Heartbeat(entities, warning, shardId, sequenceId, capacities, 
						reportsCapturedDuties, this.creation, differences);
			} else {
				final List<ShardEntity> cloned = new ArrayList<>();
				for (ShardEntity d : from.getReportedCapturedDuties()) {
					cloned.add(ShardEntity.Builder.builderFrom(d).build());
				}
				return new Heartbeat(cloned, warning, shardId, sequenceId, capacities, 
						reportsCapturedDuties, this.creation, differences);
			}
		}
	}

	public enum AbsenseType {
		REPORTED_UNKNOWN,
		SHARDED_UNREPORTED
		;
	}
	
	private Heartbeat(final List<ShardEntity> duties, final boolean warning, final NetworkShardIdentifier id,
			final long sequenceId, final Map<Pallet<?>, Capacity> capacities, final boolean includesDuties, 
			final DateTime creation, List<DutyDiff> differences) {
		this.reportedCapturedDuties = duties;
		this.warning = warning;
		this.shardId = id;
		this.creation = creation;
		this.sequenceId = sequenceId;
		this.capacities = capacities;
		this.isReportedCapturedDuties = includesDuties;
		this.differences = differences;
	}

	@JsonProperty(index=9, value="reports-duties")
	public boolean isReportedCapturedDuties() {
		return this.isReportedCapturedDuties;
	}
	
	@JsonProperty(index=2, value="has-differences")
	public boolean hasDifferences() {
		return this.differences!=null;
	}
	@JsonIgnore
	public List<DutyDiff> getDifferences() {
		return this.differences;
	}

	public void cleanDuties() {
		this.reportedCapturedDuties = new ArrayList<>(1);
	}
	
	@JsonIgnore
	public DateTime getCreation() {
        return this.creation;
    }
	@JsonProperty(index=3, value="creation")
	private String getCreation_() {
		return this.creation.toString();
	}

	@JsonProperty(index=4, value="reception-delay")
	public long getReceptionDelay() {
		return reception.getMillis() - creation.getMillis();
	}
	
	@JsonIgnore
	public DateTime getReception() {
        return this.reception;
    }
	
	@JsonProperty(index=5, value="reception")
	private String getReception_() {
		return this.reception.toString();
	}

	public void setReception(DateTime reception) {
		this.reception = reception;
	}

	@JsonIgnore
	public NetworkShardIdentifier getShardId() {
		return this.shardId;
	}

	@JsonProperty(index=6, value="state-change")
	public ShardState getStateChange() {
		return this.stateChange;
	}

	public void setStateChange(ShardState stateChange) {
		this.stateChange = stateChange;
	}

	@JsonIgnore
	public List<ShardEntity> getReportedCapturedDuties() {
		return this.reportedCapturedDuties;
	}

	@JsonProperty(index=7, value="reported-duties")
	private int getDutySize() {
        return this.reportedCapturedDuties!=null ? this.reportedCapturedDuties.size() : 0;
    }

	@JsonProperty(index=8, value="has-warning")
	public boolean hasWarning() {
		return warning;
	}

	@JsonProperty(index=1, value="sequence-id")
	public long getSequenceId() {
		return this.sequenceId;
	}

	public int hashCode() {
		return new HashCodeBuilder()
				.append(getShardId())
				.append(getCreation())
				.append(getSequenceId())
				.toHashCode();
	}

	public boolean equalsInContent(Heartbeat hb) {
		if (hb == null || !hb.getShardId().equals(getShardId())) {
			return false;
		} else {
			if (hb.getReportedCapturedDuties().size() != getReportedCapturedDuties().size()) {
				return false;
			} else {
				for (ShardEntity duty : getReportedCapturedDuties()) {
					boolean found = false;
					for (ShardEntity other : hb.getReportedCapturedDuties()) {
						found |= duty.equals(other) && duty.getState() == other.getState()
								&& duty.getDutyEvent() == other.getDutyEvent();
						if (found) {
							break;
						}
					}
					if (!found) {
						return false;
					}
				}
			}
		}
		return true;
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
		return new StringBuilder()
				.append(" Sequence: ").append(sequenceId)
				.append(" - Created: ").append(getCreation())
				.append(" - ShardID: ").append(getShardId())
				.append(" - Duties: ").append(isReportedCapturedDuties() ? 
						getReportedCapturedDuties().size() : "<single>")
				.toString();
	}

	@Override
	public int compareTo(Heartbeat o) {
		return o.getCreation().compareTo(getCreation());
	}

	@Override
	public String getId() {
		return this.shardId.getStringIdentity();
	}
	
	@JsonIgnore
	public Map<Pallet<?>, Capacity> getCapacities() {
		return this.capacities;
	}
}
