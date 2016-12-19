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

import io.tilt.minka.api.Duty;
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
	private final NetworkShardID shardId;
	private final DateTime creation;
	private DateTime reception;
	private final boolean warning;
	private final long sequenceId;
	private final boolean isReportedCapturedDuties;
	private List<DutyDiff> differences;
	
	/* only set when change is owned by follower */
	private ShardState stateChange;

	
	public static class Builder {
		private Heartbeat from;
		private List<ShardEntity> entities;
		private boolean warning;
		private List<DutyDiff> differences;
		
		private boolean reportsCapturedDuties;		
		private final long sequenceId;
		private final DateTime creation;
		private final NetworkShardID shardId;
		private final Map<Pallet<?>, ShardCapacity.Capacity> capacities = new HashMap<>();

		private Builder(final long sequenceId, final NetworkShardID shardId) {
			this.shardId = shardId;
			this.sequenceId = sequenceId;
			this.creation = new DateTime(DateTimeZone.UTC);
		}
		public static Builder builder(final long sequenceId, final NetworkShardID shardId) {
			Validate.notNull(shardId);
			final Builder b = new Builder(sequenceId, shardId);
			return b;
		}
		public static Builder builderFrom(final Heartbeat hb) {
			Validate.notNull(hb);
			final Builder b = new Builder(hb.sequenceId, hb.shardId);
			b.from = hb;
			return b;
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
	
	public static class DutyDiff {
		
		private static final String DUTY2 = "-duty2";
		private static final String DUTY1 = "-duty1";
		private static final String HASH_CODES = "hash-codes";
		private static final String PALLET_PAYLOAD = "pallet-payload";
		private static final String PAYLOAD = "payload";
		private static final String CLASS_TYPES = "class-types";
		private static final String DIFF = "<!=>";
		private static final String PALLET_ID = "palletId";
		private static final String DUTY = "duty";
		private static final String NULL = "null";
		private static final String NOTNULL = "notnull";
		private static final String DUTY12 = "duty1";
		
		private final Duty<?> duty1;
		private final Duty<?> duty2;
		private Map<String, String> diffs;
		
		public DutyDiff(final Duty<?> d1, final Duty<?> d2) {
			super();
			Validate.notNull(d2);
			Validate.notNull(d1);
			this.duty1 = d1;
			this.duty2 = d2;
		}
		
		public Map<String, String> getDiff() {
			return diffs;
		}
		
		public boolean hasDiffs() {
			boolean ret = false;
			if (duty1==null || duty2==null) {
				init();
				diffs.put(DUTY12, (duty1 == null ? NULL : NOTNULL));
				diffs.put(DUTY12, (duty1 == null ? NULL : NOTNULL));
				ret = true;
			} else {
				ret |=! hashAndMethod(duty1, duty2, DUTY);
				try {
					ret |=! duty1.getClassType().equals(duty2.getClassType());
					if (!ret && init()) {
						diffs.put(CLASS_TYPES, duty1.getClassType() + DIFF + duty2.getClassType());
					}
				} catch (Exception e) {
					init();
					diffs.put(CLASS_TYPES, "duty1.getClassType().equals() fails:" + e.getMessage());
				}
				ret |=! hashAndMethod(duty1.get(), duty2.get(), PAYLOAD);
				ret |=! duty1.getPalletId().equals(duty2.getPalletId());
				if (!ret && init()) {
					diffs.put(PALLET_ID, duty1.getPalletId() + DIFF + duty2.getPalletId());
				}
				ret |=! hashAndMethod(duty1.get(), duty2.get(), PALLET_PAYLOAD);
			}
			return ret;
		}
		private boolean hashAndMethod(final Object o1 , final Object o2, final String prefix ) {
			boolean ret = true;
			if (o1!=null && o2!=null) {
				ret &= o1.hashCode()==o2.hashCode();
				if (!ret && init()) {
					diffs.put(prefix + HASH_CODES, o1.hashCode() + DIFF + o2.hashCode());
				}
				try {
					ret &= o1.equals(o2);
					if (!ret && init()) {
						diffs.put(prefix, "equals() fails");
					}
				} catch (Exception e) {
					init();
					diffs.put(prefix, "duty1.get().equals() fails:" + e.getMessage());
				}
			} else {
				init();
				diffs.put(prefix + DUTY1, o1==null ? NULL : NOTNULL);
				diffs.put(prefix + DUTY2, o2==null ? NULL : NOTNULL);
			}
			return ret;
		}
		private boolean init() {
			if (diffs==null) {
				diffs = new HashMap<>();
			}
			return true;
		}
	}

	private Heartbeat(final List<ShardEntity> duties, final boolean warning, final NetworkShardID id,
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

	public boolean isReportedCapturedDuties() {
		return this.isReportedCapturedDuties;
	}
	
	public boolean hasDifferences() {
		return this.differences!=null;
	}
	
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

	public NetworkShardID getShardId() {
		return this.shardId;
	}

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

	public boolean hasWarning() {
		return warning;
	}

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
	
	public Map<Pallet<?>, Capacity> getCapacities() {
		return this.capacities;
	}
}
