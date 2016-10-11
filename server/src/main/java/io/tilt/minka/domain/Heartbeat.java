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
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import io.tilt.minka.api.Pallet;
import io.tilt.minka.core.follower.Follower;
import io.tilt.minka.core.leader.Leader;
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

	private List<ShardEntity> duties;
	private Map<Pallet<?>, Double> maxWeights;
	private final NetworkShardID shardId;
	private final DateTime creation;
	private DateTime reception;
	private final boolean warning;
	private final long sequenceId;

	/* only set when change is owned by follower */
	private ShardState stateChange;

	public static Heartbeat create(final List<ShardEntity> entities, final boolean warning, final NetworkShardID id,
			long sequenceId) {
		return new Heartbeat(entities, warning, id, sequenceId);
	}

	private Heartbeat(final List<ShardEntity> duties, final boolean warning, final NetworkShardID id,
			final long sequenceId) {
		this.duties = duties;
		this.warning = warning;
		this.shardId = id;
		this.creation = new DateTime(DateTimeZone.UTC);
		this.sequenceId = sequenceId;
	}

	public static Heartbeat copy(final Heartbeat hb) {
		final List<ShardEntity> cloned = new ArrayList<>();
		for (ShardEntity d : hb.getDuties()) {
			cloned.add(ShardEntity.copy(d));
		}
		return new Heartbeat(cloned, hb.hasWarning(), hb.shardId, hb.sequenceId);
	}

	public void cleanDuties() {
		this.duties = new ArrayList<>(1);
	}

	public DateTime getCreation() {
		return this.creation;
	}

	public long getReceptionDelay() {
		return reception.getMillis() - creation.getMillis();
	}

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

	public List<ShardEntity> getDuties() {
		return this.duties;
	}

	public boolean hasWarning() {
		return warning;
	}

	public long getSequenceId() {
		return this.sequenceId;
	}

	public int hashCode() {
		return new HashCodeBuilder().append(getShardId()).append(getCreation()).append(getSequenceId()).toHashCode();
	}

	public boolean equalsInContent(Heartbeat hb) {
		if (hb == null || !hb.getShardId().equals(getShardId())) {
			return false;
		} else {
			if (hb.getDuties().size() != getDuties().size()) {
				return false;
			} else {
				for (ShardEntity duty : getDuties()) {
					boolean found = false;
					for (ShardEntity other : hb.getDuties()) {
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
				.append(" - Duties: ").append(getDuties().size())
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
}
