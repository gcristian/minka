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
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.builder.HashCodeBuilder;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnore;

import io.tilt.minka.api.Pallet;
import io.tilt.minka.broker.EventBroker.BrokerChannel;
import io.tilt.minka.domain.ShardCapacity.Capacity;
import io.tilt.minka.utils.SlidingSortedSet;

/**
 * Compound information about a node, maintained by the Leader
 * 
 * @author Cristian Gonzalez
 * @since Nov 5, 2015
 */
@JsonAutoDetect
public class Shard implements Comparator<Shard> {

	private static final int MAX_HEARBEATS_TO_EVALUATE = 50;

	private final BrokerChannel brokerChannel;
	private final NetworkShardID shardId;
	@JsonIgnore
	private final DateTime firstTimeSeen;
	@JsonIgnore
	private DateTime lastStatusChange;
	private final SlidingSortedSet<Heartbeat> cardiacLapse;
	private ShardState serviceState;
	private Map<Pallet<?>, Capacity> capacities;
	
	public Shard(final BrokerChannel channel, final NetworkShardID memberId) {
		super();
		this.brokerChannel = channel;
		this.shardId = memberId;
		this.serviceState = ShardState.JOINING;
		this.cardiacLapse = new SlidingSortedSet<>(MAX_HEARBEATS_TO_EVALUATE);
		this.firstTimeSeen = new DateTime(DateTimeZone.UTC);
		this.lastStatusChange = new DateTime(DateTimeZone.UTC);
		this.capacities = new HashMap<>();
	}
	@JsonIgnore
	public DateTime getLastStatusChange() {
		return this.lastStatusChange;
	}
	@JsonIgnore
	public DateTime getFirstTimeSeen() {
		return this.firstTimeSeen;
	}

	public BrokerChannel getBrokerChannel() {
		return this.brokerChannel;
	}

	public NetworkShardID getShardID() {
		return this.shardId;
	}
	
	public void setCapacities(Map<Pallet<?>, Capacity> capacities) {
		this.capacities = capacities;
	}
	
	public Map<Pallet<?>, Capacity> getCapacities() {
		return this.capacities;
	}

	public void addHeartbeat(final Heartbeat hb) {
		if (hb.getStateChange() != null) {
			this.serviceState = hb.getStateChange();
		}
		this.cardiacLapse.add(hb);
	}

	public List<Heartbeat> getHeartbeats() {
		return this.cardiacLapse.values();
	}

	public ShardState getState() {
		return this.serviceState;
	}

	public void setState(ShardState serviceState) {
		this.serviceState = serviceState;
		this.lastStatusChange = new DateTime(DateTimeZone.UTC);
	}

	public int hashCode() {
		return new HashCodeBuilder().append(this.getShardID()).toHashCode();
	}

	@Override
	public boolean equals(Object obj) {
		if (obj instanceof Shard) {
			Shard other = (Shard) obj;
			return other.getShardID().equals(getShardID());
		} else {
			return false;
		}
	}

	@Override
	public String toString() {
		return this.shardId.toString();
	}

	@Override
	public int compare(Shard o1, Shard o2) {
		return o1.getFirstTimeSeen().compareTo(o2.getFirstTimeSeen());
	}
	
	public static class DateComparer implements Comparator<Shard>, Serializable {
		private static final long serialVersionUID = -2098725005810996576L;
		@Override
		public int compare(final Shard s, final Shard s2) {
			return s.getFirstTimeSeen().compareTo(s2.getFirstTimeSeen());
		}
	}
	
	public static class CapacityComparer implements Comparator<Shard>, Serializable {
		private static final long serialVersionUID = 2191475545082914908L;
		private final Pallet<?> pallet;
		public CapacityComparer(Pallet<?> pallet) {
			super();
			this.pallet = pallet;
		}
		@Override
		public int compare(final Shard s, final Shard s2) {
			final Capacity cap1 = s.getCapacities().get(pallet);
			final Capacity cap2 = s2.getCapacities().get(pallet);
			if (cap1 == null) {
				return -1;
			} else if (cap2 == null) {
				return 1;
			} else {
				int ret = Double.compare(cap1.getTotal(), cap2.getTotal());
				return ret == 0 ? -1 : ret;
			}
		}
	}

}
