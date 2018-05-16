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

import static java.util.Objects.requireNonNull;

import java.io.Serializable;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.commons.lang3.StringUtils;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.sun.org.apache.xml.internal.utils.StringComparable;

import io.netty.util.internal.StringUtil;
import io.tilt.minka.api.Pallet;
import io.tilt.minka.api.config.ProctorSettings;
import io.tilt.minka.broker.EventBroker.BrokerChannel;
import io.tilt.minka.core.leader.balancer.Balancer.NetworkLocation;
import io.tilt.minka.domain.ShardCapacity.Capacity;
import io.tilt.minka.utils.CollectionUtils;
import io.tilt.minka.utils.CollectionUtils.SlidingSortedSet;

/**
 * Compound information about a node, maintained by the Leader
 * 
 * @author Cristian Gonzalez
 * @since Nov 5, 2015
 */
@JsonAutoDetect
public class Shard implements Comparator<Shard>, Comparable<Shard> {

	public enum ShardState {
		/** all nodes START in this state while becoming Online after a Quarantine period */
		JOINING,
		/** the node has been continuously online for a long time so it can trustworthly receive work */
		ONLINE,
		/** the node interrupted heartbeats time enough to be considered not healthly
		 * online. in this state all nodes tend to rapidly go ONLINE or fall GONE */
		QUARANTINE,
		/** the node emited a last heartbeat announcing offline mode either being
		 * manually stopped or cleanly shuting down so its ignored by the master */
		QUITTED,
		/** the server discontinued heartbeats and cannot longer be considered alive,
		 * recover its reserved duties */
		GONE
		;
		public boolean isAlive() {
			return this == ONLINE || this == QUARANTINE || this == JOINING;
		}
		public Predicate<Shard> filter() {
			return shard->shard.getState()==this;
		}
		public Predicate<Shard> negative() {
			return shard->shard.getState()!=this;
		}
	}
	
	public static class Change implements Comparator<Change>, Comparable<Change>{
		
		private final Cause cause;
		private final ShardState state;
		private Instant timestamp;
		
		public Change(final Cause cause, final ShardState state) {
			super();
			this.cause = cause;
			this.state = state;
			this.timestamp = Instant.now();
		}
		public Cause getCause() {
			return cause;
		}
		public ShardState getState() {
			return state;
		}
		public Instant getTimestamp() {
			return timestamp;
		}
		@JsonProperty("timestamp")
		public String getTimestamp_() {
			return timestamp.toString();
		}
		@Override
		public int compare(Change o1, Change o2) {
			if (o1==null) {
				return 1;
			} else if (o2==null) {
				return -1;
			} else {
				return o1.getTimestamp().compareTo(o2.getTimestamp());
			}
		}
		@Override
		public int compareTo(Change o) {
			return compare(this, o);
		}
		@Override
		public String toString() {
			return new StringBuilder()
					.append(getTimestamp_()).append(' ')
					.append(state).append(' ')
					.append(cause)
					.toString();
		}
	}
	
	public enum Cause {
		INIT("Initializing"),
		// recognition of a shard who was too much time without beats
		BECAME_ANCIENT("BecameAncient"),
		// too many beats falling below distance and deviation factor to stay online 
		MAX_SICK_FOR_ONLINE("MaxSickForOnline"),
		// too few healthly beats to be useful (after a healthly phase)
		MIN_ABSENT("MinAbsent"),
		// reaching or escaping quarantine-online frontier   
		HEALTHLY_THRESHOLD("MinHealthly"),
		// too few beats yet 
		FEW_HEARTBEATS("FewHeartbeats"),
		// too much time joining
		JOINING_STARVED("JoiningStarved"),
		// follower quitted fine
		FOLLOWER_BREAKUP("FollowerBreakUp"),
		SWITCH_BACK("SwitchBack"),
		;
		final String code;
		Cause(final String code) {
			this.code = code;
		}
		public String getCode() {
			return code;
		}
		@Override
		public String toString() {
			return getCode();
		}
		;
	}
	
	private final BrokerChannel brokerChannel;
	private final NetworkShardIdentifier shardId;
	private final Instant firstTimeSeen;
	private final SlidingSortedSet<Heartbeat> beats;
	private final SlidingSortedSet<Change> changes;
	
	private ShardState serviceState;
	private Map<Pallet<?>, Capacity> capacities;

	public Shard(
			final BrokerChannel channel, 
			final NetworkShardIdentifier memberId) {
		super();
		this.brokerChannel = requireNonNull(channel);
		this.shardId = requireNonNull(memberId);		
		this.beats = CollectionUtils.sliding(ProctorSettings.MAX_HEARBEATS_TO_EVALUATE);
		this.changes = CollectionUtils.sliding(ProctorSettings.MAX_SHARD_CHANGES_TO_HOLD);
		this.capacities = new HashMap<>();
		final Shard.Change first = new Shard.Change(Cause.INIT, ShardState.JOINING);
		applyChange(first);
		this.firstTimeSeen = first.getTimestamp();
	}
	@JsonIgnore
	public Instant getLastStatusChange() {
		return this.changes.last().getTimestamp();
	}
	@JsonIgnore
	public Instant getFirstTimeSeen() {
		return this.firstTimeSeen;
	}
	
	@JsonProperty("last-beat-id")
	private String lastBeatId() {
		return String.valueOf(this.getLast().getSequenceId());
	}

	@JsonIgnore
	public BrokerChannel getBrokerChannel() {
		return this.brokerChannel;
	}

	public NetworkShardIdentifier getShardID() {
		return this.shardId;
	}
	
	public void setCapacities(Map<Pallet<?>, Capacity> capacities) {
		this.capacities = capacities;
	}
	
	@JsonIgnore
	public Map<Pallet<?>, Capacity> getCapacities() {
		return this.capacities;
	}

	public void enterHeartbeat(final Heartbeat hb) {
		this.beats.add(hb);
	}
	
	@JsonIgnore
	public SlidingSortedSet<Heartbeat> getHeartbeats() {
		return this.beats;
	}
	
	@JsonIgnore
	private Heartbeat getLast() {
	    return this.beats.first();
	}

	public ShardState getState() {
		return this.serviceState;
	}

	public void applyChange(final Change change) {
		this.serviceState = change.getState();
		this.changes.add(change);
	}
	
	@JsonIgnore
	public Set<Change> getChanges() {
		return changes.values();
	}
	@JsonProperty("evolution")
	public Collection<String> getChanges_() {
		return changes.values().stream().map(c->c.toString()).collect(Collectors.toList());
	}

	public int hashCode() {
		return new HashCodeBuilder().append(this.getShardID()).toHashCode();
	}

	@Override
	public boolean equals(final Object obj) {
		if (obj == null || !(obj instanceof Shard)) {
			return false;
		} else if (obj == this) {
			return true;
		} else {
			return ((Shard) obj).getShardID().equals(getShardID());
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
	
	public static class DateComparer implements Comparator<NetworkLocation>, Serializable {
		private static final long serialVersionUID = -2098725005810996576L;
		@Override
		public int compare(final NetworkLocation s, final NetworkLocation s2) {
			return compareByCreation(s, s2);
		}
		static int compareByCreation(final NetworkLocation s, final NetworkLocation s2) {
			return s.getCreation().compareTo(s2.getCreation());
		}
	}
	
	
	public static class CapacityComparer implements Comparator<NetworkLocation>, Serializable {
		private static final long serialVersionUID = 2191475545082914908L;
		private final Pallet<?> pallet;
		public CapacityComparer(Pallet<?> pallet) {
			super();
			this.pallet = pallet;
		}
		@Override
		public int compare(final NetworkLocation s, final NetworkLocation s2) {
			final Capacity cap1 = s.getCapacities().get(pallet);
			final Capacity cap2 = s2.getCapacities().get(pallet);
			if (cap1 == null) {
				return -1;
			} else if (cap2 == null) {
				return 1;
			} else {
				int ret = Double.compare(cap1.getTotal(), cap2.getTotal());
				// always the same predictable order 
				ret = ret != 0 ? ret : DateComparer.compareByCreation(s, s2);
				if (ret==0) {
					// TODO refactory
					ret = Arrays.asList(s.getId().getId(), s2.getId().getId())
						.stream().sorted()
						.collect(Collectors.toList())
						.get(0).equals(s.getId().getId()) ? -1 : 1;
				}
				return ret;
			}
		}
	}

	@Override
	public int compareTo(final Shard arg0) {
		return arg0.compare(this, arg0);
	}

}
