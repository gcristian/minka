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
package io.tilt.minka.shard;

import static java.util.Objects.requireNonNull;

import java.time.Instant;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.lang.builder.HashCodeBuilder;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

import io.tilt.minka.api.Pallet;
import io.tilt.minka.api.config.ProctorSettings;
import io.tilt.minka.broker.EventBroker.BrokerChannel;
import io.tilt.minka.domain.Heartbeat;
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

	private final BrokerChannel brokerChannel;
	private final NetworkShardIdentifier shardId;
	private final Instant firstTimeSeen;
	private final SlidingSortedSet<Heartbeat> beats;
	private final SlidingSortedSet<Transition> transitions;
	
	private ShardState serviceState;
	private Map<Pallet, ShardCapacity> shardCapacities;

	public Shard(
			final BrokerChannel channel, 
			final NetworkShardIdentifier memberId) {
		super();
		this.brokerChannel = requireNonNull(channel);
		this.shardId = requireNonNull(memberId);		
		this.beats = CollectionUtils.sliding(ProctorSettings.MAX_HEARBEATS_TO_EVALUATE);
		this.transitions = CollectionUtils.sliding(ProctorSettings.MAX_SHARD_CHANGES_TO_HOLD);
		this.shardCapacities = new HashMap<>();
		final Transition first = new Transition(TransitionCause.INIT, ShardState.JOINING);
		applyChange(first);
		this.firstTimeSeen = first.getTimestamp();
	}
	
	@JsonProperty(value="tag", index=1)
	private String getTag() {
		return this.shardId.getTag();
	}

	@JsonProperty(value="broker-connect", index=2)
	private String getBrokerConnect() {
		return this.shardId.getAddress().getHostAddress();
	}
	@JsonProperty(value="web-connect",index=3)
	private String getWebConnect() {
		return this.shardId.getWebHostPort();
	}
	
	@JsonIgnore
	public Instant getLastTransition() {
		return this.transitions.last().getTimestamp();
	}
	@JsonIgnore
	public Instant getFirstTimeSeen() {
		return this.firstTimeSeen;
	}

	@JsonIgnore
	public BrokerChannel getBrokerChannel() {
		return this.brokerChannel;
	}

	@JsonIgnore
	public NetworkShardIdentifier getShardID() {
		return this.shardId;
	}
		
	public void setCapacities(Map<Pallet, ShardCapacity> shardCapacities) {
		this.shardCapacities = shardCapacities;
	}
	
	@JsonIgnore
	public Map<Pallet, ShardCapacity> getCapacities() {
		return this.shardCapacities;
	}
	@JsonProperty(value="shardCapacities", index=5)
	public Map<String, Double> briefCapacities() {
		final Map<String, Double> ret = new LinkedHashMap<>();
		for (final Map.Entry<Pallet, ShardCapacity> e: shardCapacities.entrySet()) {
			ret.put(e.getKey().getId(), e.getValue().getTotal());
		}
		return ret;
	}

	public void enterHeartbeat(final Heartbeat hb) {
		final Map<Pallet, ShardCapacity> cap = hb.getCapacities();
		if (cap!=null) {
			setCapacities(new HashMap<>(cap));
		}
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

	@JsonProperty(index=1)
	public ShardState getState() {
		return this.serviceState;
	}

	public void applyChange(final Transition transition) {
		this.serviceState = transition.getState();
		this.transitions.add(transition);
	}
	
	@JsonIgnore
	public SlidingSortedSet<Transition> getTransitions() {
		return transitions;
	}
	
	@JsonProperty(value="state-transitions", index=6)
	public Collection<String> getTransitions_() {
		return transitions.values().stream().map(c->c.toString()).collect(Collectors.toList());
	}

	@JsonProperty(value="last-beat-id", index=7)
	private String lastBeatId() {
		return String.valueOf(this.getLast().getSequenceId());
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
	
	@Override
	public int compareTo(final Shard arg0) {
		return arg0.compare(this, arg0);
	}

}
