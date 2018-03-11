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

import org.apache.commons.lang.Validate;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

import io.tilt.minka.api.Pallet;
import io.tilt.minka.broker.EventBroker.BrokerChannel;
import io.tilt.minka.core.leader.distributor.Balancer.Location;
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
public class Shard implements Comparator<Shard> {

	private static final int MAX_HEARBEATS_TO_EVALUATE = 50;
	private static final int MAX_HEARBEATS_CHANGES_TO_KEEP = 10;

	private final BrokerChannel brokerChannel;
	private final NetworkShardIdentifier shardId;
	
	private final DateTime firstTimeSeen;
	
	private DateTime lastStatusChange;
	private final SlidingSortedSet<Heartbeat> cardiacLapse;
	private final SlidingSortedSet<Heartbeat> changesToKeep;
	private ShardState serviceState;
	private Map<Pallet<?>, Capacity> capacities;
	
	public Shard(final BrokerChannel channel, final NetworkShardIdentifier memberId) {
		super();
		Validate.notNull(memberId);
		Validate.notNull(channel);
		this.brokerChannel = channel;
		this.shardId = memberId;
		this.serviceState = ShardState.JOINING;
		this.cardiacLapse = CollectionUtils.sliding(MAX_HEARBEATS_TO_EVALUATE);
		this.changesToKeep = CollectionUtils.sliding(MAX_HEARBEATS_CHANGES_TO_KEEP);
		this.firstTimeSeen = new DateTime(DateTimeZone.UTC);
		this.lastStatusChange = new DateTime(DateTimeZone.UTC);
		this.capacities = new HashMap<>();
	}
	
	@JsonIgnore
	public DateTime getLastStatusChange() {
		return this.lastStatusChange;
	}
	@JsonProperty(index=2, value="last-change")
	private String getLastStatusChange_() {
		return this.lastStatusChange.toString();
	}
	@JsonIgnore
	public DateTime getFirstTimeSeen() {
		return this.firstTimeSeen;
	}
	@JsonProperty(index=1, value="first-seen")
	private String getFirstTime() {
		return this.firstTimeSeen.toString();
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

	public void addHeartbeat(final Heartbeat hb) {
		if (hb.getStateChange() != null) {
			this.serviceState = hb.getStateChange();
		}
		this.cardiacLapse.add(hb);
		if (hb.hasDifferences() || hb.hasWarning()) {
		    this.changesToKeep.add(hb);
		}
	}

	@JsonIgnore
	public List<Heartbeat> getHeartbeats() {
		return this.cardiacLapse.values();
	}
	
	@JsonProperty(index=3, value="heartbeat-last")
	private Heartbeat getLast() {
	    return this.cardiacLapse.first();
	}
	
	@JsonProperty(index=4, value="heartbeat-changes")
	private SlidingSortedSet<Heartbeat> getChangesToKeep() {
        return this.changesToKeep;
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
	public boolean equals(final Object obj) {
	    if (obj==null || !(obj instanceof Shard)) {
	        return false;
	    } else if (obj==this) {
	        return true;
	    } else {
			return ((Shard)obj).getShardID().equals(getShardID());
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
	
	public static class DateComparer implements Comparator<Location>, Serializable {
		private static final long serialVersionUID = -2098725005810996576L;
		@Override
		public int compare(final Location s, final Location s2) {
			return s.getCreation().compareTo(s2.getCreation());
		}
	}
	
	
	public static class CapacityComparer implements Comparator<Location>, Serializable {
		private static final long serialVersionUID = 2191475545082914908L;
		private final Pallet<?> pallet;
		public CapacityComparer(Pallet<?> pallet) {
			super();
			this.pallet = pallet;
		}
		@Override
		public int compare(final Location s, final Location s2) {
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

	public enum ShardState {

	    /**
	     * all nodes START in this state
	     * while becoming Online after a Quarantine period
	     */
	    JOINING,
	    /** 
	    * the node has been continuously online for a long time
	    * so it can trustworthly receive work 
	    */
	    ONLINE,
	    /**
	        * the node interrupted heartbeats time enough to be
	        * considered not healthly online.
	        * in this state all nodes tend to rapidly go ONLINE or fall GONE 
	        */
	    QUARANTINE,

	    /** 
	    * the node emited a last heartbeat announcing offline mode
	    * either being manually stopped or cleanly shuting down
	    * so its ignored by the master
	    */
	    QUITTED,

	    /**
	    * the server discontinued heartbeats and cannot longer
	    * be considered alive, recover its reserved duties
	    */
	    GONE

	    ;

	    public boolean isAlive() {
	        return this == ONLINE || this == QUARANTINE || this == JOINING;
	    }

	    public enum Reason {

	        INITIALIZING,
	        /*
	         * the shard persistently ignores commands from the Follower shard
	         */
	        REBELL,
	        /*
	         * "inconsistent behaviour measured in short lapses"
	         *
	         * not trustworthly shard
	         */
	        FLAPPING,
	        /*
	         * "no recent HBs from the shard, long enough"
	         * 
	         * the node has ceased to send heartbeats for time enough to be
	         * considered gone and unrecoverable so it is ignored by the master
	         */
	        LOST;
	    }
	}

}
