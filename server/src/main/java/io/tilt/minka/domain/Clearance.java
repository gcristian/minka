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
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import io.tilt.minka.core.follower.Follower;
import io.tilt.minka.core.leader.Leader;

/**
 * A blessing from the Leader to Follower for it to know God exists 
 * built by {@link Leader} allowing the {@link Follower} to keep working hard for the Holy Church  
 *  
 * @author Cristian Gonzalez
 * @since Nov 5, 2015
 */
public class Clearance implements Serializable, Comparable<Clearance> {

	private static final long serialVersionUID = 4828220405145911529L;
	private static final AtomicLong sequencer = new AtomicLong();

	private final NetworkShardIdentifier leaderShardId;
	private final DateTime creation;
	private final long sequenceId;

	public static Clearance create(final NetworkShardIdentifier leaderShardId) {
		return new Clearance(leaderShardId);
	}

	private Clearance(final NetworkShardIdentifier leaderShardId) {
		this.creation = new DateTime(DateTimeZone.UTC);
		this.sequenceId = sequencer.incrementAndGet();
		this.leaderShardId = leaderShardId;
	}

	public DateTime getCreation() {
		return this.creation;
	}

	public long getSequenceId() {
		return this.sequenceId;
	}

	@Override
	public int hashCode() {
		return new HashCodeBuilder()
				.append(getLeaderShardId())
				.append(getCreation())
				.append(getSequenceId())
				.toHashCode();
	}

	@Override
	public boolean equals(Object obj) {
		if (obj instanceof Clearance) {
			Clearance other = (Clearance) obj;
			return new EqualsBuilder()
					.append(getSequenceId(), other.getSequenceId())
					.append(getLeaderShardId(), other.getLeaderShardId())
					.isEquals();
		} else {
			return false;
		}
	}

	public NetworkShardIdentifier getLeaderShardId() {
		return this.leaderShardId;
	}

	@Override
	public String toString() {
		return new StringBuilder().append(" Clearance Sequence ID: ").append(sequenceId).append(" - ShardID: ")
				.append(getLeaderShardId()).append(" - Created: ").append(getCreation()).toString();
	}

	@Override
	public int compareTo(Clearance o) {
		return o.getCreation().compareTo(getCreation());
	}
}
