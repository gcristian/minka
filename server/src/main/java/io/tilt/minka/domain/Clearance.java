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
import java.util.concurrent.atomic.AtomicLong;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

/**
 * A signal of clearance from the leader to the followers to keep having authority on the delegated duties  
 *  
 * @author Cristian Gonzalez
 * @since Nov 5, 2015
 */
public class Clearance implements Serializable, Comparable<Clearance> {

	private static final long serialVersionUID = 4828220405145911529L;
	private static final AtomicLong sequencer = new AtomicLong();

	private final NetworkShardIdentifier leaderShardId;
	private final DateTime creation;
	private final DomainInfo info;
	private final long sequenceId;

	public static Clearance create(
			final NetworkShardIdentifier leaderShardId,
			final DomainInfo info) {
		return new Clearance(leaderShardId, info);
	}

	private Clearance(final NetworkShardIdentifier leaderShardId, final DomainInfo info) {
		this.creation = new DateTime(DateTimeZone.UTC);
		this.sequenceId = sequencer.incrementAndGet();
		this.leaderShardId = requireNonNull(leaderShardId);
		this.info = requireNonNull(info);
	}

	public DateTime getCreation() {
		return this.creation;
	}
	public long getSequenceId() {
		return this.sequenceId;
	}
	public DomainInfo getInfo() {
		return info;
	}
	public NetworkShardIdentifier getLeaderShardId() {
		return this.leaderShardId;
	}
	
	@Override
	public int hashCode() {
		final int prime = 31;
		int res = 0;
		res *= prime + (leaderShardId.hashCode());
		res *= prime + (sequenceId);
		return res;
	}

	@Override
	public boolean equals(Object obj) {
		if (obj== null || !(obj instanceof Clearance) ) {
			return false;
		} else if (obj==this) {
			return true;
		} else {
			final Clearance o = (Clearance)obj;
			return o.getLeaderShardId().equals(leaderShardId)
					&& o.getSequenceId()==sequenceId;
		}
	}

	@Override
	public String toString() {
		return new StringBuilder()
				.append(" Clearance Sequence ID: ").append(sequenceId)
				.append(" - ShardID: ").append(getLeaderShardId())
				.append(" - Created: ").append(getCreation())
				.toString();
	}

	@Override
	public int compareTo(Clearance o) {
		return o.getCreation().compareTo(getCreation());
	}
}
