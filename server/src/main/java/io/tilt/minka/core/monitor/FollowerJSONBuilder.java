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
package io.tilt.minka.core.monitor;

import static java.util.Objects.requireNonNull;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import org.apache.commons.lang.Validate;

import io.tilt.minka.core.follower.LeaderEventsHandler;
import io.tilt.minka.core.leader.data.Scheme;
import io.tilt.minka.domain.Heartbeat;
import io.tilt.minka.domain.ShardEntity;
import io.tilt.minka.domain.ShardedPartition;
import io.tilt.minka.shard.Shard;

/**
 * Read only views built at request about the system state  
 * JSON format.
 * 
 * @author Cristian Gonzalez
 * @since Nov 6, 2016
 */
public class FollowerJSONBuilder {

	private final Scheme scheme;
	private final ShardedPartition partition;
	private final LeaderEventsHandler leaderHandler;
	
	
	public FollowerJSONBuilder(
			final Scheme scheme,
			final ShardedPartition partition,
			final LeaderEventsHandler leaderHandler) {
		
		this.scheme = requireNonNull(scheme);
		this.partition = requireNonNull(partition);
		this.leaderHandler = requireNonNull(leaderHandler);

	}
	
	public String beatsToJson() {
		return SystemStateMonitor.toJson(buildBeats());
	}

	/**
	 * <p>
	 * Shows the duties captured by the shard.
	 * @return			a String in json format
	 */
	public String partitionToJson(boolean detailed) {
		if (leaderHandler.getLastClearance()==null) {
			return "";
		}
		
		final Map<String, Object> ret = new LinkedHashMap<>(6);
		ret.put("domain-pallets", ShardEntity.toStringBrief(
				leaderHandler.getLastClearance().getInfo().getDomainPallets()));		
		final long distance = System.currentTimeMillis() - 
				leaderHandler.getLastClearance().getCreation().getMillis();
		ret.put("clearance-distance-ms", distance);
		
		ret.putAll(buildPartitionDuties(partition, detailed));
		
		return SystemStateMonitor.toJson(ret);
	}
	
	private Map<Shard, Heartbeat> buildBeats() {
		final Map<Shard, Heartbeat> ret = new HashMap<>();
		this.scheme.getCommitedState().findShards(null, s-> {
			Heartbeat last = null;
			Iterator<Heartbeat> it=s.getHeartbeats().descend();
			while(it.hasNext()) {
				final Heartbeat hb = it.next();
				if (last==null) {
					last = hb;
					ret.put(s, hb);
				} else {
					// put the last relevant to watch
					if (hb.reportsDuties()) {
						if (!last.reportsDuties()) {
							last = hb;
						} else {
							if (hb.getCreation().isAfter(last.getCreation())) {
								last = hb;
							}
						}
					}
				}
			}
		});
		return ret;
	}
	
	private Map<String, List<Object>> buildReplicas(final boolean detail) {
		Validate.notNull(scheme);
		final Map<String, List<Object>> byPalletId = new LinkedHashMap<>();
		final Consumer<ShardEntity> adder = addler(detail, byPalletId);
		partition.getReplicas().forEach(adder);;
		return byPalletId;
	}

	private Consumer<ShardEntity> addler(final boolean detail, final Map<String, List<Object>> byPalletId) {
		final Consumer<ShardEntity> adder = d-> {
			List<Object> pid = byPalletId.get(d.getDuty().getPalletId());
			if (pid==null) {
				byPalletId.put(d.getDuty().getPalletId(), pid = new ArrayList<>());
			}
			pid.add(detail ? d : d.getDuty().getId());
		};
		return adder;
	}

	private Map<String, Object> buildPartitionDuties(final ShardedPartition partition, boolean detail) {
		Validate.notNull(partition);
		
		final StringBuilder sb1 = new StringBuilder(partition.getDuties().size() * 16);
		for (ShardEntity e: partition.getDuties()) {
			sb1.append(detail ? e: e.getDuty().getId()).append(',');
		}
		
		final StringBuilder sb2 = new StringBuilder(partition.getReplicas().size() * 16);
		for (ShardEntity e: partition.getReplicas()) {
			sb2.append(detail ? e: e.getDuty().getId()).append(',');
		}
		final Map<String, Object> ret = new LinkedHashMap<>(4);
		ret.put("size-part", partition.getDuties().size());
		ret.put("size-repl", partition.getReplicas().size());
		ret.put("partition", sb1.toString());
		ret.put("replicas", sb2.toString());
		return ret;
	}
		
}
