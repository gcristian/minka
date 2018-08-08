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
package io.tilt.minka.api.inspect;

import static io.tilt.minka.api.inspect.SystemStateMonitor.toJson;
import static java.util.Objects.requireNonNull;

import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ScheduledThreadPoolExecutor;

import io.tilt.minka.api.Config;
import io.tilt.minka.broker.EventBroker;
import io.tilt.minka.broker.EventBroker.BrokerChannel;
import io.tilt.minka.broker.EventBroker.Channel;
import io.tilt.minka.broker.impl.SocketClient;
import io.tilt.minka.core.leader.data.Scheme;
import io.tilt.minka.core.task.Scheduler;
import io.tilt.minka.core.task.Scheduler.Agent;
import io.tilt.minka.core.task.Semaphore;
import io.tilt.minka.core.task.Semaphore.Action;

/**
 * Read only views built at request about the system state  
 * JSON format.
 * 
 * @author Cristian Gonzalez
 * @since Nov 6, 2016
 */
public class CrossMonitor {

	private final Scheduler scheduler;
	private final EventBroker broker;
	private final Config config;
	
	public CrossMonitor(
			final Scheme scheme,
			final Scheduler scheduler,
			final EventBroker broker, 
			final Config config) {
		
		this.scheduler = requireNonNull(scheduler);
		this.broker = requireNonNull(broker);
		this.config = requireNonNull(config);
	}

	/**
	 * <p>
	 * Shows metrics on the event broker's server and clients, used to communicate shards.  
	 * Each shard has at least one server to listen events from the leader and one client to send
	 * events to the leader. The Leader has one server to listen events from all followers, and 
	 * one client for each follower that sends events to.  
	 * <p>
	 * Full when the current server is the Leader, self broker information when the server is a Follower.
	 * @return			a String in json format
	 */
	public String brokerToJson() {
		return toJson(buildBroker());
	}
	
	/**
	 * <p>
	 * Shows the state of the scheduler 
	 * <p>
	 * Non-Empty only when the current server is the Leader.
	 * @return			a String in json format
	 */
	public String scheduleToJson(final boolean detail) {
		return toJson(buildSchedule(scheduler, detail));
	}
	
	private Map<String, Object> buildBroker() {
		final Map<String, Object> global = new LinkedHashMap<>(3);
		global.put("myself", config.getBroker().getHostPort());
		global.put("inbound", broker.getReceptionMetrics());
		final Set<Entry<BrokerChannel, Object>> entryset = broker.getSendMetrics().entrySet();
		final Map<Channel, Map<String, Object>> clients = new LinkedHashMap<>(entryset.size());
		for (Entry<BrokerChannel, Object> e: entryset) {
			Map<String, Object> shardsByChannel = clients.get(e.getKey().getChannel());
			if (shardsByChannel==null) {
				clients.put(e.getKey().getChannel(), shardsByChannel=new LinkedHashMap<>());
			}
			final Map<String, String> brief = new LinkedHashMap<>(4);
			if (e.getValue() instanceof SocketClient) {
				final SocketClient sc = (SocketClient)e.getValue();				
				brief.put("queue-size", String.valueOf(sc.getQueueSize()));
				brief.put("sent-counter", String.valueOf(sc.getSentCounter()));
				brief.put("alive", String.valueOf(sc.getAlive()));
				brief.put("retry-counter", String.valueOf(sc.getRetry()));
			}
			shardsByChannel.put(e.getKey().getAddress().toString(), brief);
		}
		global.put("outbound", clients);
		return global;
	}
	
	private Map<String, Object> buildSchedule(final Scheduler schedule, final boolean detail) {
		final Set<Entry<Action, Agent>> entrySet = schedule.getAgents().entrySet();
		final Map<String, Object> ret = new LinkedHashMap<>(entrySet.size() + 4);
		try {
			final ScheduledThreadPoolExecutor executor = schedule.getExecutor();
			final long now = System.currentTimeMillis();
			ret.put("queue-size", executor.getQueue().size());
			ret.put("corepool-size", executor.getCorePoolSize());
			ret.put("tasks-count", executor.getTaskCount());
			ret.put("tasks-completed", executor.getCompletedTaskCount());
			
			final Map<String, List> byFrequency = new LinkedHashMap<>(2);
			for (final Entry<Semaphore.Action, Scheduler.Agent> e: entrySet) {
				final Agent sync = e.getValue();
				final Map<String, String> mapped = agentToMap(now, detail, sync);
				
				List byTask = byFrequency.get(sync.getFrequency().name());
				if (byTask==null) {
					byFrequency.put(sync.getFrequency().name(), byTask = new LinkedList<>());
				}
				byTask.add(mapped);
			}
			ret.put("agents", byFrequency);
			
		} catch (Throwable t) {
		}
		return ret;
	}

	private Map<String, String> agentToMap(final long now, final boolean detail, final Agent sync) {
		final Map<String, String> t = new LinkedHashMap<>(9);
		t.put("task", sync.getAction().name());
		if (detail && sync.getFrequency()==Scheduler.Frequency.PERIODIC) {
			t.put("frequency-unit", String.valueOf(sync.getTimeUnit()));
			t.put("frequency-time", String.valueOf(sync.getPeriodicDelay()));
		}
		if (detail && sync.getDelay()>0) {
			t.put("start-delay", String.valueOf(sync.getDelay()));
		}
		if (detail && sync.getLastQueueWait()>0) {
			t.put("blocked-last", String.valueOf(sync.getLastQueueWait()));
		}
		t.put("blocked-accum", String.valueOf(sync.getAccumulatedWait()));
		if (detail) { 	
			t.put("last-run", String.valueOf(sync.getLastTimestamp()));
		}
		if (sync.getLastSuccessfulTimestamp()!=sync.getLastTimestamp()) {
			t.put("last-run-success", String.valueOf(sync.getLastSuccessfulTimestamp()));
		}
		if (sync.getFrequency()==Scheduler.Frequency.PERIODIC) {
			t.put("last-run-distance", String.valueOf(now - sync.getLastTimestamp()));
		}
		if (sync.getLastSuccessfulDuration()>0) {
			t.put("last-run-duration", String.valueOf(sync.getLastSuccessfulDuration()));
		}
		t.put("duration-accum", String.valueOf(sync.getAccumulatedDuration()));
		if (sync.getLastException() != null) { 
			t.put("exception", sync.getLastException().toString());
		}
		if (detail) { 
			t.put("lambda", sync.getTask().getClass().getSimpleName());
		}
		return t;
	}

	
}
