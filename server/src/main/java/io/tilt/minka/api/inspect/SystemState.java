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

import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.stream.Collectors;

import javax.ws.rs.core.Response;

import org.apache.commons.lang.Validate;
import org.joda.time.DateTime;
import org.json.JSONArray;
import org.json.JSONObject;

import com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.core.JsonGenerator.Feature;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.util.JSONPObject;

import io.tilt.minka.broker.EventBroker;
import io.tilt.minka.broker.EventBroker.BrokerChannel;
import io.tilt.minka.broker.EventBroker.Channel;
import io.tilt.minka.broker.impl.SocketClient;
import io.tilt.minka.core.leader.balancer.Balancer.BalancerMetadata;
import io.tilt.minka.core.leader.data.Backstage;
import io.tilt.minka.core.leader.data.Scheme;
import io.tilt.minka.core.leader.data.ShardingScheme;
import io.tilt.minka.core.leader.distributor.ChangePlan;
import io.tilt.minka.core.task.LeaderShardContainer;
import io.tilt.minka.core.task.Scheduler;
import io.tilt.minka.core.task.Scheduler.Agent;
import io.tilt.minka.core.task.Scheduler.Synchronized;
import io.tilt.minka.core.task.Semaphore;
import io.tilt.minka.domain.EntityEvent;
import io.tilt.minka.domain.EntityState;
import io.tilt.minka.domain.NetworkShardIdentifier;
import io.tilt.minka.domain.Shard;
import io.tilt.minka.domain.ShardEntity;
import io.tilt.minka.domain.ShardIdentifier;
import io.tilt.minka.domain.ShardedPartition;
import io.tilt.minka.utils.CollectionUtils;
import io.tilt.minka.utils.CollectionUtils.SlidingSortedSet;

/**
 * Read only views about the {@linkplain Scheme} 
 * JSON format.
 * 
 * @author Cristian Gonzalez
 * @since Nov 6, 2016
 */
@SuppressWarnings("unused")
@JsonPropertyOrder({"global", "shards", "pallets", "roadmaps"})
public class SchemeViews {

	private final LeaderShardContainer leaderShardContainer; 
	private final SlidingSortedSet<String> changePlanHistory;
	private final SlidingSortedSet<Long> planids;
	private final ShardingScheme scheme;
	private final ChangePlan[] lastPlan = {null};
	private final EventBroker broker;
	
	public static final ObjectMapper mapper; 
	static {
		mapper = new ObjectMapper();
		mapper.setVisibility(PropertyAccessor.ALL, Visibility.NONE);
		mapper.setVisibility(PropertyAccessor.FIELD, Visibility.ANY);
		//mapper.configure(SerializationFeature.INDENT_OUTPUT, true);
		mapper.configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, true);
		mapper.configure(SerializationFeature.FAIL_ON_SELF_REFERENCES, false);
		mapper.configure(SerializationFeature.WRITE_SINGLE_ELEM_ARRAYS_UNWRAPPED, false);
		mapper.configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);
		mapper.configure(Feature.WRITE_NUMBERS_AS_STRINGS, true);
	}

	public SchemeViews(
			final LeaderShardContainer leaderShardContainer, 
			final ShardingScheme scheme, 
			final EventBroker broker) {
		this.leaderShardContainer = requireNonNull(leaderShardContainer);
		this.scheme = requireNonNull(scheme);
		this.broker = requireNonNull(broker);
		this.changePlanHistory = CollectionUtils.sliding(20);
		this.planids = CollectionUtils.sliding(10);
		scheme.addChangeObserver(()->catchPlan());
	}

	private void catchPlan() {
		try {
			if (lastPlan[0]!=null) {
				if (!lastPlan[0].equals(scheme.getCurrentPlan())) {
					changePlanHistory.add(toJson(lastPlan[0]));
					planids.add(lastPlan[0].getId());
				}
			}
			lastPlan[0] = scheme.getCurrentPlan();
		} catch (Exception e) {
		}
	}

	public String toJson(final Object o) throws JsonProcessingException {
		return mapper.writeValueAsString(o);
	}
	
	public String plansToJson() {
		try {
			final JSONObject js = new JSONObject();
			final JSONArray arr = new JSONArray();
			js.put("size", changePlanHistory.size());
			changePlanHistory.values().forEach(s-> arr.put(new JSONObject(s)));
			js.put("ids", this.planids.values());
			if (lastPlan[0]!=null && !lastPlan[0].getResult().isClosed()) {
				js.put("current", new JSONObject(toJson(lastPlan[0])));
			} else if (lastPlan[0]!=null) {
				arr.put(new JSONObject(toJson(lastPlan[0])));
			}
			
			js.put("history", arr);
	        return toJson(js.toMap());
		} catch (Exception e) {
			e.printStackTrace();
		}
		return "";
	}
	
	public String brokerToJSon() throws JsonProcessingException {
		final Map<String, Map> global = new LinkedHashMap<>();
		global.put("inbound", broker.getReceptionMetrics());
		final Map<Channel, Map<String, Object>> clients = new LinkedHashMap<>();
		for (Entry<BrokerChannel, Object> e: broker.getSendMetrics().entrySet()) {
			Map<String, Object> shardsByChannel = clients.get(e.getKey().getChannel());
			if (shardsByChannel==null) {
				clients.put(e.getKey().getChannel(), shardsByChannel=new LinkedHashMap<>());
			}
			final Map<String, String> brief = new LinkedHashMap<>();
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
		
		return toJson(global);
	}
	public String shardsToJson() throws JsonProcessingException {
		return mapper.writeValueAsString(buildShards(scheme));
	}

	private Map<String, Object> buildShards(final ShardingScheme scheme) {
		final Map<String, Object> map = new LinkedHashMap<>();
		map.put("leader", leaderShardContainer.getLeaderShardId());
		map.put("previous", leaderShardContainer.getAllPreviousLeaders());
		final Map<String, List<String>> tmp = new HashMap<>();
		scheme.getScheme().getGoneShards().entrySet().forEach(e-> {
			tmp.put(e.getKey().getId(), e.getValue().values().stream().map(c->c.toString()).collect(toList()));
		});
		map.put("gone", tmp);
		final List<Shard> list = new ArrayList<>();
		scheme.getScheme().findShards(null, list::add);
		map.put("shards", list);
		return map;
	}

	public String distributionToJson() throws JsonProcessingException {
		return mapper.writeValueAsString(buildDistribution());
	}
	
	public Map<String, Object> buildDistribution() {
		final Map<String, Object> map = new LinkedHashMap<>();
		map.put("global", buildGlobal(scheme));
		map.put("distribution", buildShardRep(scheme));
		return map;
	}

	public String schemeToJson(final boolean detail) throws JsonProcessingException {
		Map<String, Object> m = new LinkedHashMap<>();
		m.put("scheme", buildDuties(detail));
		m.put("backstage", buildBackstage(detail, scheme.getBackstage()));
		return toJson(m);
	}
	
	public String followerEntitiesToJson(final ShardedPartition partition) throws JsonProcessingException {
		return toJson(buildFollowerDuties(partition, true));
	}
	
	private Map<String, List<Object>> buildDuties(final boolean detail) {
		Validate.notNull(scheme);
		final Map<String, List<Object>> byPalletId = new LinkedHashMap<>();
		scheme.getScheme().findDuties(d-> {
			List<Object> pid = byPalletId.get(d.getDuty().getPalletId());
			if (pid==null) {
				byPalletId.put(d.getDuty().getPalletId(), pid = new ArrayList<>());
			}
			pid.add(detail ? d : d.getDuty().getId());
		});
		return byPalletId;
	}

	private List<Object> dutyBrief(final Collection<ShardEntity> coll, final boolean detail) {
		List<Object> ret = new ArrayList<>();
		if (detail) {
			coll.stream()
				.map(d->d.getDuty().getId())
				.forEach(ret::add);
		} else {
			coll.forEach(ret::add);
		}
		return ret;
	}
	
	private Map<String, List<Object>> buildBackstage(final boolean detail, final Backstage stage) {
		final Map<String, List<Object>> ret = new LinkedHashMap<>();		
		ret.put("crud", dutyBrief(stage.getDutiesCrud(), detail));
		ret.put("dangling", dutyBrief(stage.getDutiesDangling(), detail));
		ret.put("missing", dutyBrief(stage.getDutiesMissing(), detail));
		return ret;
	}

	private List<Object> buildFollowerDuties(final ShardedPartition partition, boolean entities) {
		Validate.notNull(partition);
		final List<Object> ret = new ArrayList<>();
		for (ShardEntity e: partition.getDuties()) {
			ret.add(entities ? e: e.getDuty());
		}
		return ret;
	}

	public String palletsToJson() throws JsonProcessingException {
		return toJson(buildPallets());
	}
		
	private List<Map<String, Object>> buildPallets() {
		final List<Map<String, Object>> ret = new ArrayList<>();
		final Scheme.SchemeExtractor extractor = new Scheme.SchemeExtractor(scheme.getScheme());
		
		for (final ShardEntity pallet: extractor.getPallets()) {
			
			final double[] dettachedWeight = {0};
			final int[] crudSize = new int[1];
			scheme.getBackstage().findDutiesCrud(EntityEvent.CREATE::equals, EntityState.PREPARED::equals, e-> {
				if (e.getDuty().getPalletId().equals(pallet.getPallet().getId())) {
					crudSize[0]++;
					dettachedWeight[0]+=e.getDuty().getWeight();
				}
			});
								

			final List<DutyView> dutyRepList = new ArrayList<>();
			scheme.getScheme().findDutiesByPallet(pallet.getPallet(), 
					d -> dutyRepList.add(new DutyView(
									d.getDuty().getId(),
									d.getDuty().getWeight())));
			
			ret.add(palletView(
						pallet.getPallet().getId(),
						extractor.getCapacityTotal(pallet.getPallet()),
						extractor.getSizeTotal(pallet.getPallet()),
						extractor.getWeightTotal(pallet.getPallet()),
						pallet.getPallet().getMetadata().getBalancer().getName(), 
						crudSize[0], 
						dettachedWeight[0], 
						new DateTime(pallet.getJournal().getFirst().getHead()),
						pallet.getPallet().getMetadata(),
						dutyRepList
					));
		}
		return ret;
	}

	private static List<Map<String, Object>> buildShardRep(final ShardingScheme table) {	    
		final List<Map<String, Object>> ret = new LinkedList<>();
		final Scheme.SchemeExtractor extractor = new Scheme.SchemeExtractor(table.getScheme());
		for (final Shard shard : extractor.getShards()) {
			final List<Map<String , Object>> palletsAtShard =new LinkedList<>();
			
			for (final ShardEntity pallet: extractor.getPallets()) {
				final List<DutyView> dutyRepList = new LinkedList<>();
				int[] size = new int[1];
				table.getScheme().findDuties(shard, pallet.getPallet(), d-> {
					size[0]++;
					dutyRepList.add(
						new DutyView(
								d.getDuty().getId(),
								d.getDuty().getWeight()));});
				palletsAtShard.add(
						palletAtShardView(
								pallet.getPallet().getId(),
								extractor.getCapacity(pallet.getPallet(), shard),
								size[0],
								extractor.getWeight(pallet.getPallet(), shard),
								dutyRepList));
				
			}
			ret.add(shardView(
					shard.getShardID().getId(),
					shard.getFirstTimeSeen(),
					palletsAtShard, 
					shard.getState().toString()));
		}
		return ret;
	}

	private static Map<String, Object> buildGlobal(final ShardingScheme table) {
		Scheme.SchemeExtractor extractor = new Scheme.SchemeExtractor(table.getScheme());
		final Map<String, Object> map = new LinkedHashMap<>();
		map.put("size-shards", extractor.getShards().size());
		map.put("size-pallets", extractor.getPallets().size());
		map.put("size-scheme", extractor.getSizeTotal());
		map.put("size-crud", table.getBackstage().getDutiesCrud().size());
		map.put("size-missings", table.getBackstage().getDutiesMissing().size());
		map.put("size-dangling", table.getBackstage().getDutiesDangling().size());
		map.put("stage-change", table.getBackstage().isStealthChange());
		map.put("scheme-change", table.getScheme().isStealthChange());
		return map;
	}
	
	
	private static Map<String, Object> shardView(
			final String shardId, 
			final Instant creation, 
			final List<Map<String, Object>> pallets, 
			final String status) {
			
		final Map<String, Object> map = new LinkedHashMap<>();
		map.put("shard-id", shardId);
		map.put("creation", creation.toString());
		map.put("status", status);
		map.put("pallets", pallets);
		return map;
	}
	
	
	private static Map<String , Object> palletAtShardView (
			final String id, final double capacity, final int size, final double weight, 
			final List<DutyView> duties) {

		final Map<String , Object> map = new LinkedHashMap<>();
		map.put("id", id);
		map.put("size", size);
		map.put("capacity", capacity);
		map.put("weight", weight);
		StringBuilder sb = new StringBuilder();
		duties.forEach(d->sb.append(d.id).append(", "));
		map.put("duties",sb.toString());
		return map;
	}
	
	private static class DutyView {
		private final String id;
		private final double weight;
		protected DutyView(final String id, final double weight) {
			super();
			this.id = id;
			this.weight = weight;
		}
	}
	
	private static Map<String, Object> palletView(final String id, final double capacity, final int stagedSize, 
			final double stagedWeight, final String balancer, final int unstagedSize, 
			final double unstagedWeight, final DateTime creation, final BalancerMetadata meta, 
			final List<DutyView> duties) {
		
		final Map<String, Object> map = new LinkedHashMap<>();
		map.put("id", id);
		map.put("size", unstagedSize + stagedSize);
		map.put("cluster-capacity", capacity);
		map.put("size-staged", stagedSize);
		map.put("size-unstaged", unstagedSize);
		map.put("weight-staged", stagedWeight);
		map.put("weight-unstaged", unstagedWeight);
		String str = "0";
		if (unstagedSize + stagedSize > 0 && stagedSize > 0) {
			str = String.valueOf((stagedSize*100) / (unstagedSize + stagedSize)) ;
		}
		map.put("allocation", str + "%");
		map.put("creation", creation.toString());
		map.put("balancer-metadata", meta); 
		map.put("balancer", balancer);
		StringBuilder sb = new StringBuilder();
		duties.forEach(d->sb.append(d.id).append(":").append(d.weight).append(","));
		map.put("duties", sb.toString());
		return map;
	}
	
	public String scheduleToJson(final Scheduler schedule) throws JsonProcessingException {
		return toJson(getDetail(schedule));
				
	}
	
	
	public Map<String, Object> getDetail(final Scheduler schedule) {
		final Map<String, Object> ret = new LinkedHashMap<>();
		try {
			final ScheduledThreadPoolExecutor executor = schedule.getExecutor();
			final Map<Synchronized, ScheduledFuture<?>> futures = schedule.getFutures();
			final long now = System.currentTimeMillis();
			ret.put("queue-size", executor.getQueue().size());
			ret.put("corepool-size", executor.getCorePoolSize());
			ret.put("tasks-count", executor.getTaskCount());
			ret.put("tasks-completed", executor.getCompletedTaskCount());
			
			for (final Entry<Semaphore.Action, Scheduler.Agent> e: schedule.getAgents().entrySet()) {
				final Agent sync = e.getValue();
				
				final Map<String, String> t = new LinkedHashMap<>();
				t.put("enum", e.getKey().name());
				t.put("frequency", sync.getFrequency().name());
				t.put("periodic-delay", String.valueOf(sync.getPeriodicDelay()));
				t.put("start-delay", String.valueOf(sync.getDelay()));
				t.put("time-unit", String.valueOf(sync.getTimeUnit()));

				final long timestamp = now - sync.getLastExecutionTimestamp();
				t.put("timestamp", String.valueOf(timestamp));
				final long stime = now - sync.getLastSuccessfulExecutionTimestamp();
				if (stime!=timestamp) {
					t.put("success-timestamp", String.valueOf(stime));
				}
				t.put("success-elapsed", String.valueOf(sync.getLastSuccessfulExecutionLapse()));
				if (sync.getLastException() != null) { 
					t.put("exception", sync.getLastException().toString());
				}
				ret.put(sync.getTask().getClass().getSimpleName(), t);
			}
			
		} catch (Throwable t) {
		}
		return ret;
	}

	
}
