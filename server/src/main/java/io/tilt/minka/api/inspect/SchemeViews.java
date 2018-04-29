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

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.stream.Collectors;

import org.apache.commons.lang.Validate;
import org.joda.time.DateTime;
import org.springframework.beans.factory.annotation.Autowired;

import com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.core.JsonGenerator.Feature;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.google.common.util.concurrent.AtomicDouble;

import io.tilt.minka.core.leader.PartitionScheme;
import io.tilt.minka.core.leader.PartitionScheme.Scheme;
import io.tilt.minka.core.leader.PartitionScheme.Scheme.SchemeExtractor;
import io.tilt.minka.core.leader.balancer.Balancer.BalancerMetadata;
import io.tilt.minka.core.task.LeaderShardContainer;
import io.tilt.minka.core.task.Scheduler;
import io.tilt.minka.core.task.Scheduler.Agent;
import io.tilt.minka.core.task.Scheduler.Synchronized;
import io.tilt.minka.core.task.Semaphore;
import io.tilt.minka.domain.EntityEvent;
import io.tilt.minka.domain.Shard;
import io.tilt.minka.domain.ShardEntity;
import io.tilt.minka.domain.ShardedPartition;
import io.tilt.minka.domain.EntityState;

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
		
	protected static final ObjectMapper mapper; 
	static {
		mapper = new ObjectMapper();
		mapper.setVisibility(PropertyAccessor.ALL, Visibility.NONE);
		mapper.setVisibility(PropertyAccessor.FIELD, Visibility.ANY);
		mapper.configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, true);
		mapper.configure(SerializationFeature.FAIL_ON_SELF_REFERENCES, false);
		mapper.configure(SerializationFeature.WRITE_SINGLE_ELEM_ARRAYS_UNWRAPPED, false);
		mapper.configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);
		mapper.configure(Feature.WRITE_NUMBERS_AS_STRINGS, true);
	}

	public SchemeViews(final LeaderShardContainer leaderShardContainer) {
		this.leaderShardContainer = requireNonNull(leaderShardContainer);
	}

	public String elementToJson(final Object o) throws JsonProcessingException {
		Validate.notNull(o);
		return mapper.writeValueAsString(o);
	}

	public String shardsToJson(final PartitionScheme table) throws JsonProcessingException {
		Validate.notNull(table);
		return mapper.writeValueAsString(buildShards(table));
	}

	private Map<String, Object> buildShards(final PartitionScheme table) {
		Validate.notNull(table);
		final Map<String, Object> map = new LinkedHashMap<>();
		map.put("leaderShardId", leaderShardContainer.getLeaderShardId());
		map.put("previousLeaders", leaderShardContainer.getAllPreviousLeaders());
		final List<Shard> list = new ArrayList<>();
		table.getScheme().onShards(null, list::add);
		map.put("shards", list);
		return map;
	}

	public String distributionToJson(final PartitionScheme table) throws JsonProcessingException {
		Validate.notNull(table);
		return mapper.writeValueAsString(buildDistribution(table));
	}
	
	public Map<String, Object> buildDistribution(final PartitionScheme table) {
		Validate.notNull(table);
		final Map<String, Object> map = new LinkedHashMap<>();
		map.put("global", buildGlobal(table));
		map.put("distribution", buildShardRep(table));
		return map;
	}

	public String dutiesToJson(final PartitionScheme table) throws JsonProcessingException {
		return elementToJson(buildDuties(table, false));
	}

	public String entitiesToJson(final PartitionScheme table) throws JsonProcessingException {
		Map<String, Object> m = new HashMap<>();
		m.put("scheme", buildDuties(table, true));
		m.put("backstage", buildBackstage(table.getBackstage()));
		return elementToJson(m);
	}
	public String shardedEntitiesToJson(final ShardedPartition partition) throws JsonProcessingException {
		return elementToJson(buildDuties(partition, true));
	}
	private Map<String, List<Object>> buildDuties(final PartitionScheme table, boolean entities) {
		Validate.notNull(table);
		final Map<String, List<Object>> byPalletId = new LinkedHashMap<>();
		table.getScheme().onDuties(d-> {
			List<Object> pid = byPalletId.get(d.getDuty().getPalletId());
			if (pid==null) {
				byPalletId.put(d.getDuty().getPalletId(), pid = new ArrayList<>());
			}
			pid.add(entities ? d : d.getDuty().getId());
		});
		return byPalletId;
	}

	private Map<String, List<Object>> buildBackstage(final PartitionScheme.Backstage stage) {
		Validate.notNull(stage);
		List<Object> ret = new ArrayList<>();
		final Map<String, List<Object>> m = new HashMap<>();
		stage.getDutiesCrud().forEach(ret::add);
		m.put("crud", ret);
		ret = new ArrayList<>();
		stage.getDutiesDangling().forEach(ret::add);
		m.put("dangling", ret);
		ret = new ArrayList<>();
		stage.getDutiesMissing().forEach(ret::add);
		m.put("missing", ret);
		return m;
	}

	private List<Object> buildDuties(final ShardedPartition partition, boolean entities) {
		Validate.notNull(partition);
		final List<Object> ret = new ArrayList<>();
		for (ShardEntity e: partition.getDuties()) {
			ret.add(entities ? e: e.getDuty());
		}
		return ret;
	}

	public String palletsToJson(PartitionScheme table) throws JsonProcessingException {
		Validate.notNull(table);
		return elementToJson(buildPallets(table));
	}
		
	private static List<Map<String, Object>> buildPallets(final PartitionScheme table) {
		final List<Map<String, Object>> ret = new ArrayList<>();
		
		final SchemeExtractor extractor = new SchemeExtractor(table.getScheme());
		
		for (final ShardEntity pallet: extractor.getPallets()) {
			
			AtomicDouble dettachedWeight = new AtomicDouble();
			final int[] crudSize = new int[1];
			table.getBackstage().onDutiesCrud(EntityEvent.CREATE::equals, EntityState.PREPARED::equals, e-> {
				if (e.getDuty().getPalletId().equals(pallet.getPallet().getId())) {
					crudSize[0]++;
					dettachedWeight.addAndGet(e.getDuty().getWeight());
				}
			});
								

			final List<DutyView> dutyRepList = new ArrayList<>();
			table.getScheme().onDutiesByPallet(pallet.getPallet(), 
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
						dettachedWeight.get(), 
						new DateTime(pallet.getJournal().getFirst().getHead()),
						pallet.getPallet().getMetadata(),
						dutyRepList
					));
		}
		return ret;
	}

	private static List<Map<String, Object>> buildShardRep(final PartitionScheme table) {	    
		final List<Map<String, Object>> ret = new LinkedList<>();
		final SchemeExtractor extractor = new SchemeExtractor(table.getScheme());
		for (final Shard shard : extractor.getShards()) {
			final List<Map<String , Object>> palletsAtShard =new LinkedList<>();
			
			for (final ShardEntity pallet: extractor.getPallets()) {
				final List<DutyView> dutyRepList = new LinkedList<>();
				int[] size = new int[1];
				table.getScheme().onDuties(shard, pallet.getPallet(), d-> {
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

	private static Map<String, Object> buildGlobal(final PartitionScheme table) {
		SchemeExtractor extractor = new SchemeExtractor(table.getScheme());
		final int unstaged = table.getBackstage().sizeDutiesCrud(ee->ee==EntityEvent.CREATE, null) 
				+ table.getBackstage().getDutiesDangling().size() 
				+ table.getBackstage().getDutiesMissing().size();
		final int staged = extractor.getSizeTotal();		
		final Map<String, Object> map = new LinkedHashMap<>();
		map.put("size-duties", staged+unstaged);
		map.put("size-pallets", extractor.getPallets().size());
		map.put("size-staged", staged);
		map.put("size-unstaged", unstaged);
		map.put("size-shards", extractor.getShards().size());
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
		return elementToJson(getDetail(schedule));
				
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
				t.put("action", sync.getAction().name());
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
