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
package io.tilt.minka.core.leader;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.lang.Validate;
import org.joda.time.DateTime;

import com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.core.JsonGenerator.Feature;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.google.common.util.concurrent.AtomicDouble;

import io.tilt.minka.api.Duty;
import io.tilt.minka.core.leader.distributor.Balancer.BalancerMetadata;
import io.tilt.minka.domain.EntityEvent;
import io.tilt.minka.domain.Shard;
import io.tilt.minka.domain.ShardEntity;
import io.tilt.minka.domain.ShardID;

/**
 * Plain representation of the domain objects
 * 
 * @author Cristian Gonzalez
 * @since Nov 6, 2016
 */
@SuppressWarnings("unused")
@JsonPropertyOrder({"global", "shards", "pallets", "roadmaps"})
public class Status {
	
	
	public static class Shards {
		private List<Shard> shards;
		private ShardID leaderShardId;
	
		public static String toJson(final PartitionTable table) throws JsonProcessingException {
			Validate.notNull(table);
			return mapper.writeValueAsString(build(table));
		}
		public static Shards build(final PartitionTable table) {
			Validate.notNull(table);
			return new Shards(table.getStage().getShards(), table.getLeaderShardContainer().getLeaderShardId());
		}
		private Shards(final List<Shard> shards, final ShardID leaderId) {
			this.shards = shards;
			this.leaderShardId = leaderId;
		}
	}
	
	
	
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
	
	protected final List<ShardView> shards;
	protected final Global global;
	
	@JsonCreator
	private Status(final List<ShardView> shards, final Global global) {
		super();
		this.shards = shards;
		this.global = global;
	}

	public static String elementToJson(final Object o) throws JsonProcessingException {
		Validate.notNull(o);
		return mapper.writeValueAsString(o);
	}
	
	public static String toJson(final PartitionTable table) throws JsonProcessingException {
		Validate.notNull(table);
		return mapper.writeValueAsString(build(table));
	}
	
	public static Status build(final PartitionTable table) {
		Validate.notNull(table);
		return new Status(
				buildShardRep(table), 
				buildGlobal(table));
	}

	public static String dutiesToJson(final PartitionTable table) throws JsonProcessingException {
		return elementToJson(buildDuties(table));
	}
	private static List<Duty<?>> buildDuties(final PartitionTable table) {
		Validate.notNull(table);
		final List<Duty<?>> ret = new ArrayList<>();
		table.getStage().getDuties().forEach(e->ret.add(e.getDuty()));
		return ret;
	}

	public static String palletsToJson(PartitionTable table) throws JsonProcessingException {
		Validate.notNull(table);
		return elementToJson(buildPallets(table));
	}
		
	private static List<PalletView> buildPallets(final PartitionTable table) {
		final List<PalletView> ret = new ArrayList<>();
		
		for (final ShardEntity pallet: table.getStage().getPallets()) {
			final Set<ShardEntity> crud = table.getNextStage().getDutiesCrudWithFilters(EntityEvent.CREATE, 
					ShardEntity.State.PREPARED).stream()
					.filter(d->d.getDuty().getPalletId().equals(pallet.getPallet().getId()))
					.collect(Collectors.toSet());
								
			AtomicDouble dettachedWeight = new AtomicDouble();
			crud.forEach(d->dettachedWeight.addAndGet(d.getDuty().getWeight()));

			final List<DutyView> dutyRepList = new ArrayList<>();
			table.getStage().getDutiesByPallet(pallet.getPallet()).forEach(d->dutyRepList.add(new DutyView(d.getDuty().getId(), d.getDuty().getWeight())));
			
			ret.add(new PalletView(pallet.getPallet().getId(), 
						table.getStage().getCapacityTotal(pallet.getPallet()), 
						table.getStage().getSizeTotal(pallet.getPallet()), 
						table.getStage().getWeightTotal(pallet.getPallet()), 
						pallet.getPallet().getMetadata().getBalancer().getName(), 
						crud.size(), 
						dettachedWeight.get(), 
						pallet.getEventDateForPartition(EntityEvent.CREATE),
						pallet.getPallet().getMetadata(),
						dutyRepList
					));
		}
		return ret;
	}

	private static List<ShardView> buildShardRep(final PartitionTable table) {
		final List<ShardView> ret = new ArrayList<>();
		for (final Shard shard: table.getStage().getShards()) {
			final List<PalletAtShardView> palletsAtShard =new ArrayList<>();
			
			for (final ShardEntity pallet: table.getStage().getPallets()) {
				final Set<ShardEntity> duties = table.getStage().getDutiesByShard(pallet.getPallet(), shard);
				final List<DutyView> dutyRepList = new ArrayList<>();
				duties.forEach(d->dutyRepList.add(new DutyView(d.getDuty().getId(), d.getDuty().getWeight())));
				palletsAtShard.add(new PalletAtShardView(pallet.getPallet().getId(), 
						table.getStage().getCapacity(pallet.getPallet(), shard), 
					duties.size(), table.getStage().getWeight(pallet.getPallet(), shard), dutyRepList));
				
			}
			ret.add(new ShardView(shard.getShardID().getSynthetizedID(), shard.getFirstTimeSeen(), 
					palletsAtShard, shard.getState().toString()));
		}
		return ret;
	}

	private static Global buildGlobal(final PartitionTable table) {
		final int unstaged = table.getNextStage().getDutiesCrudWithFilters(EntityEvent.CREATE, null).size();
		final int staged = table.getStage().getSizeTotal();
		return new Global(staged +unstaged, staged, unstaged, table.getStage().getPallets().size(), 
				table.getStage().getShards().size());
	}
	
	public static class Global {
		@JsonPropertyOrder({"shard-size", "duty-size", "pallet-size", "staged-size", "unstaged-size"})
		@JsonProperty(index=1, value="shard-size") private final int shardSize;
		@JsonProperty(index=2, value="duty-size") private final int dutySize;
		@JsonProperty(index=3, value="pallet-size") private final int palletSize;
		@JsonProperty(index=4, value="staged-size") private final int stagedSize;
		@JsonProperty(index=5, value="unstaged-size") private final int unstagedSize;
		protected Global(final int dutySize, final int stagedSize, final int unstagedSize, final int palletSize, final int shardSize) {
			super();
			this.dutySize = dutySize;
			this.unstagedSize = unstagedSize;
			this.stagedSize = stagedSize;
			this.palletSize = palletSize;
			this.shardSize = shardSize;
		}
	}
	
	public static class ShardView {
		@JsonPropertyOrder({"host", "creation", "status", "pallets"})
		private final String host;
		private final String creation;
		private final String status;
		private final List<PalletAtShardView> pallets;
		
		@JsonCreator
		protected ShardView(final String host, final DateTime creation, final List<PalletAtShardView> pallets, final String status) {
			super();
			this.host = host;
			this.creation = creation.toString();
			this.pallets = pallets;
			this.status = status;
		}
	}
	
	
	public static class PalletAtShardView {
		@JsonPropertyOrder({"id", "size", "capacity", "weight", "duties"})
		@JsonProperty(index=1, value ="id") private final String id;
		@JsonProperty(index=2, value ="size") private final int size;
		@JsonProperty(index=3, value ="capacity") private final double capacity;
		@JsonProperty(index=4, value ="weight") private final double weight;
		@JsonProperty(index=5, value ="duties") private final String duties;
		protected PalletAtShardView(final String id, final double capacity, final int size, final double weight, 
				final List<DutyView> duties) {
			super();
			this.id = id;
			this.capacity = capacity;
			this.size = size;
			this.weight = weight;
			StringBuilder sb = new StringBuilder();
			duties.forEach(d->sb.append(d.id).append(":").append(d.weight).append(","));
			this.duties=sb.toString();
		}
	}
	
	public static class DutyView {
		private final String id;
		private final double weight;
		protected DutyView(final String id, final double weight) {
			super();
			this.id = id;
			this.weight = weight;
		}
	}
	
	public static class PalletView {
		@JsonPropertyOrder({"id", "creation", "cluster-capacity", "allocation", "staged-size", 
			"staged-weight", "unstaged-size", "unstaged-weight", "balancer", "balancer-metadata"})	@JsonProperty(index=1, value ="id") private final String id;
		@JsonProperty(index=2, value ="creation") private final String creation;
		@JsonProperty(index=3, value ="size") private final int size;
		@JsonProperty(index=4, value ="cluster-capacity") private final double capacity;
		@JsonProperty(index=5, value ="allocation") private final String allocationPercent;
		@JsonProperty(index=6, value ="staged-size") private final int stagedSize;
		@JsonProperty(index=7, value ="staged-weight") private final double stagedWeight;
		@JsonProperty(index=8, value ="unstaged-size") private final int unstagedSize;
		@JsonProperty(index=9, value ="unstaged-weight") private final double unstagedWeight;
		@JsonProperty(index=10, value ="balancer-metadata") private final BalancerMetadata meta;		
		@JsonProperty(index=11, value ="balancer") private final String balancer;
		@JsonProperty(index=12, value ="duties") private final String duties;
		protected PalletView(final String id, final double capacity, final int stagedSize, final double stagedWeight, 
				final String balancer, final int unstagedSize, final double unstagedWeight, final DateTime creation, 
				final BalancerMetadata meta, final List<DutyView> duties) {
			super();
			this.id = id;
			this.capacity = capacity;
			this.stagedSize = stagedSize;
			this.stagedWeight = stagedWeight;
			this.balancer = balancer;
			this.unstagedSize = unstagedSize;
			this.unstagedWeight = unstagedWeight;
			this.size = unstagedSize + stagedSize;
			String str = "0";
			if (size > 0 && stagedSize > 0) {
				str = String.valueOf((stagedSize*100) / size) ;
			}
			this.allocationPercent = str + "%";
			this.creation = creation.toString();
			this.meta = meta;
			StringBuilder sb = new StringBuilder();
			duties.forEach(d->sb.append(d.id).append(":").append(d.weight).append(","));
			this.duties=sb.toString();
		}
	}
}
