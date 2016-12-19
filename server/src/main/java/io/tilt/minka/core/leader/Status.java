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

import io.tilt.minka.core.leader.distributor.Balancer.BalancerMetadata;
import io.tilt.minka.core.leader.distributor.Roadmap;
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
	
	protected final List<ShardRep> shards;
	protected final List<PalletRep> pallets;
	protected final Global global;
	protected final List<Roadmap> roadmaps;
	
	@JsonCreator
	private Status(final List<ShardRep> shards, final List<PalletRep> pallets, final Global global, 
			final List<Roadmap> roadmaps) {
		super();
		this.shards = shards;
		this.pallets = pallets;
		this.global = global;
		this.roadmaps = roadmaps;
	}

	public static String toJson(final PartitionTable table) throws JsonProcessingException {
		Validate.notNull(table);
		return mapper.writeValueAsString(build(table));
	}
	
	public static Status build(final PartitionTable table) {
		Validate.notNull(table);
		return new Status(
				buildShardRep(table), 
				buildPallets(table), 
				buildGlobal(table), 
				table.getHistory());
	}

	private static List<PalletRep> buildPallets(final PartitionTable table) {
		final List<PalletRep> ret = new ArrayList<>();
		
		for (final ShardEntity pallet: table.getStage().getPallets()) {
			final Set<ShardEntity> crud = table.getNextStage().getDutiesCrudWithFilters(EntityEvent.CREATE, 
					ShardEntity.State.PREPARED).stream()
					.filter(d->d.getDuty().getPalletId().equals(pallet.getPallet().getId()))
					.collect(Collectors.toSet());
								
			AtomicDouble dettachedWeight = new AtomicDouble();
			crud.forEach(d->dettachedWeight.addAndGet(d.getDuty().getWeight()));
			
			ret.add(new PalletRep(pallet.getPallet().getId(), 
				table.getStage().getCapacityTotal(pallet.getPallet()), 
				table.getStage().getSizeTotal(pallet.getPallet()), 
				table.getStage().getWeightTotal(pallet.getPallet()), 
				pallet.getPallet().getMetadata().getBalancer().getName(), 
				crud.size(), dettachedWeight.get(), pallet.getEventDateForPartition(EntityEvent.CREATE),
						pallet.getPallet().getMetadata()));
		}
		return ret;
	}

	private static List<ShardRep> buildShardRep(final PartitionTable table) {
		final List<ShardRep> ret = new ArrayList<>();
		for (final Shard shard: table.getStage().getShards()) {
			final List<PalletAtShardRep> palletsAtShard =new ArrayList<>();
			
			for (final ShardEntity pallet: table.getStage().getPallets()) {
				final Set<ShardEntity> duties = table.getStage().getDutiesByShard(pallet.getPallet(), shard);
				final List<DutyRep> dutyRepList = new ArrayList<>();
				duties.forEach(d->dutyRepList.add(new DutyRep(d.getDuty().getId(), d.getDuty().getWeight())));
				palletsAtShard.add(new PalletAtShardRep(pallet.getPallet().getId(), 
						table.getStage().getCapacity(pallet.getPallet(), shard), 
					duties.size(), table.getStage().getWeight(pallet.getPallet(), shard), dutyRepList));
				
			}
			ret.add(new ShardRep(shard.getShardID().getSynthetizedID(), shard.getFirstTimeSeen(), 
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
	
	public static class ShardRep {
		@JsonPropertyOrder({"host", "creation", "status", "pallets"})
		private final String host;
		private final String creation;
		private final String status;
		private final List<PalletAtShardRep> pallets;
		
		@JsonCreator
		protected ShardRep(final String host, final DateTime creation, final List<PalletAtShardRep> pallets, final String status) {
			super();
			this.host = host;
			this.creation = creation.toString();
			this.pallets = pallets;
			this.status = status;
		}
	}
	
	
	public static class PalletAtShardRep {
		@JsonPropertyOrder({"id", "size", "capacity", "weight", "duties"})
		@JsonProperty(index=1, value ="id") private final String id;
		@JsonProperty(index=2, value ="size") private final int size;
		@JsonProperty(index=3, value ="capacity") private final double capacity;
		@JsonProperty(index=4, value ="weight") private final double weight;
		@JsonProperty(index=5, value ="duties") private final String duties;
		protected PalletAtShardRep(final String id, final double capacity, final int size, final double weight, 
				final List<DutyRep> duties) {
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
	
	public static class DutyRep {
		private final String id;
		private final double weight;
		protected DutyRep(final String id, final double weight) {
			super();
			this.id = id;
			this.weight = weight;
		}
	}
	
	public static class PalletRep {
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
		protected PalletRep(final String id, final double capacity, final int stagedSize, final double stagedWeight, 
				final String balancer, final int unstagedSize, final double unstagedWeight, final DateTime creation, 
				final BalancerMetadata meta) {
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
				str = String.valueOf((size * stagedSize) / 100) ;
			}
			this.allocationPercent = str + "%";
			this.creation = creation.toString();
			this.meta = meta;
		}
	}
}
