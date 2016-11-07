/**
 *  Copyright (c) 2011-2015 Zauber S.A.  -- All rights reserved
 */

package io.tilt.minka.core.leader;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.joda.time.DateTime;

import com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.core.JsonGenerator.Feature;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.tilt.minka.domain.Shard;
import io.tilt.minka.domain.ShardEntity;

/**
 * Plain representation of the domain objects
 * 
 * @author Cristian Gonzalez
 * @since Nov 6, 2016
 */
public class Status {
	
	protected static final ObjectMapper mapper; 
	static {
		mapper = new ObjectMapper();
		mapper.setVisibility(PropertyAccessor.ALL, Visibility.NONE);
		mapper.setVisibility(PropertyAccessor.FIELD, Visibility.ANY);
		mapper.configure(Feature.WRITE_NUMBERS_AS_STRINGS, true);
	}
	
	protected final List<ShardRep> shards;
	protected final List<PalletRep> pallets;
	
	@JsonCreator
	public Status(List<ShardRep> shards, List<PalletRep> pallets) {
		super();
		this.shards = shards;
		this.pallets = pallets;
	}

	public static String toJson(final PartitionTable table) throws JsonProcessingException {
		return mapper.writeValueAsString(getStatus(table));
	}
	
	public static Status getStatus(final PartitionTable table) {
		final List<ShardRep> shardRep = buildShardRep(table);
		final List<PalletRep> pallets = new ArrayList<>();
		
		for (final ShardEntity pallet: table.getStage().getPallets()) {
			pallets.add(new PalletRep(pallet.getPallet().getId(), 
				table.getStage().getCapacityTotal(pallet.getPallet()), 
				table.getStage().getSizeTotal(pallet.getPallet()), 
				table.getStage().getWeightTotal(pallet.getPallet()), 
				pallet.getPallet().getStrategy().getBalancer().getSimpleName()));
		}
		
		return new Status(shardRep, pallets);
	}

	private static List<ShardRep> buildShardRep(final PartitionTable table) {
		final List<ShardRep> shards = new ArrayList<>();
		for (final Shard shard: table.getStage().getShards()) {
			final List<PalletShardRep> palletsAtShard =new ArrayList<>();
			
			for (final ShardEntity pallet: table.getStage().getPallets()) {
				final Set<ShardEntity> duties = table.getStage().getDutiesByShard(pallet.getPallet(), shard);
				final List<DutyRep> dutyRepList = new ArrayList<>();
				duties.forEach(d->dutyRepList.add(new DutyRep(d.getDuty().getId(), d.getDuty().getWeight())));
				
				palletsAtShard.add(new PalletShardRep(pallet.getPallet().getId(), 
						table.getStage().getCapacity(pallet.getPallet(), shard), 
					duties.size(), table.getStage().getWeight(pallet.getPallet(), shard), dutyRepList));
				
			}
			shards.add(new ShardRep(shard.getShardID().getSynthetizedID(), shard.getFirstTimeSeen(), 
					palletsAtShard, shard.getState().toString()));
		}
		return shards;
	}
	
	@JsonPropertyOrder({"host", "pallets", "creation", "status"})
	public static class ShardRep {
		public final String host;
		public final String creation;
		public final String status;
		public final List<PalletShardRep> pallets;
		
		@JsonCreator
		public ShardRep(String host, DateTime creation, List<PalletShardRep> pallets, String status) {
			super();
			this.host = host;
			this.creation = creation.toString();
			this.pallets = pallets;
			this.status = status;
		}
	}
	
	@JsonPropertyOrder({"id", "capacity", "size", "weight"})
	public static class PalletShardRep {
		public final String id;
		public final double capacity;
		public final int size;
		public final double weight;
		@JsonIgnore
		public final List<DutyRep> duties;
		public final String dutiesStr;
		public PalletShardRep(String id, double capacity, int size, double weight, List<DutyRep> duties) {
			super();
			this.id = id;
			this.capacity = capacity;
			this.size = size;
			this.weight = weight;
			this.duties = duties;
			StringBuilder sb = new StringBuilder();
			duties.forEach(d->sb.append(d.id).append(","));
			this.dutiesStr=sb.toString();
		}
	}
	
	public static class DutyRep {
		public final String id;
		public final double weight;
		public DutyRep(String id, double weight) {
			super();
			this.id = id;
			this.weight = weight;
		}
	}
	
	@JsonPropertyOrder({"id", "capacity", "size", "weight", "balancer"})
	public static class PalletRep {
		public final String id;
		public final double capacity;
		public final int size;
		public final double weight;
		public final String balancer;
		public PalletRep(String id, double capacity, int size, double weight, String balancer) {
			super();
			this.id = id;
			this.capacity = capacity;
			this.size = size;
			this.weight = weight;
			this.balancer = balancer;
		}
	}
}
