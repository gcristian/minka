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
import static java.util.stream.Collectors.toList;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import org.apache.commons.lang.Validate;
import org.joda.time.DateTime;
import org.json.JSONArray;
import org.json.JSONObject;

import io.tilt.minka.api.Config;
import io.tilt.minka.core.leader.balancer.Spot;
import io.tilt.minka.core.leader.balancer.Balancer.BalancerMetadata;
import io.tilt.minka.core.leader.data.CommitedState;
import io.tilt.minka.core.leader.data.Scheme;
import io.tilt.minka.core.leader.data.UncommitedChanges;
import io.tilt.minka.core.leader.distributor.ChangePlan;
import io.tilt.minka.core.task.LeaderAware;
import io.tilt.minka.domain.EntityEvent;
import io.tilt.minka.domain.EntityState;
import io.tilt.minka.domain.ShardEntity;
import io.tilt.minka.domain.ShardedPartition;
import io.tilt.minka.shard.Shard;
import io.tilt.minka.utils.CollectionUtils;
import io.tilt.minka.utils.CollectionUtils.SlidingSortedSet;

/**
 * Read only views built at request about the system state  
 * JSON format.
 * 
 * @author Cristian Gonzalez
 * @since Nov 6, 2016
 */
public class LeaderMonitor {

	private final LeaderAware leaderAware;
	private final Config config;
	
	private final Scheme scheme;
	private final ShardedPartition partition;
	
	private final ChangePlan[] lastPlan = {null};
	private final SlidingSortedSet<String> changePlanHistory;
	private final SlidingSortedSet<Long> planids;
	private long planAccum;

	public LeaderMonitor(
			final LeaderAware leaderAware,
			final Config config,
			final Scheme scheme,
			final ShardedPartition partition) {
		super();
		
		this.leaderAware = requireNonNull(leaderAware); 
		this.config = requireNonNull(config);
		this.scheme = requireNonNull(scheme);
		this.partition = requireNonNull(partition);
		
		this.changePlanHistory = CollectionUtils.sliding(20);
		this.planids = CollectionUtils.sliding(10);
		
		scheme.addChangeObserver(()-> {
			try {
				if (lastPlan[0]!=null) {
					if (!lastPlan[0].equals(scheme.getCurrentPlan())) {
						changePlanHistory.add(SystemStateMonitor.toJson(lastPlan[0]));
						planids.add(lastPlan[0].getId());
						planAccum++;
					}
				}
				lastPlan[0] = scheme.getCurrentPlan();
			} catch (Exception e) {
			}
		});
	}


	/**
	 * <p>
	 * Gives detailed information on the shards, members of the cluster within the namespace, reporting to the leader.
	 * <p>
	 * Full when the current server is the Leader, self shard info. when the server is a Follower.
	 * @return			a String in json format
	 */
	public String shardsToJson()  {
		return SystemStateMonitor.toJson(buildShards(scheme));
	}
	
	/**
	 * <p>
	 * Gives detailed information on change plans built in distributor phase
	 * to accomodate duties into shards. Including any on-going plan, and historic plans.
	 * <p>
	 * Non-Empty only when the current server is the Leader.
	 * @return			a String in json format
	 */
	public String plansToJson(final boolean detail) {
		return SystemStateMonitor.toJson(buildPlans(detail).toMap());
	}

	/**
	 * <p>
	 * Gives detailed information on distributed duties and their shard locations
	 * <p>
	 * Non-Empty only when the current server is the Leader.
	 * @return			a String in json format
	 */
	public String distributionToJson() {
		return SystemStateMonitor.toJson(buildDistribution());
	}

	public String palletsToJson() {
		return SystemStateMonitor.toJson(buildPallets());
	}

	/**
	 * 
	 * <p>
	 * Non-Empty only when the current server is the Leader.
	 * @return			a String in json format
	 */
	public String schemeToJson(final boolean detail) {
		Map<String, Object> m = new LinkedHashMap<>(2);
		m.put("commited", buildCommitedState(detail));
		m.put("replicas", buildReplicas(detail));
		m.put("uncommited", buildUncommitedChanges(detail, scheme.getUncommited()));
		return SystemStateMonitor.toJson(m);
	}
	
	private JSONObject buildPlans(final boolean detail) {
		final JSONObject js = new JSONObject();
		try {
			js.put("total", planAccum);
			js.put("retention	", changePlanHistory.size() + (lastPlan[0]!=null ? 1 : 0));
			js.put("ids", this.planids.values());
			if (lastPlan[0]!=null) {
				js.put("last", new JSONObject(SystemStateMonitor.toJson(lastPlan[0])));
			}
			
			final JSONArray arr = new JSONArray();
			final Iterator<String> it = changePlanHistory.descend();
			while(it.hasNext()) {
				final String s = it.next();
				final JSONObject jsono = new JSONObject(s);
				if (!detail) {
					jsono.remove("deliveries");
				}
				arr.put(jsono);
			}
			js.put("closed", arr);
			
		} catch (Exception e) {
		}
		return js;
	}

	private Map<String, Object> buildShards(final Scheme scheme) {
		final Map<String, Object> map = new LinkedHashMap<>(5);
		map.put("namespace", config.getBootstrap().getNamespace());
		map.put("leader", leaderAware.getLeaderShardId());
		map.put("previous", leaderAware.getAllPreviousLeaders());
		final Map<String, List<String>> tmp = new HashMap<>();
		scheme.getCommitedState().getGoneShards().entrySet().forEach(e-> {
			tmp.put(e.getKey().getId(), e.getValue().values().stream().map(c->c.toString()).collect(toList()));
		});
		map.put("gone", tmp);
		final List<Shard> list = new ArrayList<>();
		scheme.getCommitedState().findShards(null, list::add);
		map.put("shards", list);
		return map;
	}
	
	public Map<String, Object> buildDistribution() {
		final Map<String, Object> map = new LinkedHashMap<>(2);
		map.put("global", buildGlobal(scheme));
		map.put("distribution", buildShardRep(scheme));
		return map;
	}
	
	private Map<String, List<Object>> buildCommitedState(final boolean detail) {
		Validate.notNull(scheme);
		final Map<String, List<Object>> byPalletId = new LinkedHashMap<>();
		final Consumer<ShardEntity> adder = addler(detail, byPalletId);
		scheme.getCommitedState().findDuties(adder);
		return byPalletId;
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
			pid.add(detail ? d : d.getDuty().getId(	));
		};
		return adder;
	}

	private List<Object> dutyBrief(final Collection<ShardEntity> coll, final boolean detail) {
		List<Object> ret = new ArrayList<>();
		if (!detail) {
			coll.stream()
				.map(d->d.getDuty().getId())
				.forEach(ret::add);
		} else {
			coll.forEach(ret::add);
		}
		return ret;
	}
	
	private Map<String, List<Object>> buildUncommitedChanges(final boolean detail, final UncommitedChanges uncommitedChanges) {
		final Map<String, List<Object>> ret = new LinkedHashMap<>(3);		
		ret.put("crud", dutyBrief(uncommitedChanges.getDutiesCrud(), detail));
		ret.put("dangling", dutyBrief(uncommitedChanges.getDutiesDangling(), detail));
		ret.put("missing", dutyBrief(uncommitedChanges.getDutiesMissing(), detail));
		return ret;
	}

	private List<Map<String, Object>> buildPallets() {
		
		final CommitedState.SchemeExtractor extractor = new CommitedState.SchemeExtractor(scheme.getCommitedState());
		final List<Map<String, Object>> ret = new ArrayList<>(extractor.getPallets().size());
		for (final ShardEntity pallet: extractor.getPallets()) {
			
			final double[] dettachedWeight = {0};
			final int[] crudSize = new int[1];
			scheme.getUncommited().findDutiesCrud(EntityEvent.CREATE::equals, EntityState.PREPARED::equals, e-> {
				if (e.getDuty().getPalletId().equals(pallet.getPallet().getId())) {
					crudSize[0]++;
					dettachedWeight[0]+=e.getDuty().getWeight();
				}
			});
								

			final List<DutyView> dutyRepList = new ArrayList<>();
			scheme.getCommitedState().findDutiesByPallet(pallet.getPallet(), 
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
						new DateTime(pallet.getCommitTree().getFirst().getHead().getTime()),
						pallet.getPallet().getMetadata(),
						dutyRepList
					));
		}
		return ret;
	}

	private static List<Map<String, Object>> buildShardRep(final Scheme table) {	    
		final List<Map<String, Object>> ret = new LinkedList<>();
		final CommitedState.SchemeExtractor extractor = new CommitedState.SchemeExtractor(table.getCommitedState());
		for (final Shard shard : extractor.getShards()) {
			final List<Map<String , Object>> palletsAtShard =new LinkedList<>();
			
			for (final ShardEntity pallet: extractor.getPallets()) {
				final List<DutyView> dutyRepList = new LinkedList<>();
				final List<DutyView> repliRepList = new LinkedList<>();
				int[] size = new int[1];
				table.getCommitedState().findShards(sh->sh.equals(shard),  sh-> {
					for (ShardEntity se: table.getCommitedState().getReplicasByShard(sh)) {
						if (se.getDuty().getPalletId().equals(pallet.getPallet().getId())) {
							repliRepList.add(new DutyView(se.getDuty().getId(), se.getDuty().getWeight()));
						}
					}
				});
				
				table.getCommitedState().findDuties(shard, pallet.getPallet(), d-> {
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
								dutyRepList, repliRepList));
				
			}
			ret.add(shardView(
					shard.getShardID().getId() + " (" + shard.getShardID().getTag() + ")",
					shard.getFirstTimeSeen(),
					palletsAtShard, 
					shard.getState().toString()));
		}
		return ret;
	}

	private static Map<String, Object> buildGlobal(final Scheme table) {
		CommitedState.SchemeExtractor extractor = new CommitedState.SchemeExtractor(table.getCommitedState());
		final Map<String, Object> map = new LinkedHashMap<>(8);
		map.put("size-shards", extractor.getShards().size());
		map.put("size-pallets", extractor.getPallets().size());
		map.put("size-scheme", extractor.getSizeTotal());
		map.put("size-crud", table.getUncommited().getDutiesCrud().size());
		map.put("size-missings", table.getUncommited().getDutiesMissing().size());
		map.put("size-dangling", table.getUncommited().getDutiesDangling().size());
		map.put("uncommited-change", table.getUncommited().isStealthChange());
		map.put("commited-change", table.getCommitedState().isStealthChange());
		return map;
	}
	
	
	private static Map<String, Object> shardView(
			final String shardId, 
			final Instant creation, 
			final List<Map<String, Object>> pallets, 
			final String status) {
			
		final Map<String, Object> map = new LinkedHashMap<>(4);
		map.put("shard-id", shardId);
		map.put("creation", creation.toString());
		map.put("status", status);
		map.put("pallets", pallets);
		return map;
	}
	
	
	private static Map<String , Object> palletAtShardView (
			final String id, final double capacity, final int size, final double weight, 
			final List<DutyView> duties, final List<DutyView> replicas) {

		final Map<String , Object> map = new LinkedHashMap<>(5);
		map.put("id", id);
		map.put("size", size);
		map.put("capacity", capacity);
		map.put("weight", weight);
		
		final StringBuilder sb = new StringBuilder(duties.size() * (5 + 2));
		duties.forEach(d->sb.append(d.id).append(", "));
		map.put("duties",sb.toString());
		
		final StringBuilder sb2 = new StringBuilder(replicas.size() * (5 + 2));
		replicas.forEach(d->sb2.append(d.id).append(", "));
		map.put("replicas",sb2.toString());
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
		
		final Map<String, Object> map = new LinkedHashMap<>(12);
		map.put("id", id);
		map.put("size", unstagedSize + stagedSize);
		map.put("cluster-capacity", capacity);
		map.put("size-commited", stagedSize);
		map.put("size-uncommited", unstagedSize);
		map.put("weight-committed", stagedWeight);
		map.put("weight-uncommited", unstagedWeight);
		String str = "0";
		if (unstagedSize + stagedSize > 0 && stagedSize > 0) {
			str = String.valueOf((stagedSize*100) / (unstagedSize + stagedSize)) ;
		}
		map.put("allocation", str + "%");
		map.put("creation", creation.toString());
		map.put("balancer-metadata", meta); 
		map.put("balancer", balancer);
		StringBuilder sb = new StringBuilder(duties.size() * (5 + 2 + 5));
		duties.forEach(d->sb.append(d.id).append(":").append(d.weight).append(","));
		map.put("duties", sb.toString());
		return map;
	}
	
	
}
