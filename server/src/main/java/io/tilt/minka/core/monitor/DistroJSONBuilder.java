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
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.joda.time.DateTime;
import org.json.JSONArray;
import org.json.JSONObject;

import io.tilt.minka.api.Config;
import io.tilt.minka.core.leader.balancer.Balancer.BalancerMetadata;
import io.tilt.minka.core.leader.data.CommittedState;
import io.tilt.minka.core.leader.data.Scheme;
import io.tilt.minka.core.leader.distributor.ChangePlan;
import io.tilt.minka.core.task.LeaderAware;
import io.tilt.minka.domain.EntityEvent;
import io.tilt.minka.domain.EntityState;
import io.tilt.minka.domain.ShardEntity;
import io.tilt.minka.shard.NetworkShardIdentifier;
import io.tilt.minka.shard.Shard;
import io.tilt.minka.utils.CollectionUtils;
import io.tilt.minka.utils.CollectionUtils.SlidingMap;
import io.tilt.minka.utils.CollectionUtils.SlidingSortedSet;

/**
 * Read only views built at request about the system state  
 * JSON format.
 * 
 * @author Cristian Gonzalez
 * @since Nov 6, 2016
 */
public class DistroJSONBuilder {

	private final LeaderAware leaderAware;
	private final Config config;
	
	private final Scheme scheme;
	
	private final ChangePlan[] lastPlan = {null};
	private final SlidingSortedSet<String> changePlanHistory;
	private final SlidingMap<Integer, Long> planids;
	private long planAccum;

	public DistroJSONBuilder(
			final LeaderAware leaderAware,
			final Config config,
			final Scheme scheme) {
		super();
		
		this.leaderAware = requireNonNull(leaderAware); 
		this.config = requireNonNull(config);
		this.scheme = requireNonNull(scheme);
		
		this.changePlanHistory = CollectionUtils.sliding(20);
		this.planids = CollectionUtils.slidingMap(10);
		
		scheme.addChangeObserver(()-> {
			try {
				if (lastPlan[0]!=null) {
					if (!lastPlan[0].equals(scheme.getCurrentPlan())) {
						changePlanHistory.add(SystemStateMonitor.toJson(lastPlan[0]));
						planids.put(lastPlan[0].getVersion(), lastPlan[0].getId());
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
	 * Full when the current server is the LeaderBootstrap, self shard info. when the server is a FollowerBootstrap.
	 * @return			a String in json format
	 */
	public String shardsToJson()  {
		return SystemStateMonitor.toJson(buildShards(scheme));
	}
	
	private Map<String, Object> buildShards(final Scheme scheme) {
		final Map<String, Object> map = new LinkedHashMap<>(5);
		
		map.put("namespace", config.getBootstrap().getNamespace());
		map.put("leader", leaderAware.getLeaderShardId());
		
		final List<Shard> list = new ArrayList<>();
		scheme.getCommitedState().findShards(null, list::add);
		map.put("shards", list);
		
		final Map<String, List<String>> tmp = new HashMap<>();
		scheme.getVault().getGoneShards().entrySet().forEach(e-> {
			tmp.put(e.getKey().getId(), 
					e.getValue().values().stream()
						.map(c->c.toString()).collect(toList()));
		});
		map.put("gone", tmp);
		map.put("previous-leader", leaderAware.getAllPreviousLeaders());
		return map;
	}
	
	/**
	 * <p>
	 * Gives detailed information on change plans built in distributor phase
	 * to accomodate duties into shards. Including any on-going plan, and historic plans.
	 * <p>
	 * Non-Empty only when the current server is the LeaderBootstrap.
	 * @return			a String in json format
	 */
	public String plansToJson(final boolean detail) {
		return SystemStateMonitor.toJson(buildPlans(detail).toMap());
	}
	
	private JSONObject buildPlans(final boolean detail) {
		final JSONObject js = new JSONObject();
		try {
			js.put("total", planAccum);
			js.put("retention", changePlanHistory.size() + (lastPlan[0]!=null ? 1 : 0));
			js.put("vids", this.planids.map());
			if (lastPlan[0]!=null) {
				js.put("last", new JSONObject(SystemStateMonitor.toJson(lastPlan[0])));
			}
			
			final JSONArray arr = new JSONArray();
			final Iterator<String> it = changePlanHistory.descend();
			while(it.hasNext()) {
				final String s = it.next();
				final JSONObject jsono = new JSONObject(s);
				if (!detail) {
					jsono.remove(ChangePlan.FIELD_DISPATCHES);
				}
				arr.put(jsono);
			}
			js.put("closed", arr);
			
		} catch (Exception e) {
		}
		return js;
	}

	/**
	 * <p>
	 * Gives detailed information on distributed duties and their shard locations
	 * <p>
	 * Non-Empty only when the current server is the LeaderBootstrap.
	 * @return			a String in json format
	 */
	public String distributionToJson() {
		return SystemStateMonitor.toJson(buildDistribution());
	}

	public String palletsToJson() {
		return SystemStateMonitor.toJson(buildPallets());
	}

	public Map<String, Object> buildDistribution() {
		final Map<String, Object> map = new LinkedHashMap<>(2);
		map.put("global", buildGlobal(scheme));
		map.put("distribution", buildShardRep(scheme, leaderAware.getLeaderShardId()));
		return map;
	}
	
	private List<Map<String, Object>> buildPallets() {
		
		final CommittedState.SchemeExtractor extractor = new CommittedState.SchemeExtractor(scheme.getCommitedState());
		final List<Map<String, Object>> ret = new ArrayList<>(extractor.getPallets().size());
		for (final ShardEntity pallet: extractor.getPallets()) {
			
			final double[] dettachedWeight = {0};
			final int[] crudSize = new int[1];
			scheme.getDirty().findDutiesCrud(EntityEvent.CREATE, EntityState.PREPARED::equals, e-> {
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

	private static List<Map<String, Object>> buildShardRep(final Scheme table, final NetworkShardIdentifier leader) {
		final List<Map<String, Object>> ret = new LinkedList<>();
		final CommittedState.SchemeExtractor extractor = new CommittedState.SchemeExtractor(table.getCommitedState());
		for (final Shard shard : extractor.getShards()) {
			final List<Map<String , Object>> palletsAtShard =new LinkedList<>();
			
			for (final ShardEntity pallet: extractor.getPallets()) {
				final List<DutyView> dutyRepList = new LinkedList<>();
				final List<DutyView> repliRepList = new LinkedList<>();
				table.getCommitedState().findShards(sh->sh.equals(shard),  sh-> {
					for (ShardEntity se: table.getCommitedState().getReplicasByShard(sh)) {
						if (se.getDuty().getPalletId().equals(pallet.getPallet().getId())) {
							repliRepList.add(new DutyView(se.getDuty().getId(), se.getDuty().getWeight()));
						}
					}
				});
				
				table.getCommitedState().findDuties(shard, pallet.getPallet(), d-> {
					dutyRepList.add(
						new DutyView(
								d.getDuty().getId(),
								d.getDuty().getWeight()));});
				palletsAtShard.add(
						palletAtShardView(
								pallet.getPallet().getId(),
								extractor.getCapacity(pallet.getPallet(), shard),
								extractor.getWeight(pallet.getPallet(), shard),
								dutyRepList, repliRepList));
				
			}
			ret.add(shardView(
					shard.getShardID().getId() + " (" + shard.getShardID().getTag() + ")",
					shard.getFirstTimeSeen(),
					palletsAtShard, 
					shard.getState().toString(),
					shard.getShardID().equals(leader) ? "leader":"follower"));
		}
		return ret;
	}

	private static Map<String, Object> buildGlobal(final Scheme table) {
		CommittedState.SchemeExtractor extractor = new CommittedState.SchemeExtractor(table.getCommitedState());
		final Map<String, Object> map = new LinkedHashMap<>(8);
		map.put("size-shards", extractor.getShards().size());
		map.put("size-pallets", extractor.getPallets().size());
		map.put("size-scheme", extractor.getSizeTotal());
		map.put("size-crud", table.getDirty().getDutiesCrud().size());
		map.put("size-missings", table.getDirty().getDisturbance(EntityState.MISSING).size());
		map.put("size-dangling", table.getDirty().getDisturbance(EntityState.DANGLING).size());
		map.put("uncommited-change", table.getDirty().isStealthChange());
		map.put("commited-change", table.getCommitedState().isStealthChange());
		return map;
	}
	
	
	private static Map<String, Object> shardView(
			final String shardId, 
			final Instant creation, 
			final List<Map<String, Object>> pallets, 
			final String status, 
			final String mode) {
			
		final Map<String, Object> map = new LinkedHashMap<>(4);
		map.put("mode", mode);
		map.put("shard-id", shardId);
		map.put("creation", creation.toString());
		map.put("status", status);
		map.put("pallets", pallets);
		return map;
	}
	
	
	private static Map<String , Object> palletAtShardView (
			final String id, 
			final double capacity, 
			final double weight, 
			final List<DutyView> duties,
			final List<DutyView> replicas) {

		final Map<String , Object> map = new LinkedHashMap<>(5);
		map.put("id", id);
		map.put("size-duties", duties.size());
		map.put("size-replicas", replicas.size());
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
		//duties.forEach(d->sb.append(d.id).append(":").append(d.weight).append(","));
		duties.forEach(d->sb.append(d.id).append(","));		
		map.put("duties", sb.toString());
		return map;
	}
	
	
}
