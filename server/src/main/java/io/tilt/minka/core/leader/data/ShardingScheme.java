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
package io.tilt.minka.core.leader.data;

import static io.tilt.minka.core.leader.data.ShardingScheme.ClusterHealth.STABLE;
import static io.tilt.minka.core.leader.data.ShardingScheme.ClusterHealth.UNSTABLE;

import java.util.LinkedList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.tilt.minka.api.Duty;
import io.tilt.minka.core.leader.SchemeSentry;
import io.tilt.minka.core.leader.distributor.ChangePlan;
import io.tilt.minka.domain.EntityEvent;
import io.tilt.minka.domain.Shard;
import io.tilt.minka.domain.ShardEntity;

/**
 * Only one modifier allowed: {@linkplain SchemeSentry} with a {@linkplain ChangePlan} after a distribution process.
 * 
 * Contains the relations between {@linkplain Shard} and {@linkplain Duty}.
 * Continuously checked truth in {@linkplain Scheme}.
 * Client CRUD requests and detected problems in {@linkplain Stage}
 * Built at leader promotion.
 * 
 * @author Cristian Gonzalez
 * @since Dec 2, 2015	
 */
public class ShardingScheme {

	private static final Logger logger = LoggerFactory.getLogger(Scheme.class);
	
	private ClusterHealth visibilityHealth;
	private ClusterHealth distributionHealth;
	
	private final Scheme scheme;
	private final Stage stage;
	private ChangePlan currentPlan;
	private List<Runnable> observers;
	
	/**
	 * status for the cluster taken as Avg. for the last 5 cycles
	 * when this happens the cluster rebalances workload 
	 * by making shards exchange duties  
	 */
	public enum ClusterHealth {
		/* visibility: if there was no shard changing status working: theere's
		 * work-load balance among the nodes */
		STABLE,
		/* visibility: if there was at least one shard changing status that
		 * will provoke a duty reallocation working: there's a reallocation in progress */
		UNSTABLE,;
	}

	/** the shard's total reported capacities for all pallets */ 
	public enum ClusterCapacity {
		/* no weighting duties loaded */
		IDLE, 
		/* good: yet enough capacity for more duties of avg. weight */
		NORMAL,
		/* warning: no more capacity for average duty weight */
		FRAGILE,
		/* bad: dutie's total weight bigger than shard's total capacity, 
		 * some duties are not being distributed !! */
		INSUFFICIENT,
	}

	public ShardingScheme() {
		this.visibilityHealth = ClusterHealth.STABLE;
		this.distributionHealth = ClusterHealth.STABLE;
		this.scheme = new Scheme();
		this.stage = new Stage();
	}
	
	public ChangePlan getCurrentPlan() {
		return this.currentPlan;
	}

	public void setPlan(final ChangePlan change) {
		this.currentPlan = change;
		notifyObservers();
	}
	
	private void notifyObservers() {
		if (observers!=null) {
			for (Runnable run: observers) {
				try {
					run.run();
				} catch (Exception e) {
				}
			}
		}
	}

	public Stage getStage() {
		return this.stage;
	}
	
	public Scheme getScheme() {
		return this.scheme;
	}

	public ClusterHealth getHealth() {
		return this.distributionHealth == visibilityHealth 
				&& distributionHealth == STABLE ? STABLE : UNSTABLE;
	}

	public ClusterHealth getDistributionHealth() {
		return this.distributionHealth;
	}

	public void setDistributionHealth(ClusterHealth distributingHealth) {
		this.distributionHealth = distributingHealth;
	}

	public ClusterHealth getShardsHealth() {
		return this.visibilityHealth;
	}

	public void setShardsHealth(ClusterHealth health) {
		this.visibilityHealth = health;
	}

	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder()
				.append("Shards: ")
				.append(getScheme().shardsSize())
				.append(" Crud Duties: ")
				.append(getStage().dutyCrud.size());
		//.append(" Change: ").append(change.getGroupedIssues().size());
		return sb.toString();
	}

	public void logStatus() {
		getScheme().logStatus();
		getStage().logStatus();
		logger.info("{}: Health: {}", getClass().getSimpleName(), getDistributionHealth());
	}

	/** 
	 * add without considerations (they're staged but not distributed per se)
	 * @return TRUE if the operation is done for the first time 
	 */
	public boolean addCrudPallet(final ShardEntity pallet) {
		boolean done = false;
		if (pallet.getLastEvent()==EntityEvent.REMOVE) {
			// TRUE: something removed
			done = getScheme().palletsById.remove(pallet.getPallet().getId())!=null;
		} else if (pallet.getLastEvent()==EntityEvent.CREATE) {
			// TRUE: done first time
			done = getScheme().palletsById.put(pallet.getPallet().getId(), pallet)==null;	
		}
		return done;
	}

	public void addChangeObserver(final Runnable observer) {
		if (observers==null) {
			this.observers = new LinkedList<>();
		}
		observers.add(observer);
	}


}
