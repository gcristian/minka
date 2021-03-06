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

import static io.tilt.minka.core.leader.data.Scheme.ClusterHealth.STABLE;
import static io.tilt.minka.core.leader.data.Scheme.ClusterHealth.UNSTABLE;

import java.util.LinkedList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.tilt.minka.core.leader.StateSentry;
import io.tilt.minka.core.leader.distributor.ChangePlan;
import io.tilt.minka.domain.EntityEvent;
import io.tilt.minka.domain.ShardEntity;
import io.tilt.minka.model.Duty;
import io.tilt.minka.shard.Shard;

/**
 * Only one modifier allowed: {@linkplain StateSentry} with a {@linkplain ChangePlan} after a distribution process.
 * 
 * Contains the relations between {@linkplain Shard} and {@linkplain Duty}.
 * Continuously checked truth in {@linkplain CommittedState}.
 * Client CRUD requests and detected problems in {@linkplain DirtyState}
 * Built at leader promotion.
 * 
 * @author Cristian Gonzalez
 * @since Dec 2, 2015	
 */
public class Scheme {

	private static final Logger logger = LoggerFactory.getLogger(CommittedState.class);
	
	private ClusterHealth visibilityHealth;
	private ClusterHealth distributionHealth;
	
	/** record result of changes applied and verified, as a reaction to Client CRUDs or cluster changes*/
	private final CommittedState committedState;
	/** temporal Client CRUDs willing to appear in the commited state */
	private final DirtyState dirtyState;
	/** Cluster elected new leader: followers redirect their state to the new one: a learning process starts */
	private LearningState learningState;
	/** Place for deleted duties only for debug */
	private Vault vault;
	/** the last and the current plan in progress of verification and application */
	private ChangePlan currentPlan;
	private List<Runnable> observers;
	private Long firstPlanId;
	
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

	public Scheme() {
		this.visibilityHealth = ClusterHealth.STABLE;
		this.distributionHealth = ClusterHealth.STABLE;
		this.committedState = new CommittedState();
		this.dirtyState = new DirtyState();
		this.vault = new Vault();
	}
	
	public ChangePlan getCurrentPlan() {
		return this.currentPlan;
	}

	public Long getFirstPlanId() {
		return firstPlanId;
	}
	public void setFirstPlanId(Long firstPlanId) {
		this.firstPlanId = firstPlanId;
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

	public DirtyState getDirty() {
		return this.dirtyState;
	}
	
	public CommittedState getCommitedState() {
		return this.committedState;
	}

	public LearningState getLearningState() {
		if (learningState == null) {
			learningState = new LearningState();
		}
		return learningState;
	}
	
	public Vault getVault() {
		return vault;
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
				.append(getCommitedState().shardsSize())
				;
		//.append(" Transition: ").append(change.getGroupedIssues().size());
		return sb.toString();
	}

	public void logStatus() {
		getCommitedState().logStatus();
		getDirty().logStatus();
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
			done = getCommitedState().palletsById.remove(pallet.getPallet().getId())!=null;
		} else if (pallet.getLastEvent()==EntityEvent.CREATE) {
			// TRUE: done first time
			done = getCommitedState().palletsById.put(pallet.getPallet().getId(), pallet)==null;	
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
