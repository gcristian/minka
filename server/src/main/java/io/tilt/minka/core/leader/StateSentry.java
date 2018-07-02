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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.tilt.minka.core.leader.data.Scheme;
import io.tilt.minka.core.leader.distributor.ChangePlan;
import io.tilt.minka.core.leader.distributor.Delivery;
import io.tilt.minka.core.task.Scheduler;
import io.tilt.minka.domain.CommitTree;
import io.tilt.minka.domain.CommitTree.Log;
import io.tilt.minka.domain.EntityEvent;
import io.tilt.minka.domain.EntityRecord;
import io.tilt.minka.domain.EntityState;
import io.tilt.minka.domain.Heartbeat;
import io.tilt.minka.domain.ShardEntity;
import io.tilt.minka.shard.Shard;
import io.tilt.minka.shard.ShardState;
/**
 * Consumption of heartbeats and reaction to state changes
 * 
 * Beats with planned distributions are delegated to a {@linkplain StateExpected}
 * Beats with anomalies out of plan to a {@linkplain StateUnexpected}
 * Both detections are flowed thru {@linkplain StateWriter}  
 * 
 * 
 * @author Cristian Gonzalez
 * @since Jan 4, 2016
 */
public class StateSentry implements BiConsumer<Heartbeat, Shard> {

	private final Logger logger = LoggerFactory.getLogger(getClass());
	private final String classname = getClass().getSimpleName();
	
	private final Scheme scheme;
	private final StateExpected expected;
	private final StateUnexpected unexpected;
	private final StateWriter writer;
	
	StateSentry(final Scheme scheme, final Scheduler scheduler, final StateWriter writer) {
		this.scheme = scheme;
		this.expected = new StateExpected(scheme);
		this.unexpected = new StateUnexpected(scheme);
		this.writer = writer;
	}

	/**
	 * Mission: watch Follower's heartbeats for changes.
	 */
	@Override
	public void accept(final Heartbeat beat, final Shard shard) {
		if (beat.getShardChange() != null) {
			logger.info("{}: ShardID: {} changes to: {}", classname, shard, beat.getShardChange());
   			writer.shardStateTransition(shard, shard.getState(), beat.getShardChange());
			if (beat.getShardChange().getState() == ShardState.QUITTED) {
				return;
			}
		}	
		// proctor will need it
		shard.enterHeartbeat(beat);
		
		// contrast current change-plan deliveries and discover new answers
		detectExpectedChanges(shard, beat);

		// look for problems
		if ((beat.reportsDuties()) 
				&& shard.getState().isAlive() 
				&& scheme.getLearningState().isEmpty()) {
			detectUnexpected(shard, beat.getCaptured());
		}
		beat.clear();
	}
	
	/* this checks partition table looking for missing duties (not declared dangling, that's diff) */
	private void detectUnexpected(final Shard shard, final List<EntityRecord> reportedDuties) {
		for (Map.Entry<EntityState, List<ShardEntity>> e: 
			unexpected.findLost(shard, reportedDuties).entrySet()) {
			writer.recover(shard, e);
		}
		if (scheme.getCurrentPlan()!=null) {
			unexpected.detectInvalidSpots(shard, reportedDuties);
		}
	}

	
	private void detectExpectedChanges(final Shard shard, final Heartbeat beat) {
		final ChangePlan changePlan = scheme.getCurrentPlan();
		if (changePlan!=null && !changePlan.getResult().isClosed()) {
			
			final Delivery delivery = changePlan.getDelivery(shard);
			if (delivery!=null && delivery.getStep()==Delivery.Step.PENDING) {
				final Map<EntityEvent, StringBuilder> logg = new HashMap<>(EntityEvent.values().length);				
				if (expected.detect(
						delivery, 
						changePlan.getId(), 
						beat, 
						shard.getShardID(), 
						(log, del)-> writer.commit(shard, log, del, logg),
						EntityEvent.ATTACH, EntityEvent.STOCK, EntityEvent.DROP)) { 
					delivery.calculateState(logger::info);
				}
				logging(shard, changePlan, logg);
			} else if (logger.isDebugEnabled()) {
				logger.debug("{}: no {} Delivery for heartbeat's shard: {}", getClass().getSimpleName(), 
						Delivery.Step.PENDING, shard.getShardID().toString());
			}			
		} else if (isLazyOrSurvivor(beat, changePlan)) {
			scheme.getLearningState().learn(beat.getCaptured(), shard);
		}
	}

	/**
	 * @return TRUE if we must consider heartbeat reports as part of previous state
	 */
	private boolean isLazyOrSurvivor(final Heartbeat beat, final ChangePlan changePlan) {
		final boolean survivor = changePlan == null && beat.reportsDuties();
		boolean lazy = false;
		if (changePlan != null && changePlan.getResult().isClosed() && beat.reportsDuties()) {
			// wait only 1 change-plan for the lazy followers
			if (changePlan.getId() == scheme.getFirstPlanId()) {
				long max = 0;
				for (EntityRecord er : beat.getCaptured()) {
					final Log l = er.getCommitTree().findOne(
							CommitTree.PLAN_LAST, 
							beat.getShardId(), 
							EntityEvent.ATTACH);
					if (l != null && max < l.getPlanId()) {
						max = l.getPlanId();

					}
				}
				lazy = max < scheme.getFirstPlanId();
			}
		}
		return lazy || survivor;
	}

	private void logging(final Shard shard, final ChangePlan changePlan, final Map<EntityEvent, StringBuilder> logg) {
		if (logg.size()>0 && logger.isInfoEnabled()) {
			logg.entrySet().forEach(e-> logger.info("{}: Written expected change {} at [{}] on: {}", 
					getClass().getSimpleName(), e.getKey().name(), shard, e.getValue().toString()));
		}
		
		if (changePlan.getResult().isClosed()) {
			if (logger.isInfoEnabled()) {
				logger.info("{}: ChangePlan finished ! (promoted) duration: {}ms", classname, 
						System.currentTimeMillis()-changePlan.getCreation().toEpochMilli());
			}
		} else if (logger.isInfoEnabled() && changePlan.hasUnlatched()) {
			logger.info("{}: ChangePlan unlatched, fwd >> distributor agent ", classname);
			//scheduler.forward(scheduler.get(Semaphore.Action.DISTRIBUTOR));
		}
	}

}
