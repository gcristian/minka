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
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.tilt.minka.broker.EventBroker;
import io.tilt.minka.core.leader.data.CommitRequest;
import io.tilt.minka.core.leader.data.CommitState;
import io.tilt.minka.core.leader.data.Scheme;
import io.tilt.minka.core.leader.distributor.ChangePlan;
import io.tilt.minka.core.leader.distributor.Dispatch;
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
@SuppressWarnings({ "rawtypes", "unchecked" })
public class StateSentry implements BiConsumer<Heartbeat, Shard> {

	private final Logger logger = LoggerFactory.getLogger(getClass());
	private final String classname = getClass().getSimpleName();
	
	private final Scheme scheme;
	private final StateExpected expected;
	private final StateUnexpected unexpected;
	private final StateWriter writer;
	private final EventBroker broker; 
	
	StateSentry(final Scheme scheme, final Scheduler scheduler, final StateWriter writer, final EventBroker broker) {
		this.scheme = scheme;
		this.expected = new StateExpected(scheme);
		this.unexpected = new StateUnexpected(scheme);
		this.writer = writer;
		this.broker = broker;
	}

	/**
	 * Mission: watch FollowerBootstrap's heartbeats for changes.
	 */
	@Override
	public void accept(final Heartbeat beat, final Shard shard) {
		if (beat.getShardChange() != null) {
			logger.info("{}: ShardID: {} changes to: {}", classname, shard, beat.getShardChange());
   			writer.writeShardState(shard, shard.getState(), beat.getShardChange());
			if (beat.getShardChange().getState() == ShardState.QUITTED) {
				return;
			}
		}	
		// proctor will need it
		shard.enterHeartbeat(beat);
		
		// contrast current change-plan deliveries and discover new answers
		final Collection<CommitRequest> reqs = detectExpectedChanges(shard, beat);

		// look for problems
		if ((beat.reportsDuties()) 
				&& shard.getState().isAlive() 
				&& scheme.getLearningState().isEmpty()) {
			detectUnexpected(shard, beat.getCaptured());
		}
		if (reqs!=null) {
			notifyUser(reqs);
		}	
		
		beat.clear();
	}
	
	/* this checks partition table looking for missing duties (not declared dangling, that's diff) */
	private void detectUnexpected(final Shard shard, final Collection<EntityRecord> reportedDuties) {
		for (Map.Entry<EntityState, Collection<ShardEntity>> e: 
			unexpected.findLost(shard, reportedDuties).entrySet()) {
			writer.recover(shard, e);
		}
		if (scheme.getCurrentPlan()!=null) {
			unexpected.detectInvalidSpots(shard, reportedDuties);
		}
	}
	
	private Collection<CommitRequest> detectExpectedChanges(final Shard shard, final Heartbeat beat) {
		final ChangePlan changePlan = scheme.getCurrentPlan();
		if (changePlan!=null && !changePlan.getResult().isClosed()) {
			final Dispatch dispatch = changePlan.get(shard);
			if (dispatch!=null && dispatch.getStep()==Dispatch.Step.PENDING) {
				final Map<EntityEvent, StringBuilder> logg = new HashMap<>(EntityEvent.values().length);
				final List<CommitRequest> requests = new ArrayList<>(beat.getCaptured().size());
				if (expected.detect(
						dispatch, 
						changePlan.getId(), 
						beat, 
						shard.getShardID(), 
						(log, del)-> writer.commit(shard, log, del, requests::add, logg),
						EntityEvent.ATTACH, EntityEvent.STOCK)) {
					dispatch.calculateState(logger::info);
				}
				doLogging(shard, changePlan, logg);
				return requests;
			}
		} else if (isLazyOrSurvivor(beat, changePlan)) {
			scheme.getLearningState().learn(beat.getCaptured(), shard);
		}
		return null;
	}
	
	private void notifyUser(final Collection<CommitRequest> requests) {
		requests.stream()
			.filter(CommitRequest::isRespondState)
			.filter(r->!r.isSent(EntityEvent.Type.ALLOC) 
					&& (r.getState()==CommitState.FINISHED 
						|| r.getState()==CommitState.ALLOCATION))
			.collect(Collectors.groupingBy((req)->
				scheme.getCommitedState().findShard(
						s->s.getShardID().getId().equals(
								req.getEntity().getCommitTree().getFirst().getTargetId()))
		)).forEach((shard, stageRequestsGroup)-> {
			if (shard!=null) {
				logger.info("{}: Notifying user at {}", getClass().getSimpleName(), shard);
				if (broker.send(shard.getBrokerChannel(), (List)stageRequestsGroup)) {
					for (CommitRequest req: stageRequestsGroup) {
						req.markSent();
						// clear for good out of DirtyState							
						scheme.getDirty().updateCommitRequest(
								req.getEntity().getLastEvent(), 
								req.getEntity(), 
								CommitState.FINISHED);
					}
				} else {
					logger.error("{}: Couldnt notify CommitRequest's to User", getClass().getSimpleName());
				}
			} else {
				logger.error("{}: Cannot process CommitRequest on dissapeared Shards", 
						getClass().getSimpleName());
			}
		});
		
		requests.stream()
			.filter(r-> !r.isRespondState())
			.forEach(r-> {
				scheme.getDirty().updateCommitRequest(
						r.getEntity().getLastEvent(), 
						r.getEntity());
			});
	}

	/**
	 * @return TRUE if we must consider heartbeat reports as part of previous state
	 */
	private boolean isLazyOrSurvivor(final Heartbeat beat, final ChangePlan changePlan) {
		if (!beat.reportsDuties()) {
			return false;
		}
		final boolean recentSurvivor = changePlan == null;
		boolean lazySurvivor = false;
		if (!recentSurvivor && changePlan.getResult().isClosed()) {
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
				lazySurvivor = max < scheme.getFirstPlanId();
			}
		}
		return lazySurvivor || recentSurvivor;
	}

	private void doLogging(final Shard shard, final ChangePlan changePlan, final Map<EntityEvent, StringBuilder> logg) {
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
