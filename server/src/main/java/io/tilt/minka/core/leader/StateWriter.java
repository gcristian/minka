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

import static io.tilt.minka.domain.EntityEvent.DETACH;
import static io.tilt.minka.domain.EntityEvent.REMOVE;
import static io.tilt.minka.domain.EntityState.COMMITED;
import static io.tilt.minka.domain.EntityState.DANGLING;
import static io.tilt.minka.domain.EntityState.MISSING;

import java.util.Collection;
import java.util.Map;
import java.util.Map.Entry;
import java.util.function.Consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.tilt.minka.core.leader.data.CommitRequest;
import io.tilt.minka.core.leader.data.CommitState;
import io.tilt.minka.core.leader.data.CommittedState;
import io.tilt.minka.core.leader.data.DirtyState;
import io.tilt.minka.core.leader.data.Scheme;
import io.tilt.minka.domain.CommitTree.Log;
import io.tilt.minka.domain.EntityEvent;
import io.tilt.minka.domain.EntityRecord;
import io.tilt.minka.domain.EntityState;
import io.tilt.minka.domain.ShardEntity;
import io.tilt.minka.shard.Shard;
import io.tilt.minka.shard.ShardState;
import io.tilt.minka.shard.Transition;
/**
 * Single-point of write access to the {@linkplain CommittedState}
 * Watches follower's heartbeats taking action on any update
 * Beats with changes are delegated to a {@linkplain StateExpected} 
 * Anomalies and CRUD ops. are recorded into {@linkplain DirtyState}
 * 
 * @author Cristian Gonzalez
 * @since Jan 4, 2016
 */
public class StateWriter {

	private final Logger logger = LoggerFactory.getLogger(getClass());
	private final String classname = getClass().getSimpleName();
	
	private final Scheme scheme;
	
	public StateWriter(final Scheme scheme) {
		super();
		this.scheme = scheme;
	}

	void commit(final Shard shard, 
			final Log changelog, 
			final ShardEntity entity, 
			final Consumer<CommitRequest> requestConsumer,
			final Map<EntityEvent, StringBuilder> logging) {
		
		if (scheme.getCommitedState().commit(entity, shard, changelog.getEvent())) {
			log(changelog, entity, logging);

			// copy the found situation to the instance we care
			entity.getCommitTree().addEvent(changelog.getEvent(),
					COMMITED,
					shard.getShardID(),
					changelog.getPlanId());
						
			final Log request = findEventRootLog(changelog, entity); 
			if (request!=null) {
				// move to vault only on drop (detach preceeds drop)
				if (request.getEvent().is(EntityEvent.REMOVE) 
						&& changelog.getEvent().is(EntityEvent.DROP)) {
					scheme.getVault().add(shard.getShardID().getId(), EntityRecord.fromEntity(entity, false));
				}
				final CommitRequest req = scheme.getDirty().updateCommitRequest(changelog.getEvent(), entity);				
				if (req!=null) {
					requestConsumer.accept(req);
				} else {
					logger.warn("{} CommitRequest not found: {}", classname, entity.toBrief());
				}
			}
		}
	}

	private void log(final Log changelog, final ShardEntity entity, final Map<EntityEvent, StringBuilder> logging) {
		if (logger.isInfoEnabled()) {
			StringBuilder sb = logging.get(changelog.getEvent());
			if (sb==null) {
				logging.put(changelog.getEvent(), sb = new StringBuilder());
			}
			sb.append(entity.getEntity().getId()).append(',');
		}
	}
	
	/** @return Log if the event may reflect a {@linkplain CommitRequest} */
	private static Log findEventRootLog(final Log changelog, final ShardEntity entity) {
		final EntityEvent e = changelog.getEvent();
		if (e.getUserCause()==null) {
			return null;
		}
		// prevent Nthuplicated events
		if (e.getUserCause()==EntityEvent.CREATE && isNormalEcho(changelog, entity)) {
			return null;
		}
		// look up for all plans
		final long minTime = 0;
		final Log root = entity.getCommitTree().existsWithLimit(e.getUserCause(), minTime);
		if (root!=null && root.getEvent().getUserCause()!=null) {
			// REAL root has no root :) 
			return null;
		} else {
			return root;
		}
	}

	/** 
	 * @return TRUE when the current log's event is a normal 
	 * alloc/replica out of a creation/removal stage request */
	private static boolean isNormalEcho(final Log current, final ShardEntity entity) {
		final long minimumPlan = entity.getCommitTree().getFirst().getPlanId();
		final boolean echo[] = {false};
		entity.getCommitTree().filterWithLimit(current.getEvent(), minimumPlan, log-> {
			// avoid current log
			if (!log.equals(current)) {
				echo[0]|=log.getLastState()==EntityState.COMMITED;
			}
		});
		return echo[0];
	}
	
	void writeShardState(final Shard shard, final ShardState prior, final Transition transition) {
		shard.applyChange(transition);
		scheme.getCommitedState().stealthChange(true);		
		switch (transition.getState()) {
		case GONE:
		case QUITTED:
			recoverAndRetire(shard);
			// distributor will decide plan obsolecy if it must
			break;
		case ONLINE:
			// TODO get ready
			break;
		case DELAYED:
			// TODO lot of consistency checks here on duties
			// to avoid chain of shit from heartbeats reporting doubtful stuff
			break;
		default:
			break;
		}
	}

	/*
	 * dangling duties are set as already confirmed, change wont wait for this
	 * to be confirmed
	 */
	private void recoverAndRetire(final Shard shard) {
		final Collection<ShardEntity> dangling = scheme.getCommitedState().getDutiesByShard(shard);
		if (logger.isInfoEnabled()) {
			logger.info("{}: Removing fallen Shard: {} from ptable. Saving: #{} duties: {}", classname, shard,
				dangling.size(), ShardEntity.toStringIds(dangling));
		}
		for (ShardEntity e: dangling) {
			if (scheme.getDirty().addDangling(e)) {
				e.getCommitTree().addEvent(
						DETACH, 
						COMMITED, 
						shard.getShardID(), 
						e.getCommitTree().getLast().getPlanId());
			}
		}
		scheme.getCommitedState().removeShard(shard);
		scheme.getVault().addGoneShard(shard);

	}

	void recover(final Shard shard, final Entry<EntityState, Collection<ShardEntity>> e) {
		final StringBuilder log = new StringBuilder();
		boolean uncommitted = false;
		if (e.getKey()==DANGLING) {
			uncommitted = scheme.getDirty().addDangling(e.getValue());
		} else if (e.getKey()==MISSING) {
			uncommitted = scheme.getDirty().addMissing(e.getValue());
		} else {
			logger.warn("{} From ({}) comes Unexpected state {} for duty {}", getClass().getSimpleName(), 
					shard, e.getKey(), ShardEntity.toStringBrief(e.getValue()));
		}
		
		// copy the event so it's marked for later consideration
		if (uncommitted) {
			if (logger.isInfoEnabled()) {
				log.append(ShardEntity.toStringIds(e.getValue()));
			}
			for (ShardEntity d: e.getValue()) {
				if (scheme.getCommitedState().commit(d, shard, REMOVE)) {
					d.getCommitTree().addEvent(
							d.getLastEvent(),
							e.getKey(), 
							"N/A", // the last shard id 
							d.getCommitTree().getLast().getPlanId());
				}
			}
		}
		if (log.length()>0) {
			logger.info("{}: Written unexpected absents ({}) at [{}] on: {}", 
					getClass().getSimpleName(), REMOVE.name(), shard, ShardEntity.toStringIds(e.getValue()));
		}

	}


}
