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

import java.time.Instant;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.tilt.minka.core.leader.data.CommitedState;
import io.tilt.minka.core.leader.data.Scheme;
import io.tilt.minka.core.leader.data.DirtyState;
import io.tilt.minka.domain.CommitTree.Log;
import io.tilt.minka.domain.EntityEvent;
import io.tilt.minka.domain.EntityRecord;
import io.tilt.minka.domain.EntityState;
import io.tilt.minka.domain.ShardEntity;
import io.tilt.minka.shard.Shard;
import io.tilt.minka.shard.ShardState;
import io.tilt.minka.shard.Transition;
/**
 * Single-point of write access to the {@linkplain CommitedState}
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
			final Map<EntityEvent, StringBuilder> logging) {
		
		if (scheme.getCommitedState().commit(entity, shard, changelog.getEvent())) {
			log(changelog, entity, logging);

			// copy the found situation to the instance we care
			entity.getCommitTree().addEvent(changelog.getEvent(),
					COMMITED,
					shard.getShardID(),
					changelog.getPlanId());
						
			if (changelog.getEvent().getType()==EntityEvent.Type.ALLOC) {
				if (changelog.getEvent().is(EntityEvent.DETACH)) {
					moveToVault(shard, changelog, entity);
				}
				clearDirtyState(changelog, entity, shard);
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

	private void moveToVault(final Shard shard, final Log changelog, final ShardEntity entity) {
		if (null != entity.getCommitTree().findOne(
				changelog.getPlanId(), 
				shard.getShardID(), 
				EntityEvent.REMOVE)) {
			scheme.getVault().add(shard.getShardID().getId(), EntityRecord.fromEntity(entity, false));
		}
	}

	/** keep the uncommited-changes repo clean to its purpose */
	private void clearDirtyState(final Log changelog, final ShardEntity entity, Shard shard) {
		// remember crud is opaque from user, without any other info.
		final ShardEntity crud = scheme.getDirty().getCrudByDuty(entity.getDuty());
		if (crud!=null) {
			// BUG: el crud de un remove tiene shardid: leader (q recibio el req) no lo
			// va a encontrar ahi al REMOVE que planea targeteado al Shard donde esta VERDADERAMENTE..
			// esta es una asumpcion erronea
			//for (Log crudL: crud.getCommitTree().findAll(shard.getShardID())) {
			final Log crudL = crud.getCommitTree().getLast();
			final Instant lastEventOnCrud = crudL.getHead().toInstant();
			boolean previousThanCrud = changelog.getHead().toInstant().isBefore(lastEventOnCrud);
			// if the update corresponds to the last CRUD OR they're both the same event (duplicated operation)
			if (!previousThanCrud || changelog.getEvent().getUserCause()==crud.getLastEvent()) {
				if (!scheme.getDirty().removeCrud(entity)) {
					logger.warn("{} DirtyState CRUD didnt existed: {}", classname, entity);
				} else {
					logger.info("{} DirtyState CRUD discarded ok: {}", classname, entity);
				}
			} else {
				logger.warn("{}: Avoiding DirtyState remove (diff & after last event: {}, {})", 
						classname, entity, previousThanCrud);
			}			
		} else {
			// they were not crud: (dangling/missing/..)
		}
	}
	
	void shardStateTransition(final Shard shard, final ShardState prior, final Transition transition) {
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
	}

	void recover(final Shard shard, final Entry<EntityState, List<ShardEntity>> e) {
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
