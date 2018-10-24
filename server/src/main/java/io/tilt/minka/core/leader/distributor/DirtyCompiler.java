/*
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
package io.tilt.minka.core.leader.distributor;

import static io.tilt.minka.domain.EntityEvent.ATTACH;
import static io.tilt.minka.domain.EntityEvent.CREATE;
import static io.tilt.minka.domain.EntityEvent.DETACH;
import static io.tilt.minka.domain.EntityEvent.REMOVE;
import static io.tilt.minka.domain.EntityState.COMMITED;
import static io.tilt.minka.domain.EntityState.PREPARED;
import static io.tilt.minka.domain.ShardEntity.toStringIds;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Predicate;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.tilt.minka.core.leader.data.CommitState;
import io.tilt.minka.core.leader.data.CommittedState;
import io.tilt.minka.core.leader.data.DirtyState;
import io.tilt.minka.domain.EntityEvent;
import io.tilt.minka.domain.EntityState;
import io.tilt.minka.domain.ShardEntity;
import io.tilt.minka.shard.Shard;

/**
 * Compile all uncommited changes until the execution of the current plan
 * Also Dispatches deletions.
 * 
 * @author Cristian Gonzalez
 * @since Ago 3, 2018
 */
class DirtyCompiler {

	private final String DC_DANGLING_RESUME = "{}: Registered {} dangling duties {}";
	private final String DC_UNFINISHED_DANGLING	= "{}: Previous change's unfinished business saved as Dangling: {}";
	private final String DC_FALSE_UNFINISHED = "{}: Previous change although unfinished hasnt waiting duties";
	private final String DC_SHIPPED_FROM_DUTY	 =" {}: Shipped {} from: {}, Duty: {}";
	private final String DC_REMOVAL_WINS = "{}: Duty ({}) deletion wins over creation concurrency";

	private static final Logger logger = LoggerFactory.getLogger(DirtyCompiler.class);

	private final String name = getClass().getSimpleName();
	
	private final CommittedState state;
	private final ChangePlan previousChange;
	private final ChangePlan changePlan;
	private final DirtyState snapshot;
	
	private final Set<ShardEntity> creations;
	private final Set<ShardEntity> deletions;
	
	DirtyCompiler(
			final CommittedState state, 
			final ChangePlan previous, 
			final ChangePlan current,
			final DirtyState snapshot) {
		this.state = state;
		this.previousChange = previous;
		this.changePlan = current;
		this.snapshot = snapshot;		
		this.creations = compileCreations();
		this.deletions = compileRemovals(creations);
	}
	
	Set<ShardEntity> getCreations() {
		return creations;
	}
	Set<ShardEntity> getDeletions() {
		return deletions;
	}

	private Set<ShardEntity> compileRemovals(final Set<ShardEntity> dutyCreations) {
		final Set<ShardEntity> dutyDeletions = new HashSet<>();
		addDeletions(dutyCreations, dutyDeletions);
		restorePendings(previousChange, dutyDeletions::add, 
				d->d.getLastEvent()==REMOVE || d.getLastEvent()==DETACH);
		
		// lets add those duties of a certain deleting pallet
		snapshot.findPalletsCrud(REMOVE::equals, PREPARED::equals, p-> {
			state.findDutiesByPallet(p.getPallet(), dutyDeletions::add);
		});
		dispatchDeletions(changePlan, dutyDeletions);
		return dutyDeletions;
	}

	private void addDeletions(final Set<ShardEntity> dutyCreations, final Set<ShardEntity> dutyDeletions) {
		snapshot.findDutiesCrud(REMOVE, null, crud-> {
			// as a CRUD a deletion lives in stage as a mark within an Opaque ShardEntity
			// we must now search for the real one
			final ShardEntity schemed = state.getByDuty(crud.getDuty());
			if (schemed!=null) {
				// translate the REMOVAL event
				schemed.getCommitTree().addEvent(
						REMOVE, 
						EntityState.PREPARED, 
						state.findDutyLocation(schemed).getShardID(), 
						changePlan.getId());
				dutyDeletions.add(schemed);
			}
		});
		// prevail user's deletion op. over clustering restore/creation
		for (ShardEntity removing: dutyDeletions) {
			if (dutyCreations.remove(removing)) {
				logger.warn(DC_REMOVAL_WINS, getClass().getSimpleName(), removing);
				snapshot.updateCommitRequest(EntityEvent.REMOVE, removing, CommitState.CANCELLED);
				// TODO stateSentry.notifysers(...)
			}
		}
	}

	/*
	 * check waiting duties never confirmed (for fallen shards as previous
	 * target candidates)
	 */
	private void restorePendings(final ChangePlan previous, 
			final Consumer<ShardEntity> c, 
			final Predicate<ShardEntity> p) {
		if (previous != null 
				&& previous.getResult().isClosed() 
				&& !previous.getResult().isSuccess()) {
			int rescued = previous.findAllNonConfirmedFromAllDeliveries(d->{
				if (p.test(d)) {
					c.accept(d);
					changePlan.addFeature(ChangeFeature.FIXES_UNFINISHED);
				}
			});
			if (rescued ==0 && logger.isInfoEnabled()) {
				logger.info(DC_FALSE_UNFINISHED, name);
			} else {
				if (logger.isInfoEnabled()) {
					logger.info(DC_UNFINISHED_DANGLING, name, rescued);
				}
			}
		}
	}

	/* by user deleted */
	private final void dispatchDeletions(final ChangePlan changePlan,
			final Set<ShardEntity> deletions) {

		for (final ShardEntity deletion : deletions) {
			final Shard shard = state.findDutyLocation(deletion);
			deletion.getCommitTree().addEvent(DETACH, 
					PREPARED,
					shard.getShardID(),
					changePlan.getId());
			changePlan.dispatch(shard, deletion);
			if (logger.isInfoEnabled()) {
				logger.info(DC_SHIPPED_FROM_DUTY, name, DETACH, shard.getShardID(), deletion.toBrief());
			}
		}
	}
	
	private Set<ShardEntity> compileCreations() {
		// recently fallen shards
		addMissingAsCrud();
		final Set<ShardEntity> dutyCreations = new HashSet<>();
		snapshot.findDutiesCrud(CREATE, null, dutyCreations::add);
		// add danglings as creations prior to migrations
		for (ShardEntity d: snapshot.getDutiesDangling()) {
			dutyCreations.add(ShardEntity.Builder.builderFrom(d).build());			
		}
		
		// add previous fallen and never confirmed migrations
		restorePendings(previousChange, dutyCreations::add, 
				d->d.getLastEvent()==CREATE || d.getLastEvent()==ATTACH);
		return dutyCreations;
	}
	
	private void addMissingAsCrud() {
	    final Collection<ShardEntity> missing = snapshot.getDutiesMissing();
		for (final ShardEntity missed : missing) {
			final Shard lazy = state.findDutyLocation(missed);
			if (logger.isDebugEnabled()) {
				logger.debug("{}: Registering {}, missing Duty: {}", name,
					lazy == null ? "unattached" : "from falling Shard: " + lazy, missed);
			}
			if (lazy != null) {
				// this's a FIX to the right state
				if (state.commit(missed, lazy, REMOVE)) {
					// missing duties are a confirmation per-se from the very shards,
					// so the ptable gets fixed right away without a realloc.
					missed.getCommitTree().addEvent(
							REMOVE, 
							COMMITED,
							lazy.getShardID(),
							changePlan.getId());					
				}
			}
			missed.getCommitTree().addEvent(CREATE, PREPARED,"N/A",changePlan.getId());
			snapshot.createCommitRequests(EntityEvent.CREATE, Collections.singleton(missed), null, false);
		}
		if (!missing.isEmpty()) {
			logger.info(DC_DANGLING_RESUME, name, missing.size(), toStringIds(missing));
		}
	}
	
}
