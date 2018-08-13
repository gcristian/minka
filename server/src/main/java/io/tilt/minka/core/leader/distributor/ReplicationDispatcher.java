package io.tilt.minka.core.leader.distributor;

import java.util.Collection;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Predicate;

import io.tilt.minka.api.Pallet;
import io.tilt.minka.core.leader.data.CommitedState;
import io.tilt.minka.core.leader.data.Scheme;
import io.tilt.minka.domain.CommitTree.Log;
import io.tilt.minka.domain.EntityEvent;
import io.tilt.minka.domain.EntityState;
import io.tilt.minka.domain.ShardEntity;
import io.tilt.minka.shard.NetworkShardIdentifier;
import io.tilt.minka.shard.Shard;

/**
 * Replication mechanism know-how to allow Leader fall.
 * I.e.: leader's post-load affected duties by all CRUD operations (after distribution)
 * to be reported by SURVIVING followers, to the new Leader.   
 * 
 * More specifically: ships {@linkplain EntityEvent.STOCK and DROP} to the {@link ChangePlan}
 * those duties affected by Creating and Removing root causes.
 * 
 * Stocked replicas are not balanced among the shards.
 */
class ReplicationDispatcher {

	private final Scheme scheme;

	ReplicationDispatcher(final Scheme state) {
		this.scheme = state;
	}
	
	void dispatchReplicas(final ChangePlan changePlan, 
			final Set<ShardEntity> creations, 
			final Set<ShardEntity> deletions,
			final NetworkShardIdentifier leaderId,
			final Pallet p) {
		
		final Shard leader = scheme.getCommitedState().findShard(leaderId.getId());
		// those of current plan
		dispatchNewLocals(EntityEvent.CREATE, EntityEvent.ATTACH, EntityEvent.STOCK, 
				changePlan, creations, leader, p);		
		dispatchNewLocals(EntityEvent.REMOVE, EntityEvent.DETACH, EntityEvent.DROP,
				changePlan, deletions, null, p);
		// those of older plans (new followers may have turned online)
		dispatchCurrentLocals(scheme, changePlan, p, leader);
	}
	
	private void dispatchNewLocals(
			final EntityEvent evidence,
			final EntityEvent action,
			final EntityEvent reaction,
			final ChangePlan changePlan, 
			final Set<ShardEntity> involved,
			final Shard main,
			final Pallet p) {
		
		final CommitedState cs = scheme.getCommitedState();
		// those being shipped for the action event
		changePlan.onShippingsFor(action, main, (target, duty)-> {
			// same pallet, and present as new CRUD (involved)
			if (duty.getDuty().getPalletId().equals(p.getId()) && involved.contains(duty)) {
				// search back the evidence event as authentic purpose to reaction
				final long limit = System.currentTimeMillis()-(1000*60);
				if (duty.getCommitTree().exists(evidence, limit)!=null) {
					cs.findShards(
							predicate(action, target, duty),
							replicate(changePlan, duty, reaction)
					);
				}				
			}
		});
	}
	
	/**
	 * check if they were created before the shard's online
	 * or if they're already stocked there, dont do it twice ! (warnings arise)
	 */
	private void dispatchCurrentLocals(
			final Scheme state, 
			final ChangePlan changePlan,
			final Pallet pallet,
			final Shard leader) {
				
		final CommitedState cs = state.getCommitedState();
		cs.findDuties(leader, pallet, replica-> {
			cs.findShards(
					predicate(EntityEvent.ATTACH, leader, replica), 
					replicate(changePlan, replica, EntityEvent.STOCK)
			);
		});
	}

	/** it might be stock or drop */
	private static Consumer<Shard> replicate(
			final ChangePlan changePlan, 
			final ShardEntity replicated, 
			final EntityEvent event) {
		return nextHost-> {
			replicated.getCommitTree().addEvent(
					event, 
					EntityState.PREPARED, 
					nextHost.getShardID(), 
					changePlan.getId());
			changePlan.ship(nextHost, replicated);
		};
	}

	/** @return a filter to permit repication to the shard */
	private Predicate<Shard> predicate(
			final EntityEvent action, 
			final Shard leader, 
			final ShardEntity replicated) {
		
		final CommitedState cs = scheme.getCommitedState();
		return probHost -> (
			// stocking
			(action == EntityEvent.ATTACH
				// other but myself (I'll report'em if reelection occurs)
				&& (!leader.getShardID().equals(probHost.getShardID())
					// avoid repeating event
					&& !cs.getReplicasByShard(probHost).contains(replicated)
					// avoid stocking where's already attached (they'll report'em in reelection)
					&& !cs.getDutiesByShard(probHost).contains(replicated)))
			// dropping
			|| (action == EntityEvent.DETACH
				// everywhere it's stocked in
				&& cs.getReplicasByShard(probHost).contains(replicated))
		);
	}

}