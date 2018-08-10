package io.tilt.minka.core.leader.distributor;

import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Predicate;

import io.tilt.minka.api.Pallet;
import io.tilt.minka.core.leader.data.CommitedState;
import io.tilt.minka.core.leader.data.Scheme;
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
 * those duties affected by Attaching and Dettaching root causes.
 * 
 * Stocked replicas are not balanced: they're not dropped, except caused by Client remove. 
 * So eventually, everybody wil have everything. 
 * 
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
		dispatchNewLocals(EntityEvent.ATTACH, EntityEvent.STOCK, 
				changePlan, creations, leader, p);
		
		// el detach esta muy mal: donde se haga un REMOVE (no detach) tengo que enviar luego un DROP
		dispatchNewLocals(EntityEvent.REMOVE, EntityEvent.DROP, 
				changePlan, deletions, null, p);
		
		// those of older plans (new followers may have turned online)
		dispatchCurrentLocals(scheme, changePlan, p, leader);
	}
	
	/** ChangePlan::ship leader's follower allocated duties (curr plan) to all followers */
	private void dispatchNewLocals(
			final EntityEvent cause,
			final EntityEvent effect,
			final ChangePlan changePlan, 
			final Set<ShardEntity> involved,
			final Shard target,
			final Pallet p) {
		
		final CommitedState cs = scheme.getCommitedState();
		// not all de/allocations, only those shipped to leader's local follower (same shard)
		changePlan.onShippingsFor(cause, target, duty-> {
			// same pallet, and present as new CRUD (involved)
			if (duty.getDuty().getPalletId().equals(p.getId()) && involved.contains(duty)) { 				
				cs.findShards(
						attachPredicate(target, duty),
						replicate(changePlan, duty, effect)
				);
			}
		});
	
	}
	
	private void dispatchKillingLocals(
			final EntityEvent cause,
			final EntityEvent effect,
			final ChangePlan changePlan, 
			final Set<ShardEntity> involved,
			final Pallet p) {
		
		final CommitedState cs = scheme.getCommitedState();
		// not all de/allocations, only those shipped to leader's local follower (same shard)
		changePlan.onShippingsFor(cause, null, duty-> {
			// same pallet, and present as new CRUD (involved)
			if (duty.getDuty().getPalletId().equals(p.getId()) && involved.contains(duty)) { 				
				cs.findShards(
						removePredicate(target, duty),
						replicate(changePlan, duty, effect)
				);
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
		cs.findDuties(leader, pallet, replic-> {
			cs.findShards(
					attachPredicate(leader, replic), 
					replicate(changePlan, replic, EntityEvent.STOCK)
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

	/** @return a filter to other shards than current and not already present on host */
	private Predicate<Shard> attachPredicate(final Shard leader, final ShardEntity replicated) {
		final CommitedState cs = scheme.getCommitedState();
		return probHost-> !leader.getShardID().equals(probHost.getShardID())
			&& !cs.getReplicasByShard(probHost).contains(replicated)
			&& !cs.getDutiesByShard(probHost).contains(replicated);
	}
	private Predicate<Shard> removePredicate(final Shard leader, final ShardEntity replicated) {
		final CommitedState cs = scheme.getCommitedState();
		return probHost-> (leader == null || (leader!=null &&!leader.getShardID().equals(probHost.getShardID())))
			&& cs.getReplicasByShard(probHost).contains(replicated);
	}
}