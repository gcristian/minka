package io.tilt.minka.core.leader.distributor;

import static io.tilt.minka.domain.EntityEvent.ATTACH;
import static io.tilt.minka.domain.EntityEvent.CREATE;
import static io.tilt.minka.domain.EntityEvent.DETACH;
import static io.tilt.minka.domain.EntityEvent.DROP;
import static io.tilt.minka.domain.EntityEvent.REMOVE;
import static io.tilt.minka.domain.EntityEvent.STOCK;

import java.util.Set;
import java.util.function.Function;
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
 * those duties affected by Creating and Removing root causes.
 * 
 * Stocked replicas are not balanced among the shards.
 */
class Replicator {

	private final Scheme scheme;

	Replicator(final Scheme state) {
		this.scheme = state;
	}
	
	/** @return TRUE if any changes has been applied to plan */
	boolean write(final ChangePlan changePlan, 
			final Set<ShardEntity> creations, 
			final Set<ShardEntity> deletions,
			final NetworkShardIdentifier leaderId,
			final Pallet p) {
		
		final Shard leader = scheme.getCommitedState().findShard(leaderId.getId());
		// those of current plan
		boolean stocks = dispatchNewLocals(CREATE, ATTACH, STOCK, changePlan, creations, leader, p);
		final boolean drops = dispatchNewLocals(REMOVE, DETACH, DROP, changePlan, deletions, null, p);
		// those of older plans (new followers may have turned online)
		stocks |=dispatchKnownActive(scheme, changePlan, p);
		return stocks || drops;
	}
	
	private boolean dispatchNewLocals(
			final EntityEvent evidence,
			final EntityEvent action,
			final EntityEvent reaction,
			final ChangePlan changePlan, 
			final Set<ShardEntity> involved,
			final Shard main,
			final Pallet p) {
		
		final boolean[] r = {false};
		final CommitedState cs = scheme.getCommitedState();
		// those being shipped for the action event
		changePlan.onDispatchesFor(action, main, (target, duty)-> {
			// same pallet, and present as new CRUD (involved)
			if (duty.getDuty().getPalletId().equals(p.getId()) && involved.contains(duty)) {
				// search back the evidence event as authentic purpose to reaction
				r[0] |=cs.findShardsAnd(
						availabilityRule(action, target, duty),
						replicate(changePlan, duty, reaction)
				);
			}
		});
		return r[0];
	}
	
	private boolean dispatchKnownActive(
			final Scheme state, 
			final ChangePlan changePlan,
			final Pallet pallet) {
				
		final CommitedState cs = state.getCommitedState();
		final boolean[] r = {false};
		cs.findDutiesByPallet(pallet, replica-> {
			// of course avoid those going to die
			if (!changePlan.isDispatching(replica, 
					EntityEvent.DETACH, 
					EntityEvent.DROP, 
					EntityEvent.REMOVE)) {
				r[0] |= cs.findShardsAnd(
					availabilityRule(ATTACH, null, replica), 
					replicate(changePlan, replica, STOCK)
				);
			}
		});
		return r[0];
	}

	/** it might be stock or drop */
	private static Function<Shard, Boolean> replicate(
			final ChangePlan changePlan, 
			final ShardEntity replicated, 
			final EntityEvent event) {
		return nextHost-> {
			replicated.getCommitTree().addEvent(
					event, 
					EntityState.PREPARED, 
					nextHost.getShardID(), 
					changePlan.getId());
			changePlan.dispatch(nextHost, replicated);
			return true;
		};
	}

	/** @return a filter to validate replication to the shard */
	private Predicate<Shard> availabilityRule(
			final EntityEvent action, 
			final Shard target, 
			final ShardEntity replicated) {
		
		final CommitedState cs = scheme.getCommitedState();
		return probHost -> (
				probHost.getState().isAlive() && 
			// stocking
			((action == ATTACH
				// other but myself (I'll report'em if reelection occurs)
				&& ((target==null || !target.getShardID().equals(probHost.getShardID()))
					// avoid repeating event
				    && !cs.getReplicasByShard(probHost).contains(replicated)
					// avoid stocking where's already attached (they'll report'em in reelection)
					&& !cs.getDutiesByShard(probHost).contains(replicated)))
			// dropping
			|| (action == DETACH
				// everywhere it's stocked in
				&& cs.getReplicasByShard(probHost).contains(replicated))
			)
		);
	}

}