package io.tilt.minka.core.leader.distributor;

import static io.tilt.minka.domain.EntityEvent.ATTACH;
import static io.tilt.minka.domain.EntityEvent.DETACH;
import static io.tilt.minka.domain.EntityEvent.DROP;
import static io.tilt.minka.domain.EntityEvent.REMOVE;
import static io.tilt.minka.domain.EntityEvent.STOCK;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.tilt.minka.api.Pallet;
import io.tilt.minka.core.leader.data.CommittedState;
import io.tilt.minka.core.leader.data.Scheme;
import io.tilt.minka.domain.EntityEvent;
import io.tilt.minka.domain.EntityState;
import io.tilt.minka.domain.ShardEntity;
import io.tilt.minka.shard.NetworkShardIdentifier;
import io.tilt.minka.shard.Shard;

/**
 * Replication mechanism know-how to allow LeaderBootstrap fall.
 * I.e.: leader's post-load affected duties by all CRUD operations (after distribution)
 * to be reported by SURVIVING followers, to the new LeaderBootstrap.   
 * 
 * More specifically: dispatches {@linkplain EntityEvent.STOCK and DROP} to the {@link ChangePlan}
 * those duties affected by Creating and Removing root causes.
 * 
 * Stocked replicas are not balanced among the shards.
 * 
 * @since Jul 15, 2018
 * 
 */
class Replicator {

	private final static Logger logger = LoggerFactory.getLogger(Replicator.class);

	private final Scheme scheme;
	private final NetworkShardIdentifier leaderId;
	
	Replicator(
			final NetworkShardIdentifier leaderId,
			final Scheme state) {
		this.leaderId = leaderId;
		this.scheme = state;
	}
	
	/** @return TRUE if any changes has been applied to plan */
	boolean write(final ChangePlan changePlan, final DirtyCompiler compiler, final Pallet p) {
		
		final Shard leader = scheme.getCommitedState().findShard(leaderId.getId());
		// those of current plan
		boolean stocks = dispatchNew(ATTACH, changePlan, compiler.getCreations(), leader, p);
		final boolean drops = dispatchNew(DETACH, changePlan, compiler.getDeletions(), null, p);
		// those of older plans (new followers may have turned online)
		stocks |=dispatchKnownActive(changePlan, p);
		return stocks || drops;
	}
	
	private boolean dispatchNew(
			final EntityEvent event,
			final ChangePlan changePlan, 
			final Set<ShardEntity> involved,
			final Shard leader,
			final Pallet p) {
		
		final boolean[] r = {false};
		final CommittedState cs = scheme.getCommitedState();
		// those being shipped for the action event
		final Collection<ShardEntity> dispatchingAllocations = new ArrayList<>();
		changePlan.onDispatchesFor(event, null, (shard, duty)-> dispatchingAllocations.add(duty));
		
		for (ShardEntity duty: dispatchingAllocations) {
			// same pallet, and present as new CRUD (involved)
			if (duty.getDuty().getPalletId().equals(p.getId()) 
					&& involved.contains(duty)) {
				r[0] |=cs.findShardsAnd(
						availabilityRule(event, leader, duty)
							.and(shard-> ! changePlan.isDispatching(duty, shard, event)),
						replicate(changePlan, duty, event.typeSibling(), "dispatchNew")
				);
			}
		}
		if (event==DETACH) {
			for (ShardEntity duty: involved) {
				if (duty.getDuty().getPalletId().equals(p.getId())) {
					r[0] |=cs.findShardsAnd(
							availabilityRule(event, leader, duty),
							// thou not suppored: double events same shard/entity: generates inconsistent state
							// (not deletable entity)
								//.and(shard-> ! changePlan.isDispatching(duty, shard, event)),
							replicate(changePlan, duty, event.typeSibling(), "dispatchNewDetach")
					);
				}
	
			}
		}
		return r[0];
	}

	private boolean dispatchKnownActive(
			final ChangePlan changePlan,
			final Pallet pallet) {
				
		final CommittedState state = scheme.getCommitedState();
		final boolean[] r = {false};
		state.findDutiesByPallet(pallet, duty-> {
			// of course avoid those going to die (drop, remove)
			if (!changePlan.isDispatching(duty, DROP) 
					&& (!changePlan.isDispatching(duty, DETACH) 
							|| changePlan.isDispatching(duty, ATTACH))) {
				r[0] |= state.findShardsAnd(
					availabilityRule(ATTACH, null, duty)
						// leader doesnt needs the replication
						.and(shard-> ! leaderId.equals(shard.getShardID()))
						// avoid two-events on same shard/entity (not supported)
						.and(shard-> ! changePlan.isDispatching(duty, shard, ATTACH)),
					replicate(changePlan, duty, STOCK, "knownActive")
				);
			}
		});
		return r[0];
	}

	/** it might be stock or drop */
	private static Function<Shard, Boolean> replicate(
			final ChangePlan changePlan, 
			final ShardEntity replicated, 
			final EntityEvent event,
			final String tag) {
		return nextHost-> {
			replicated.getCommitTree().addEvent(
					event, 
					EntityState.PREPARED, 
					nextHost.getShardID(), 
					changePlan.getId());
			logger.info("Replicator: {} duty {} to {} ({})", event.toVerb(), replicated.getDuty().getId(), nextHost, tag);
			changePlan.dispatch(nextHost, replicated);
			return true;
		};
	}

	/** @return a filter to validate replication to the shard */
	private Predicate<Shard> availabilityRule(
			final EntityEvent event, 
			final Shard shard, 
			final ShardEntity duty) {
		
		final CommittedState cs = scheme.getCommitedState();
		return probHost -> (
				probHost.getState().isAlive() && 
			// stocking
			((event == ATTACH
				// among the shards: avoid the passed argument.
				&& ((shard==null || !shard.getShardID().equals(probHost.getShardID()))
					// avoid repeating event
				    && !cs.getReplicasByShard(probHost).contains(duty)
					// avoid stocking where's already attached (they'll report'em in reelection)
					&& !cs.getDutiesByShard(probHost).contains(duty)))
			// dropping
			|| (event == DETACH
				// everywhere it's stocked in
				&& cs.getReplicasByShard(probHost).contains(duty))
			)
		);
	}

}