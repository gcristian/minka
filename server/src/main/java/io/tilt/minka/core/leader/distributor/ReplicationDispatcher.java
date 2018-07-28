package io.tilt.minka.core.leader.distributor;

import java.util.Set;

import io.tilt.minka.api.Pallet;
import io.tilt.minka.core.leader.data.ShardingState;
import io.tilt.minka.domain.EntityEvent;
import io.tilt.minka.domain.EntityState;
import io.tilt.minka.domain.ShardEntity;
import io.tilt.minka.shard.NetworkShardIdentifier;
import io.tilt.minka.shard.Shard;

/**
 * Replication mechanism know-how on spreading to followers
 */
public class ReplicationDispatcher {

	private final ShardingState state;

	ReplicationDispatcher(final ShardingState state) {
		this.state = state;
	}
	
	void dispatchReplicas(final ChangePlan changePlan, 
			final Set<ShardEntity> creations, 
			final Set<ShardEntity> deletions,
			final NetworkShardIdentifier leaderId,
			final Pallet p) {
		
		final Shard leader = state.getCommitedState().findShard(leaderId.getId());
		// those of current plan
		dispatchLocalAllocations(EntityEvent.ATTACH, EntityEvent.STOCK, 
				changePlan, creations, leader, p);
		dispatchLocalAllocations(EntityEvent.DETACH, EntityEvent.DROP, 
				changePlan, deletions, leader, p);
		// those of older plans (new followers may have turned online)
		dispatchLocalAttached(state, changePlan, p, leader);
	}
	
	/** ChangePlan::ship leader's follower allocated duties (curr plan) to all followers */
	private void dispatchLocalAllocations(
			final EntityEvent cause,
			final EntityEvent effect,
			final ChangePlan changePlan, 
			final Set<ShardEntity> involved,
			final Shard leader,
			final Pallet p) {
		
		// not all de/allocations, only those shipped to leader's follower
		changePlan.onShippingsFor(cause, leader, duty-> { 
			if (duty.getDuty().getPalletId().equals(p.getId()) && involved.contains(duty)) { 				
				state.getCommitedState().findShards(
						shard->!leader.getShardID().equals(shard.getShardID()), 
						follower-> { 
								duty.getJournal().addEvent(
										effect, 
										EntityState.PREPARED, 
										follower.getShardID(), 
										changePlan.getId());
								changePlan.ship(follower, duty);
						}
				);
			}
		});
	
	}

	/**
	 * check if they were created before the shard's online
	 * or if they're already stocked there, dont do it twice ! (warnings arise)
	 */
	private void dispatchLocalAttached(
			final ShardingState state, 
			final ChangePlan changePlan,
			final Pallet pallet,
			final Shard leader) {
				
		state.getCommitedState().findDuties(leader, pallet, committed-> {
			state.getCommitedState().findShards(
				shard-> !leader.getShardID().equals(shard.getShardID()), 
				other-> {
					if (!state.getCommitedState().getReplicasByShard(other).contains(committed)) {
						committed.getJournal().addEvent(
								EntityEvent.STOCK, 
								EntityState.PREPARED, 
								other.getShardID(), 
								changePlan.getId());
						changePlan.ship(other, committed);
					}
				}
			);
		});
	}
}