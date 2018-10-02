package io.tilt.minka.core.leader.data;

import io.tilt.minka.domain.EntityEvent;
import io.tilt.minka.domain.EntityEvent.Type;

/**
 * 
 * Repr. the State of a User's operation related to a Duty
 * Both for CREATE and REMOVE
 * 
 * 
 * Flows: 
 * 
 *  At leader  | at Replicated | at corresponding 
 *  		   |   Follower	   |	Follower
 *  
 * 	1) Create  ->   Stock     -> Attach
 *  2) Remove  ->   Drop      -> Detach
 * 
 * This state lies within the leader's realm, once it was replied to the Client
 * with a {@linkplain ReplyValue} successful answer.
 * Otherwise futures obtained by the {@link Reply} wont never get this state.
 * 
 * PHASE					DETAIL
 * --------------------------------------------------------------------------------------------------------
 * Processing				the entity entered distribution phase and was sent to followers
 * 							for replication and allocation as well
 * --------------------------------------------------------------------------------------------------------
 * CommittedReplication		confirmed that the action derived in replication phase 
 * 							and was committed by at least 1 follower.
 * 							in single-node cluster mode: this phase will not exist
 * 		Meaning:
 * 		--------
 * 		Stocking	from now on the system guarantees the surviving of entities across shards lifecycles.
 * 		Dropping	the entity was deleted at replicated locations and wont exist after allocation-phase
 * 							
 * -------------------------------------------------------------------------------------------------------- 
 * CommittedAllocation		confirmed that the action derived in capturing or releasing 
 * 							the entity at the corresponding follower shard.
 * 		Meaning:
 * 		--------
 * 		Attach		from now on the client can assume the entity is balanced and running in a shard
 * 		Detach		from now on the entity doesnt exist in the system
 * --------------------------------------------------------------------------------------------------------
 * 
 */
public enum CommitState {
	/** as arrived to leader from client's follower: before distribution */
	//INIT(EntityEvent.Type.NONE, 0),
	/** once dispatched to follower by distributor phase */
	PROCESSING(EntityEvent.Type.NONE, 1),
	/** after reaching {@linkplain EntityEvent.Type.REPLICA} expected state */
	REPLICATION(EntityEvent.Type.REPLICA, 2),
	/** after reaching {@linkplain EntityEvent.Type.ALLOC} expected state */
	ALLOCATION(EntityEvent.Type.ALLOC, 3),
	/** an internal state only known to leader (not notified to user) */
	FINISHED(EntityEvent.Type.NONE, 4),
	/** the duty operation was cancelled */
	CANCELLED(EntityEvent.Type.NONE, 5),
	/** the operation was unable to process */
	REJECTED(EntityEvent.Type.NONE, 6),
	;
	
	private Type type;
	private int order;

	CommitState(final EntityEvent.Type type, final int order) {
		this.type = type;
		this.order = order;
	}
	public Type getType() {
		return type;
	}
	public boolean notifies() {
		return this==FINISHED;
	}
	
	public boolean isEnded() {
		return this==FINISHED || this==CANCELLED;
	}
	
	public CommitState next(final EntityEvent ee) {
		CommitState ret = null;
		if (this!=FINISHED) {
			if (this==PROCESSING || this==REPLICATION) {
				if (ee.getType()==EntityEvent.Type.ALLOC) { 
					ret = ALLOCATION;
				} else if (ee.getType()==EntityEvent.Type.REPLICA 
						&& this!=REPLICATION) {
					ret = REPLICATION;
				}
			} else if (this==CommitState.ALLOCATION) {
				ret = FINISHED;
			}
		}
		return ret;
	}
	
}