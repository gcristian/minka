package io.tilt.minka.core.leader.data;

import io.tilt.minka.domain.EntityEvent;
import io.tilt.minka.domain.EntityEvent.Type;

public enum CommitState {
	/** as arrived to leader from client's follower: before distribution */
	INIT(null),
	/** once dispatched to follower by distributor phase */
	ENTITY_PROCESSING(null),
	/** after reaching {@linkplain EntityEvent.Type.REPLICA} expected state */
	REPLICA_COMMITTED(EntityEvent.Type.REPLICA),
	/** after reaching {@linkplain EntityEvent.Type.ALLOC} expected state */
	ALLOC_COMMITTED(EntityEvent.Type.ALLOC),
	;
	
	private Type type;

	CommitState(final EntityEvent.Type type) {
		this.type = type;
	}
	public Type getType() {
		return type;
	}
}