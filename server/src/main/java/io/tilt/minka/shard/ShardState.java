package io.tilt.minka.shard;

import java.util.function.Predicate;

public enum ShardState {
	/** all nodes START in this state while becoming Online after a Quarantine period */
	JOINING,
	/** the node has been continuously online for a long time so it can trustworthly receive work */
	ONLINE,
	/** the node interrupted heartbeats time enough to be considered not healthly
	 * online. in this state all nodes tend to rapidly go ONLINE or fall GONE */
	QUARANTINE,
	/** the node emited a last heartbeat announcing offline mode either being
	 * manually stopped or cleanly shuting down so its ignored by the master */
	QUITTED,
	/** the server discontinued heartbeats and cannot longer be considered alive,
	 * recover its reserved duties */
	GONE
	;
	public boolean isAlive() {
		return this == ONLINE || this == QUARANTINE || this == JOINING;
	}
	public Predicate<Shard> filter() {
		return shard->shard.getState()==this;
	}
	public Predicate<Shard> negative() {
		return shard->shard.getState()!=this;
	}
}