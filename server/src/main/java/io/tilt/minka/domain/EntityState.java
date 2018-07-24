package io.tilt.minka.domain;

/**
 * states while the duty travels along the wire and the action is confirmed
 * because it takes time, and inconsistencies will happen
 */
public enum EntityState {
	/** when created */
	PREPARED('p'),
	/** status at leader after being sent */
	PENDING('n'),
	/** status at followet when arrives */
	ACK('r'),
	/** status at leader after the effect is confirmed */
	COMMITED('c'),
	/** status at leader when a follower falls, and at follower when absent in its delegate's report */
	DANGLING('d'),
	/** suddenly stop being reported from follower: no solution yet */
	MISSING('m'),
	/** status at a leader or follower when there's no viable solution for a duty */
	STUCK('s'),
	/** status at a follower when absent in delegate's report, only for lazy ones */
	FINALIZED('f')
	;

	private final char code;

	EntityState(final char c) {
		this.code = c;
	}

	public EntityState fromCode(final char code) {
		for (EntityState s : EntityState.values()) {
			if (s.code == code) {
				return s;
			}
		}
		throw new IllegalArgumentException("shardentity state code: " + code + " not exists");
	}
}