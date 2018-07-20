package io.tilt.minka.shard;

import java.io.Serializable;
import java.time.Instant;
import java.util.Comparator;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

public class Transition implements Comparator<Transition>, Comparable<Transition>, Serializable {
	
	private static final long serialVersionUID = -6140509862684397273L;
	private final TransitionCause transitionCause;
	private final ShardState state;
	private final long timestamp;
	private final String explain;
	
	public Transition(final TransitionCause transitionCause, final ShardState state) {
		super();
		this.transitionCause = transitionCause;
		this.state = state;
		this.timestamp = System.currentTimeMillis();
		this.explain = null;
	}
	public TransitionCause getCause() {
		return transitionCause;
	}
	public ShardState getState() {
		return state;
	}
	@JsonIgnore
	public Instant getTimestamp() {
		return Instant.ofEpochMilli(timestamp);
	}

	@JsonProperty("timestamp")
	public String getTimestamp_() {
		return getTimestamp().toString();
	}
	public String getExplain() {
		return explain;
	}
	@Override
	public int compare(Transition o1, Transition o2) {
		if (o1==null) {
			return 1;
		} else if (o2==null) {
			return -1;
		} else {
			return o1.getTimestamp().compareTo(o2.getTimestamp());
		}
	}
	@Override
	public int compareTo(Transition o) {
		return compare(this, o);
	}
	@Override
	public String toString() {
		return new StringBuilder()
				.append(getTimestamp()).append(' ')
				.append(state).append(' ')
				.append(transitionCause)
				.toString();
	}
}