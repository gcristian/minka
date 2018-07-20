package io.tilt.minka.shard;

public enum TransitionCause {
	
	INIT("Initializing"),
	// recognition of a shard who was too much time without beats
	BECAME_ANCIENT("BecameAncient"),
	// too many beats falling below distance and deviation factor to stay online 
	MAX_SICK_FOR_ONLINE("MaxSickForOnline"),
	// too few healthly beats to be useful (after a healthly phase)
	MIN_ABSENT("MinAbsent"),
	// reaching or escaping quarantine-online frontier   
	HEALTHLY_THRESHOLD("MinHealthly"),
	// too few beats yet 
	FEW_HEARTBEATS("FewHeartbeats"),
	// too much time joining
	JOINING_STARVED("JoiningStarved"),
	// follower quitted fine
	FOLLOWER_BREAKUP("FollowerBreakUp"),
	SWITCH_BACK("SwitchBack"),
	;
	
	final String code;
	
	TransitionCause(final String code) {
		this.code = code;
	}
	
	public String getCode() {
		return code;
	}
	
	@Override
	public String toString() {
		return getCode();
	}
	
}