package io.tilt.minka.api.config;

public class FollowerConfiguration {
	
	/**
	 * When true. the heartbeat factory doesnt compares duty definition between
	 * the one distributed by the leader and the reported by the follower 
	 */
	protected static final boolean HEARTBEAT_DUTY_DIFF_TOLERANT = true;
	private boolean heartbeatDutyDiffTolerant;
	
	/*
	 * How many beats delay to start sending heartbeats 
	 */
	protected static final long HEARTBEAT_START_DELAY_BEATS = 1;
	private long heartbeatDelayBeats;
	/*
	 * Frequency for sending heartbeats
	 */
	protected static final long HEARTBEAT_DELAY_BEATS = 2;
	private long heartbeatStartDelayBeats;				
	/* 10 seconds old max for clearance before releasing duties */
	protected static final int CLEARANCE_MAX_ABSENCE_BEATS = 10;
	private int clearanceMaxAbsenceBeats;
	protected static final long MAX_HEARTBEAT_ABSENCE_FOR_RELEASE_BEATS = 10;
	private long maxHeartbeatAbsenceForReleaseBeats;
	
	/* 10 errors tolerant for building HBs from followers */
	protected static final int MAX_HEARTBEAT_BUILD_FAILS_BEFORE_RELEASING = 1;
	private int maxHeartbeatBuildFailsBeforeReleasing;

	public int getMaxHeartbeatBuildFailsBeforeReleasing() {
		return this.maxHeartbeatBuildFailsBeforeReleasing;
	}
	public long getHeartbeatDelayBeats() {
		return heartbeatDelayBeats;
	}
	public void setHeartbeatDelayBeats(long heartbeatDelayBeats) {
		this.heartbeatDelayBeats = heartbeatDelayBeats;
	}
	public long getHeartbeatStartDelayBeats() {
		return heartbeatStartDelayBeats;
	}
	public void setHeartbeatStartDelayBeats(long heartbeatStartDelayBeats) {
		this.heartbeatStartDelayBeats = heartbeatStartDelayBeats;
	}
	public int getClearanceMaxAbsenceBeats() {
		return clearanceMaxAbsenceBeats;
	}
	public void setClearanceMaxAbsenceBeats(int clearanceMaxAbsenceBeats) {
		this.clearanceMaxAbsenceBeats = clearanceMaxAbsenceBeats;
	}
	public long getMaxHeartbeatAbsenceForReleaseBeats() {
		return maxHeartbeatAbsenceForReleaseBeats;
	}
	public void setMaxHeartbeatAbsenceForReleaseBeats(long maxHeartbeatAbsenceForReleaseBeats) {
		this.maxHeartbeatAbsenceForReleaseBeats = maxHeartbeatAbsenceForReleaseBeats;
	}
	public void setMaxHeartbeatBuildFailsBeforeReleasing(int maxHeartbeatBuildFailsBeforeReleasing) {
		this.maxHeartbeatBuildFailsBeforeReleasing = maxHeartbeatBuildFailsBeforeReleasing;
	}
	public boolean getHeartbeatDutyDiffTolerant() {
		return heartbeatDutyDiffTolerant;
	}
	public void setHeartbeatDutyDiffTolerant(boolean  heartbeatDutyDiffTolerant) {
		this.heartbeatDutyDiffTolerant = heartbeatDutyDiffTolerant;
	}

}