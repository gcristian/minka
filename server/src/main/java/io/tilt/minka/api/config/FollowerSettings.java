package io.tilt.minka.api.config;

public class FollowerSettings {
	
	/**
	 * When true. the heartbeat factory doesnt compares duty definition between
	 * the one distributed by the leader and the reported by the follower 
	 */
	protected static final boolean HEARTBEAT_DUTY_DIFF_TOLERANT = true;
	private boolean heartbeatDutyDiffTolerant;
	
	/*
	 * How many beats delay to start sending heartbeats 
	 */
	protected static final long HEARTBEAT_START_DELAY = 1;
	private long heartbeatStartDelay;
	/*
	 * Frequency for sending heartbeats
	 */
	protected static final long HEARTBEAT_FREQUENCY = 2;
	private long heartbeatFrequency;
	/* 10 seconds old max for clearance before releasing duties */
	protected static final int CLEARANCE_MAX_ABSENCE = 10;
	private int clearanceMaxAbsence;
	protected static final long MAX_HEARTBEAT_ABSENCE_FOR_RELEASE = 10;
	private long maxHeartbeatAbsenceForRelease;
	
	/* 10 errors tolerant for building HBs from followers */
	protected static final int MAX_HEARTBEAT_BUILD_FAILS_BEFORE_RELEASING = 1;
	private int maxHeartbeatBuildFailsBeforeReleasing;

	public int getMaxHeartbeatBuildFailsBeforeReleasing() {
		return this.maxHeartbeatBuildFailsBeforeReleasing;
	}
	public long getHeartbeatFrequency() {
		return heartbeatFrequency;
	}
	public void setHeartbeatFrequency(long heartbeatFrequency) {
		this.heartbeatFrequency = heartbeatFrequency;
	}
	public long getHeartbeatStartDelay() {
		return heartbeatStartDelay;
	}
	public void setHeartbeatStartDelay(long heartbeatStartDelay) {
		this.heartbeatStartDelay = heartbeatStartDelay;
	}
	public int getClearanceMaxAbsence() {
		return clearanceMaxAbsence;
	}
	public void setClearanceMaxAbsence(int clearanceMaxAbsence) {
		this.clearanceMaxAbsence = clearanceMaxAbsence;
	}
	public long getMaxHeartbeatAbsenceForRelease() {
		return maxHeartbeatAbsenceForRelease;
	}
	public void setMaxHeartbeatAbsenceForRelease(long maxHeartbeatAbsenceForRelease) {
		this.maxHeartbeatAbsenceForRelease = maxHeartbeatAbsenceForRelease;
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