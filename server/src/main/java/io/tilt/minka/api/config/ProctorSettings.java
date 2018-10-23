package io.tilt.minka.api.config;

public class ProctorSettings {

	public static final int MAX_HEARBEATS_TO_EVALUATE = 10;
	public final static int MAX_SHARD_CHANGES_TO_HOLD = 5;
	
	protected final static long START_DELAY = 1;
	private long startDelay;
	
	protected final static long PHASE_FREQUENCY = 2; // i jhad it on 2000
	private long phaseFrequency;
	protected static final int MAX_SHARD_JOINING_STATE = 15;
	private int maxShardJoiningState;
	
	protected static final int MIN_HEALTHLY_HEARTBEATS_FOR_SHARD_ONLINE = 2;
	private int minHealthlyHeartbeatsForShardOnline;
	protected static final int MIN_ABSENT_HEARTBEATS_BEFORE_SHARD_GONE = 2;
	private int minAbsentHeartbeatsBeforeShardGone;
	protected static final double MAX_HEARTBEAT_RECEPTION_DELAY_FACTOR_FOR_SICK = 3d;
	private double maxHeartbeatReceptionDelayFactorForSick;
	protected static final int MAX_SICK_HEARTBEATS_BEFORE_SHARD_DELAYED = 5;
	private int maxSickHeartbeatsBeforeShardDelayed;
	protected static final int MIN_SHARDS_ONLINE_BEFORE_SHARDING = 1;
	private int minShardsOnlineBeforeSharding;
	protected static final double HEARTBEAT_MAX_BIGGEST_DISTANCE_FACTOR = 2.5d;
	private double heartbeatMaxBiggestDistanceFactor;
	protected static final int HEARTBEAT_LAPSE = 30;
	private int heartbeatLapse;
	protected static final double HEARTBEAT_MAX_DISTANCE_STANDARD_DEVIATION = 4;
	private double heartbeatMaxDistanceStandardDeviation;
	protected static final int CLUSTER_HEALTH_STABILITY_DELAY_PHASES = 1;
	private int clusterHealthStabilityDelayPhases;
	
	public int getMinHealthlyHeartbeatsForShardOnline() {
		return this.minHealthlyHeartbeatsForShardOnline;
	}
	public long getStartDelay() {
		return startDelay;
	}
	public void setStartDelayBeats(long startDelayBeats) {
		this.startDelay = startDelayBeats;
	}
	public long getPhaseFrequency() {
		return phaseFrequency;
	}
	public void setPhaseFrequency(long frequency) {
		this.phaseFrequency = frequency;
	}
	public int getMaxShardJoiningState() {
		return maxShardJoiningState;
	}
	public void setMaxShardJoiningState(int value) {
		this.maxShardJoiningState = value;
	}
	public int getHeartbeatLapse() {
		return heartbeatLapse;
	}
	public void setHeartbeatLapse(int value) {
		this.heartbeatLapse = value;
	}
	public void setMinHealthlyHeartbeatsForShardOnline(int value) {
		this.minHealthlyHeartbeatsForShardOnline = value;
	}
	public int getMinAbsentHeartbeatsBeforeShardGone() {
		return this.minAbsentHeartbeatsBeforeShardGone;
	}
	public void setMinAbsentHeartbeatsBeforeShardGone(int value) {
		this.minAbsentHeartbeatsBeforeShardGone = value;
	}
	public double getMaxHeartbeatReceptionDelayFactorForSick() {
		return this.maxHeartbeatReceptionDelayFactorForSick;
	}
	public void setMaxHeartbeatReceptionDelayFactorForSick(double value) {
		this.maxHeartbeatReceptionDelayFactorForSick = value;
	}
	public int getMaxSickHeartbeatsBeforeShardDelayed() {
		return this.maxSickHeartbeatsBeforeShardDelayed;
	}
	public void setMaxSickHeartbeatsBeforeShardDelayed(int value) {
		this.maxSickHeartbeatsBeforeShardDelayed = value;
	}
	public int getMinShardsOnlineBeforeSharding() {
		return this.minShardsOnlineBeforeSharding;
	}
	public void setMinShardsOnlineBeforeSharding(int value) {
		this.minShardsOnlineBeforeSharding = value;
	}
	public double getHeartbeatMaxBiggestDistanceFactor() {
		return this.heartbeatMaxBiggestDistanceFactor;
	}
	public void setHeartbeatMaxBiggestDistanceFactor(double value) {
		this.heartbeatMaxBiggestDistanceFactor = value;
	}
	public double getHeartbeatMaxDistanceStandardDeviation() {
		return this.heartbeatMaxDistanceStandardDeviation;
	}
	public void setHeartbeatMaxDistanceStandardDeviation(double value) {
		this.heartbeatMaxDistanceStandardDeviation = value;
	}
	public int getClusterHealthStabilityDelayPhases() {
		return this.clusterHealthStabilityDelayPhases;
	}
	public void setClusterHealthStabilityDelayPhases(int value) {
		this.clusterHealthStabilityDelayPhases = value;
	}

}