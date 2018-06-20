package io.tilt.minka.api.config;

public class ProctorSettings {

	public static final int MAX_HEARBEATS_TO_EVALUATE = 10;
	public final static int MAX_SHARD_CHANGES_TO_HOLD = 5;
	
	protected final static long START_DELAY = 1;
	private long startDelay;
	
	protected final static long PHASE_FREQUENCY = 3; // i jhad it on 2000
	private long phaseFrequency;
	protected static final int MAX_SHARD_JOINING_STATE = 15;
	private int maxShardJoiningState;
	
	protected static final int MIN_HEALTHLY_HEARTBEATS_FOR_SHARD_ONLINE = 2;
	private int minHealthlyHeartbeatsForShardOnline;
	protected static final int MIN_ABSENT_HEARTBEATS_BEFORE_SHARD_GONE = 2;
	private int minAbsentHeartbeatsBeforeShardGone;
	protected static final double MAX_HEARTBEAT_RECEPTION_DELAY_FACTOR_FOR_SICK = 3d;
	private double maxHeartbeatReceptionDelayFactorForSick;
	protected static final int MAX_SICK_HEARTBEATS_BEFORE_SHARD_QUARANTINE = 5;
	private int maxSickHeartbeatsBeforeShardQuarantine;
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
	public void setMaxShardJoiningState(int maxShardJoiningState) {
		this.maxShardJoiningState = maxShardJoiningState;
	}
	public int getHeartbeatLapse() {
		return heartbeatLapse;
	}
	public void setHeartbeatLapse(int heartbeatLapse) {
		this.heartbeatLapse = heartbeatLapse;
	}
	public void setMinHealthlyHeartbeatsForShardOnline(int minHealthlyHeartbeatsForShardOnline) {
		this.minHealthlyHeartbeatsForShardOnline = minHealthlyHeartbeatsForShardOnline;
	}
	public int getMinAbsentHeartbeatsBeforeShardGone() {
		return this.minAbsentHeartbeatsBeforeShardGone;
	}
	public void setMinAbsentHeartbeatsBeforeShardGone(int minAbsentHeartbeatsBeforeShardGone) {
		this.minAbsentHeartbeatsBeforeShardGone = minAbsentHeartbeatsBeforeShardGone;
	}
	public double getMaxHeartbeatReceptionDelayFactorForSick() {
		return this.maxHeartbeatReceptionDelayFactorForSick;
	}
	public void setMaxHeartbeatReceptionDelayFactorForSick(double maxHeartbeatReceptionDelayFactorForSick) {
		this.maxHeartbeatReceptionDelayFactorForSick = maxHeartbeatReceptionDelayFactorForSick;
	}
	public int getMaxSickHeartbeatsBeforeShardQuarantine() {
		return this.maxSickHeartbeatsBeforeShardQuarantine;
	}
	public void setMaxSickHeartbeatsBeforeShardQuarantine(int maxSickHeartbeatsBeforeShardQuarantine) {
		this.maxSickHeartbeatsBeforeShardQuarantine = maxSickHeartbeatsBeforeShardQuarantine;
	}
	public int getMinShardsOnlineBeforeSharding() {
		return this.minShardsOnlineBeforeSharding;
	}
	public void setMinShardsOnlineBeforeSharding(int minShardsOnlineBeforeSharding) {
		this.minShardsOnlineBeforeSharding = minShardsOnlineBeforeSharding;
	}
	public double getHeartbeatMaxBiggestDistanceFactor() {
		return this.heartbeatMaxBiggestDistanceFactor;
	}
	public void setHeartbeatMaxBiggestDistanceFactor(double heartbeatMaxBiggestDistanceFactor) {
		this.heartbeatMaxBiggestDistanceFactor = heartbeatMaxBiggestDistanceFactor;
	}
	public double getHeartbeatMaxDistanceStandardDeviation() {
		return this.heartbeatMaxDistanceStandardDeviation;
	}
	public void setHeartbeatMaxDistanceStandardDeviation(double heartbeatMaxDistanceStandardDeviation) {
		this.heartbeatMaxDistanceStandardDeviation = heartbeatMaxDistanceStandardDeviation;
	}
	public int getClusterHealthStabilityDelayPhases() {
		return this.clusterHealthStabilityDelayPhases;
	}
	public void setClusterHealthStabilityDelayPhases(int clusterHealthStabilityDelayPhases) {
		this.clusterHealthStabilityDelayPhases = clusterHealthStabilityDelayPhases;
	}

}