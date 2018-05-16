package io.tilt.minka.api.config;

public class ProctorSettings {

	public static final int MAX_HEARBEATS_TO_EVALUATE = 10;
	public final static int MAX_SHARD_CHANGES_TO_HOLD = 5;
	
	protected final static long START_DELAY_BEATS = 1;
	
	private long startDelayBeats;
	protected final static long DELAY_BEATS = 3; // i jhad it on 2000
	private long delayBeats;
	protected static final int MAX_SHARD_JOINING_STATE_BEATS = 15;
	private int maxShardJoiningStateBeats;
	
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
	protected static final int HEARTBEAT_LAPSE_BEATS = 30;
	private int heartbeatLapseBeats;
	protected static final double HEARTBEAT_MAX_DISTANCE_STANDARD_DEVIATION = 4;
	private double heartbeatMaxDistanceStandardDeviation;
	protected static final int CLUSTER_HEALTH_STABILITY_DELAY_PERIODS = 1;
	private int clusterHealthStabilityDelayPeriods;
	
	public int getMinHealthlyHeartbeatsForShardOnline() {
		return this.minHealthlyHeartbeatsForShardOnline;
	}
	public long getStartDelayBeats() {
		return startDelayBeats;
	}
	public void setStartDelayBeats(long startDelayBeats) {
		this.startDelayBeats = startDelayBeats;
	}
	public long getDelayBeats() {
		return delayBeats;
	}
	public void setDelayBeats(long delayBeats) {
		this.delayBeats = delayBeats;
	}
	public int getMaxShardJoiningStateBeats() {
		return maxShardJoiningStateBeats;
	}
	public void setMaxShardJoiningStateBeats(int maxShardJoiningStateBeats) {
		this.maxShardJoiningStateBeats = maxShardJoiningStateBeats;
	}
	public int getHeartbeatLapseBeats() {
		return heartbeatLapseBeats;
	}
	public void setHeartbeatLapseBeats(int heartbeatLapseBeats) {
		this.heartbeatLapseBeats = heartbeatLapseBeats;
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
	public int getClusterHealthStabilityDelayPeriods() {
		return this.clusterHealthStabilityDelayPeriods;
	}
	public void setClusterHealthStabilityDelayPeriods(int clusterHealthStabilityDelayPeriods) {
		this.clusterHealthStabilityDelayPeriods = clusterHealthStabilityDelayPeriods;
	}

}