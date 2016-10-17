/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package io.tilt.minka.api;

import java.io.File;
import java.util.Properties;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.datatype.joda.ser.DateTimeSerializer;

import io.tilt.minka.api.Pallet.Storage;
import io.tilt.minka.core.leader.distributor.Balancer.Strategy;
import io.tilt.minka.core.leader.distributor.EvenLoadBalancer.PreSortType;
import io.tilt.minka.core.leader.distributor.SpillOverBalancer.MaxUnit;
import io.tilt.minka.domain.ShardID;
import io.tilt.minka.utils.Defaulter;

public class Config {

	private final Logger logger = LoggerFactory.getLogger(getClass());
	
	protected static final ObjectMapper objectMapper = new ObjectMapper();
    static {
        objectMapper.configure(SerializationFeature.WRITE_NULL_MAP_VALUES, false);
        objectMapper.configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false);
        objectMapper.configure(SerializationFeature.CLOSE_CLOSEABLE, true);
        objectMapper.configure(SerializationFeature.INDENT_OUTPUT, true);
        
        objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, true);

        final SimpleModule simpleModule = new SimpleModule("module-1");
        simpleModule.addSerializer(DateTime.class, new DateTimeSerializer());
        objectMapper.registerModule(simpleModule);
    }
	
	@JsonIgnore
	public final DateTime loadTime = new DateTime(DateTimeZone.UTC);
	@JsonIgnore
	private ShardID resolvedShardId;

	private SchedulerConf scheduler;
	private BootstrapConf bootstrap;
	private BrokerConf broker;
	private FollowerConf follower;
	private BalancerConf balancer;
	private DistributorConf distributor;
	private ShepherdConf shepherd;
	private ConsistencyConf consistency;

	public static class SchedulerConf {
		public static String PNAME = "Minka-"; // + serviceName;
		public static String THREAD_NAME_COORDINATOR_IN_BACKGROUND = PNAME + "-Scheduler";
		public static String THREAD_NAME_BROKER_SERVER_GROUP = PNAME + "-SocketServerThreadGroup";
		public static String THREAD_NAME_BROKER_SERVER_WORKER = PNAME + "-SocketServerThreadWorker";
		public static String THREAD_NANE_TCP_BROKER_CLIENT = PNAME + "-SocketClientThread";

		public static long SEMAPHORE_UNLOCK_RETRY_DELAY_MS = 100l; //50l;
		private int semaphoreUnlockRetryDelayMs;
		public static int SEMAPHORE_UNLOCK_MAX_RETRIES = 30;
		private int semaphoreUnlockMaxRetries;
		
		public static String TASK_NAME_FOLLOWER_POLICIES_CLEARANCE = "FollowerPolicyClearance";
		public static String TASK_NAME_FOLLOWER_POLICIES_HEARTATTACK = "FollowerPolicyHeartattack";
		public int getSemaphoreUnlockRetryDelayMs() {
			return this.semaphoreUnlockRetryDelayMs;
		}
		public void setSemaphoreUnlockRetryDelayMs(int semaphoreUnlockRetryDelayMs) {
			this.semaphoreUnlockRetryDelayMs = semaphoreUnlockRetryDelayMs;
		}
		public int getSemaphoreUnlockMaxRetries() {
			return this.semaphoreUnlockMaxRetries;
		}
		public void setSemaphoreUnlockMaxRetries(int semaphoreUnlockMaxRetries) {
			this.semaphoreUnlockMaxRetries = semaphoreUnlockMaxRetries;
		}

	}

	public static class BootstrapConf {
		protected static final String SERVICE_NAME = ("default-name");
		private static String serviceName;
		protected static final long READYNESS_RETRY_DELAY_MS = 5000l;
		private long readynessRetryDelayMs;
		protected final static boolean PUBLISH_LEADER_CANDIDATURE = true;
		private boolean publishLeaderCandidature;
		protected static final boolean LEADER_SHARD_ALSO_FOLLOWS = true;
		private boolean leaderShardAlsoFollows;
		protected static final String ZOOKEEPER_HOST_PORT = "localhost:2181";
		private String zookeeperHostPort;
		
		public String getServiceName() {
			return serviceName;
		}
		public void setServiceName(String serviceName) {
			BootstrapConf.serviceName = serviceName;
		}
		public long getReadynessRetryDelayMs() {
			return this.readynessRetryDelayMs;
		}
		public void setReadynessRetryDelayMs(long readynessRetryDelayMs) {
			this.readynessRetryDelayMs = readynessRetryDelayMs;
		}
		public boolean isPublishLeaderCandidature() {
			return this.publishLeaderCandidature;
		}
		public void setPublishLeaderCandidature(boolean publishLeaderCandidature) {
			this.publishLeaderCandidature = publishLeaderCandidature;
		}
		public boolean isLeaderShardAlsoFollows() {
			return this.leaderShardAlsoFollows;
		}
		public void setLeaderShardAlsoFollows(boolean leaderShardAlsoFollows) {
			this.leaderShardAlsoFollows = leaderShardAlsoFollows;
		}
		public String getZookeeperHostPort() {
			return this.zookeeperHostPort;
		}
		public void setZookeeperHostPort(String zookeeperHostPort) {
			this.zookeeperHostPort = zookeeperHostPort;
		}
		
	}

	public static class BrokerConf {
		protected final static String HOST_PORT = "127.0.0.1:9090";
		private String hostPort;
		protected final static int CONNECTION_HANDLER_THREADS = 10;
		private int connectionHandlerThreads;
		protected final static int MAX_RETRIES = 300;
		private int maxRetries;
		protected final static int RETRY_DELAY_MS = 3000;
		private int retryDelayMs;
		/** True: try number-consecutive open ports if specified is busy, False: break bootup */
		protected static final boolean ENABLE_PORT_FALLBACK = true;
		public boolean enablePortFallback;
		protected static final String SHARD_ID_SUFFIX = "";
		private String shardIdSuffix;
		protected static final String NETWORK_INTERFASE = "lo";
		private String networkInterfase;
		
		public String getHostPort() {
			return this.hostPort;
		}
		public void setHostPort(String hostPort) {
			this.hostPort = hostPort;
		}
		public int getConnectionHandlerThreads() {
			return this.connectionHandlerThreads;
		}
		public void setConnectionHandlerThreads(int connectionHandlerThreads) {
			this.connectionHandlerThreads = connectionHandlerThreads;
		}
		public int getMaxRetries() {
			return this.maxRetries;
		}
		public void setMaxRetries(int maxRetries) {
			this.maxRetries = maxRetries;
		}
		public int getRetryDelayMs() {
			return this.retryDelayMs;
		}
		public void setRetryDelayMs(int retryDelayMs) {
			this.retryDelayMs = retryDelayMs;
		}
		public boolean isEnablePortFallback() {
			return this.enablePortFallback;
		}
		public void setEnablePortFallback(boolean enablePortFallback) {
			this.enablePortFallback = enablePortFallback;
		}
		public String getShardIdSuffix() {
			return this.shardIdSuffix;
		}
		public void setShardIdSuffix(String shardIdSuffix) {
			this.shardIdSuffix = shardIdSuffix;
		}
		public String getNetworkInterfase() {
			return this.networkInterfase;
		}
		public void setNetworkInterfase(String networkInterfase) {
			this.networkInterfase = networkInterfase;
		}
	}

	public static class FollowerConf {
		/* each half second */
		protected static final long HEARTBEAT_START_DELAY_MS = 1000;
		private long heartbeatDelayMs;
		protected static final long HEARTBEAT_DELAY_MS = 2000;
		private long heartbeatStartDelayMs;
		/* 10 seconds enough to start check and release duties if no HB in x time */
		protected static final long HEARTATTACK_CHECK_START_DELAY_MS = 10000;
		private long heartattackCheckStartDelayMs;
		protected static final long HEARTATTACK_CHECK_DELAY_MS = 10000;
		private long heartattackCheckDelayMs;
		/* 20 seconds to let the leader be elected */
		protected static final long CLEARANCE_CHECK_START_DELAY_MS = 20000;
		private long clearanceCheckStartDelayMs;
		protected static final long CLEARANCE_CHECK_DELAY_MS = 10000;
		private long clearanceCheckDelayMs;
		/* 30 seconds old max for clearance before releasing duties */
		protected static final int CLEARANCE_MAX_ABSENCE_MS = 30000;
		private int clearanceMaxAbsenceMs;
		protected static final long MAX_HEARTBEAT_ABSENCE_FOR_RELEASE_MS = 10000;
		private long maxHeartbeatAbsenceForReleaseMs;
		/* 10 errors tolerant for building HBs from followers */
		protected static final int MAX_HEARTBEAT_BUILD_FAILS_BEFORE_RELEASING = 1;
		private int maxHeartbeatBuildFailsBeforeReleasing;
		
		public long getHeartbeatDelayMs() {
			return this.heartbeatDelayMs;
		}
		public void setHeartbeatDelayMs(long heartbeatDelayMs) {
			this.heartbeatDelayMs = heartbeatDelayMs;
		}
		public long getHeartbeatStartDelayMs() {
			return this.heartbeatStartDelayMs;
		}
		public void setHeartbeatStartDelayMs(long heartbeatStartDelayMs) {
			this.heartbeatStartDelayMs = heartbeatStartDelayMs;
		}
		public long getHeartattackCheckStartDelayMs() {
			return this.heartattackCheckStartDelayMs;
		}
		public void setHeartattackCheckStartDelayMs(long heartattackCheckStartDelayMs) {
			this.heartattackCheckStartDelayMs = heartattackCheckStartDelayMs;
		}
		public long getHeartattackCheckDelayMs() {
			return this.heartattackCheckDelayMs;
		}
		public void setHeartattackCheckDelayMs(long heartattackCheckDelayMs) {
			this.heartattackCheckDelayMs = heartattackCheckDelayMs;
		}
		public long getClearanceCheckStartDelayMs() {
			return this.clearanceCheckStartDelayMs;
		}
		public void setClearanceCheckStartDelayMs(long clearanceCheckStartDelayMs) {
			this.clearanceCheckStartDelayMs = clearanceCheckStartDelayMs;
		}
		public long getClearanceCheckDelayMs() {
			return this.clearanceCheckDelayMs;
		}
		public void setClearanceCheckDelayMs(long clearanceCheckDelayMs) {
			this.clearanceCheckDelayMs = clearanceCheckDelayMs;
		}
		public int getClearanceMaxAbsenceMs() {
			return this.clearanceMaxAbsenceMs;
		}
		public void setClearanceMaxAbsenceMs(int clearanceMaxAbsenceMs) {
			this.clearanceMaxAbsenceMs = clearanceMaxAbsenceMs;
		}
		public long getMaxHeartbeatAbsenceForReleaseMs() {
			return this.maxHeartbeatAbsenceForReleaseMs;
		}
		public void setMaxHeartbeatAbsenceForReleaseMs(long maxHeartbeatAbsenceForReleaseMs) {
			this.maxHeartbeatAbsenceForReleaseMs = maxHeartbeatAbsenceForReleaseMs;
		}
		public int getMaxHeartbeatBuildFailsBeforeReleasing() {
			return this.maxHeartbeatBuildFailsBeforeReleasing;
		}
		public void setMaxHeartbeatBuildFailsBeforeReleasing(int maxHeartbeatBuildFailsBeforeReleasing) {
			this.maxHeartbeatBuildFailsBeforeReleasing = maxHeartbeatBuildFailsBeforeReleasing;
		}

	}

	public static class DistributorConf {
		protected static final boolean RUN_CONSISTENCY_CHECK = true;
		private boolean runConsistencyCheck;
		protected static final boolean RELOAD_DUTIES_FROM_STORAGE = false;
		private boolean reloadDutiesFromStorage;
		protected static final int RELOAD_DUTIES_FROM_STORAGE_EACH_PERIODS = 10;
		private int reloadDutiesFromStorageEachPeriods;
		/* 10 seconds to let the Shepherd discover all Followers before distributing */
		protected final static long START_DELAY_MS = 10000;
		private long startDelayMs;
		protected final static long DELAY_MS = 5000;		
		private long delayMs;
		protected static final int REALLOCATION_EXPIRATION_SEC = 15;
		private int reallocationExpirationSec;
		protected static final int REALLOCATION_MAX_RETRIES = 3;
		private int reallocationMaxRetries;
		
		public boolean isRunConsistencyCheck() {
			return this.runConsistencyCheck;
		}
		public void setRunConsistencyCheck(boolean runConsistencyCheck) {
			this.runConsistencyCheck = runConsistencyCheck;
		}
		public boolean isReloadDutiesFromStorage() {
			return this.reloadDutiesFromStorage;
		}
		public void setReloadDutiesFromStorage(boolean reloadDutiesFromStorage) {
			this.reloadDutiesFromStorage = reloadDutiesFromStorage;
		}
		public int getReloadDutiesFromStorageEachPeriods() {
			return this.reloadDutiesFromStorageEachPeriods;
		}
		public void setReloadDutiesFromStorageEachPeriods(int reloadDutiesFromStorageEachPeriods) {
			this.reloadDutiesFromStorageEachPeriods = reloadDutiesFromStorageEachPeriods;
		}
		public long getStartDelayMs() {
			return this.startDelayMs;
		}
		public void setStartDelayMs(long startDelayMs) {
			this.startDelayMs = startDelayMs;
		}
		public long getDelayMs() {
			return this.delayMs;
		}
		public void setDelayMs(long delayMs) {
			this.delayMs = delayMs;
		}
		public int getReallocationExpirationSec() {
			return this.reallocationExpirationSec;
		}
		public void setReallocationExpirationSec(int reallocationExpirationSec) {
			this.reallocationExpirationSec = reallocationExpirationSec;
		}
		public int getReallocationMaxRetries() {
			return this.reallocationMaxRetries;
		}
		public void setReallocationMaxRetries(int reallocationMaxRetries) {
			this.reallocationMaxRetries = reallocationMaxRetries;
		}
		
	}

	public static class BalancerConf {
		public static final Strategy STRATEGY = Strategy.EVEN_WEIGHT;
		private Strategy strategy;

		public static final int ROUND_ROBIN_MAX_DUTIES_DELTA_BETWEEN_SHARDS = 1;
		private int roundRobinMaxDutiesDeltaBetweenShards;
		
		public static final PreSortType EVEN_LOAD_PRESORT = PreSortType.WEIGHT;
		private PreSortType evenLoadPresort;
		
		public static final MaxUnit SPILL_OVER_MAX_UNIT = MaxUnit.SIZE;
		private MaxUnit spillOverMaxUnit;
		public static final double SPILL_OVER_MAX_VALUE = 99999999999d;
		private double spillOverMaxValue;
		
		public int getRoundRobinMaxDutiesDeltaBetweenShards() {
			return this.roundRobinMaxDutiesDeltaBetweenShards;
		}
		public void setRoundRobinMaxDutiesDeltaBetweenShards(int roundRobinMaxDutiesDeltaBetweenShards) {
			this.roundRobinMaxDutiesDeltaBetweenShards = roundRobinMaxDutiesDeltaBetweenShards;
		}
		public Strategy getStrategy() {
			return this.strategy;
		}
		public void setStrategy(Strategy distributorbalancerStrategy) {
			this.strategy = distributorbalancerStrategy;
		}
		public PreSortType getEvenLoadPresort() {
			return this.evenLoadPresort;
		}
		public void setEvenLoadPresort(PreSortType fairLoadPresort) {
			this.evenLoadPresort = fairLoadPresort; 
		}
		public MaxUnit getSpillOverMaxUnit() {
			return this.spillOverMaxUnit;
		}
		public void setSpillOverStrategy(MaxUnit spillOverStrategy) {
			this.spillOverMaxUnit = spillOverStrategy;
		}
		public double getSpillOverMaxValue() {
			return this.spillOverMaxValue;
		}
		public void setSpillOverMaxValue(double spillOverMaxValue) {
			this.spillOverMaxValue = spillOverMaxValue;
		}
		
	}

	public static class ShepherdConf {
		/* each 3 seconds */
		protected final static long START_DELAY_MS = 500;
		private long startDelayMs;
		protected final static long DELAY_MS = 2000;
		private long delayMs;
		protected static final int MAX_SHARD_JOINING_STATE_MS = 15000;
		private int maxShardJoiningStateMs;
		protected static final int MIN_HEALTHLY_HEARTBEATS_FOR_SHARD_ONLINE = 2;
		private int minHealthlyHeartbeatsForShardOnline;
		protected static final int MAX_ABSENT_HEARTBEATS_BEFORE_SHARD_GONE =5;
		private int maxAbsentHeartbeatsBeforeShardGone;
		protected static final double MAX_HEARTBEAT_RECEPTION_DELAY_FACTOR_FOR_SICK = 3d;
		private double maxHeartbeatReceptionDelayFactorForSick;
		protected static final int MAX_SICK_HEARTBEATS_BEFORE_SHARD_QUARANTINE = 15;
		private int maxSickHeartbeatsBeforeShardQuarantine;
		protected static final int MIN_SHARDS_ONLINE_BEFORE_SHARDING = 1;
		private int minShardsOnlineBeforeSharding;
		protected static final double HEARTBEAT_MAX_BIGGEST_DISTANCE_FACTOR = 2.5d;
		private double heartbeatMaxBiggestDistanceFactor;
		protected static final int HEARTBEAT_LAPSE_SEC = 20;
		private int heartbeatLapseSec;
		protected static final double HEARTBEAT_MAX_DISTANCE_STANDARD_DEVIATION = 4;
		private double heartbeatMaxDistanceStandardDeviation;
		protected static final int CLUSTER_HEALTH_STABILITY_DELAY_PERIODS = 3;
		private int clusterHealthStabilityDelayPeriods;

		public long getStartDelayMs() {
			return this.startDelayMs;
		}
		public void setStartDelayMs(long startDelayMs) {
			this.startDelayMs = startDelayMs;
		}
		public long getDelayMs() {
			return this.delayMs;
		}
		public void setDelayMs(long shepherddDelayMs) {
			this.delayMs = shepherddDelayMs;
		}
		public int getMaxShardJoiningStateMs() {
			return this.maxShardJoiningStateMs;
		}
		public void setMaxShardJoiningStateMs(int maxShardJoiningStateMs) {
			this.maxShardJoiningStateMs = maxShardJoiningStateMs;
		}
		public int getMinHealthlyHeartbeatsForShardOnline() {
			return this.minHealthlyHeartbeatsForShardOnline;
		}
		public void setMinHealthlyHeartbeatsForShardOnline(int minHealthlyHeartbeatsForShardOnline) {
			this.minHealthlyHeartbeatsForShardOnline = minHealthlyHeartbeatsForShardOnline;
		}
		public int getMaxAbsentHeartbeatsBeforeShardGone() {
			return this.maxAbsentHeartbeatsBeforeShardGone;
		}
		public void setMaxAbsentHeartbeatsBeforeShardGone(int maxAbsentHeartbeatsBeforeShardGone) {
			this.maxAbsentHeartbeatsBeforeShardGone = maxAbsentHeartbeatsBeforeShardGone;
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
		public int getHeartbeatLapseSec() {
			return this.heartbeatLapseSec;
		}
		public void setHeartbeatLapseSec(int heartbeatLapseSec) {
			this.heartbeatLapseSec = heartbeatLapseSec;
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

	public static class ConsistencyConf {
		protected static final Storage DUTY_STORAGE = Storage.CLIENT_DEFINED;
		private Storage dutyStorage;
		public Storage getDutyStorage() {
			return this.dutyStorage;
		}
		public void setDutyStorage(Storage dutyStorage) {
			this.dutyStorage = dutyStorage;
		}
	}

	public Config(Properties prop) throws Exception {
		this.scheduler = new SchedulerConf();
		this.bootstrap = new BootstrapConf();
		this.broker = new BrokerConf();
		this.follower = new FollowerConf();
		this.distributor = new DistributorConf();
		this.shepherd = new ShepherdConf();
		this.balancer = new BalancerConf();
		this.consistency = new ConsistencyConf();
		loadFromPropOrSystem(prop);

	}
	private void loadFromPropOrSystem(Properties prop) throws Exception {
		if (prop == null) {
			prop = new Properties();
		}
		Defaulter.apply(prop, "consistency.", this.getConsistency());
		Defaulter.apply(prop, "balancer.", this.getBalancer());
		Defaulter.apply(prop, "bootstrap.", this.getBootstrap());
		Defaulter.apply(prop, "broker.", this.getBroker());
		Defaulter.apply(prop, "distributor.", this.getDistributor());
		Defaulter.apply(prop, "follower.", this.getFollower());
		Defaulter.apply(prop, "scheduler.", this.getScheduler());
		Defaulter.apply(prop, "shepherd.", this.getShepherd());
		logger.info("{}: Configuration: {} ", getClass().getSimpleName(), toJson());
	}
	
	public Config() throws Exception {
		this(null);
	}
	
	public String toJson() throws Exception {
		return objectMapper.writeValueAsString(this);
	}
	public void toJsonFile(final String filepath) throws Exception {
		objectMapper.writeValue(new File(filepath), this);
	}
	
	public static Config fromString(final String json) throws Exception {
		return objectMapper.readValue(json, Config.class);
	}
	
	public static Config fromJsonFile(final String filepath) throws Exception {
		return objectMapper.readValue(filepath, Config.class);
	}

	@JsonIgnore
	public ShardID getLoggingShardId() {
		return this.resolvedShardId;
	}

	public void setResolvedShardId(ShardID resolvedShardId) {
		this.resolvedShardId = resolvedShardId;
	}

	@Override
	public String toString() {
		try {
			return toJson();
		} catch (Exception e) {
			return "Config[unseralizable:" + e.getMessage() +"]";
		}
	}

	public BootstrapConf getBootstrap() {
		return this.bootstrap;
	}

	public void setBootstrap(BootstrapConf bootstrap) {
		this.bootstrap = bootstrap;
	}

	public BrokerConf getBroker() {
		return this.broker;
	}

	public void setBroker(BrokerConf broker) {
		this.broker = broker;
	}

	public FollowerConf getFollower() {
		return this.follower;
	}

	public void setFollower(FollowerConf follower) {
		this.follower = follower;
	}

	public DistributorConf getDistributor() {
		return this.distributor;
	}

	public void setDistributor(DistributorConf distributor) {
		this.distributor = distributor;
	}

	public ShepherdConf getShepherd() {
		return this.shepherd;
	}

	public void setShepherd(ShepherdConf shepherd) {
		this.shepherd = shepherd;
	}

	public DateTime getLoadTime() {
		return this.loadTime;
	}

	public ShardID getResolvedShardId() {
		return this.resolvedShardId;
	}

	public BalancerConf getBalancer() {
		return this.balancer;
	}

	public void setBalancer(BalancerConf balancer) {
		this.balancer = balancer;
	}

	public SchedulerConf getScheduler() {
		return scheduler;
	}

	public void setScheduler(SchedulerConf scheduler) {
		this.scheduler = scheduler;
	}

	public ConsistencyConf getConsistency() {
		return this.consistency;
	}

	public void setConsistency(ConsistencyConf consistency) {
		this.consistency = consistency;
	}

}
