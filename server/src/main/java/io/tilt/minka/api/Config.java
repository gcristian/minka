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
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.datatype.joda.ser.DateTimeSerializer;

import io.tilt.minka.api.Pallet.Storage;
import io.tilt.minka.core.leader.balancer.FairWeightBalancer.Dispersion;
import io.tilt.minka.core.leader.balancer.SpillOverBalancer.MaxUnit;
import io.tilt.minka.core.leader.distributor.Balancer;
import io.tilt.minka.core.leader.distributor.Balancer.PreSort;
import io.tilt.minka.core.leader.distributor.Balancer.Strategy;
import io.tilt.minka.domain.ShardIdentifier;
import io.tilt.minka.utils.Defaulter;

/**
 * All there's subject to vary on mika's behaviour
 * @author Cristian Gonzalez
 * @since Nov 19, 2016
 */
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
	private ShardIdentifier resolvedShardId;

	private SchedulerConf scheduler;
	private BootstrapConf bootstrap;
	private BrokerConf broker;
	private FollowerConf follower;
	private BalancerConf balancer;
	private DistributorConf distributor;
	private ProctorConf proctor;
	private ConsistencyConf consistency;

	public static class SchedulerConf {
		
		// only 1 thread for all other continuous scheduled tasks is enough
		// in case of bigger transportation payloads this can increase
		// as the Scheduler will handle permissions thru Semaphore 
		public static int MAX_CONCURRENCY = 1;
		private int maxConcurrency; 
		public int getMaxConcurrency() {
			return this.maxConcurrency;
		}
		public void setMaxConcurrency(int maxConcurrency) {
			this.maxConcurrency = maxConcurrency;
		}
		public static String PNAME = "MK"; // + serviceName;
		public static String THREAD_NAME_SCHEDULER = PNAME + "Scheduler";
		public static String THREAD_NAME_BROKER_SERVER_GROUP = PNAME + "BrokerServerGroup";
		public static String THREAD_NAME_BROKER_SERVER_WORKER = PNAME + "BrokerServerWorker";
		public static String THREAD_NANE_TCP_BROKER_CLIENT = PNAME + "BrokerClient";
		
		public static String THREAD_NAME_WEBSERVER_WORKER = PNAME + "-grizzly-ws-workers";
		public static String THREAD_NAME_WEBSERVER_KERNEL = PNAME + "-grizzly-ws-kernel";

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
		
		// this sets the pace of all time-synchronized processes
		protected static final long BEAT_UNIT_MS = 500;
		private long beatUnitMs;		
		protected static final long READYNESS_RETRY_DELAY_BEATS = 5;
		private long readynessRetryDelayBeats;
		
		//protected static final long READYNESS_RETRY_DELAY_MS = 5000l;
		//private long readynessRetryDelayMs;
		protected final static boolean PUBLISH_LEADER_CANDIDATURE = true;
		private boolean publishLeaderCandidature;
		protected static final boolean LEADER_SHARD_ALSO_FOLLOWS = true;
		private boolean leaderShardAlsoFollows;
		protected static final String ZOOKEEPER_HOST_PORT = "localhost:2181";
		private String zookeeperHostPort;
		
		protected static final boolean ENABLE_WEBSERVER = true;
		private boolean enableWebserver;
		protected static final int WEB_SERVER_PORT = 57480;
		protected static final String WEB_SERVER_HOST_PORT = "localhost:" + WEB_SERVER_PORT;
		private String webServerHostPort;
		
		protected static final String WEB_SERVER_CONTEXT_PATH = "minka";
		private String webServerContextPath;
		
		public String getServiceName() {
			return serviceName;
		}
		public void setServiceName(String serviceName) {
			BootstrapConf.serviceName = serviceName;
		}
		public long getBeatUnitMs() {
			return beatUnitMs;
		}
		public long getReadynessRetryDelayBeats() {
			return readynessRetryDelayBeats;
		}
		public void setBeatUnitMs(long beatUnitMs) {
			this.beatUnitMs = beatUnitMs;
		}
		public void setReadynessRetryDelayBeats(long readynessRetryDelayBeats) {
			this.readynessRetryDelayBeats = readynessRetryDelayBeats;
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
		public boolean isEnableWebserver() {
			return this.enableWebserver;
		}
		public void setEnableWebserver(boolean enableWebserver) {
			this.enableWebserver = enableWebserver;
		}
		public String getWebServerHostPort() {
			return this.webServerHostPort;
		}
		public void setWebServerHostPort(String webServerHostPort) {
			this.webServerHostPort = webServerHostPort;
		}
		public String getWebServerContextPath() {
			return webServerContextPath;
		}
		public void setWebServerContextPath(String webServerContextPath) {
			this.webServerContextPath = webServerContextPath;
		}
	}

	public static class BrokerConf {
		public final static int PORT = 5748;
		protected final static String HOST_PORT = "localhost:" + PORT;
		private String hostPort;
		// tested with a cluster of 10 nodes: 1 thread was enough
		// either case Heartbeats from followers will compete for leader's atention at most
		// and broker's messages range 8-30k bytes: which means a fast netty channel switch and no starvation   
		protected final static int CONNECTION_HANDLER_THREADS = 1;
		private int connectionHandlerThreads;
		protected final static int MAX_RETRIES = 3;
		private int maxRetries;
		protected final static int RETRY_DELAY_MILI_BEATS = 300;
		private long retryDelayMiliBeats;
		protected final static int MAX_LAG_BEFORE_DISCARDING_CLIENT_QUEUE_BEATS = 10;
		private long maxLagBeforeDiscardingClientQueueBeats;
		protected final static int MAX_CLIENT_QUEUE_SIZE = 50;
		private int maxClientQueueSize;
		//protected final static int RETRY_DELAY_MS = 300;
		//private int retryDelayMs;
		/** True: try number-consecutive open ports if specified is busy, False: break bootup */
		protected static final boolean ENABLE_PORT_FALLBACK = true;
		public boolean enablePortFallback;
        protected static final boolean USE_MACHINE_HOSTNAME = false;
        public boolean useMachineHostname;
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
		public long  getRetryDelayMiliBeats() {
			return retryDelayMiliBeats;
		}
		public void setRetryDelayMiliBeats(int retryDelayMiliBeats) {
			this.retryDelayMiliBeats = retryDelayMiliBeats;
		}
		public long getMaxLagBeforeDiscardingClientQueueBeats() {
			return maxLagBeforeDiscardingClientQueueBeats;
		}
		public void setMaxLagBeforeDiscardingClientQueueBeats(long maxLagBeforeDiscardingClientQueueBeats) {
			this.maxLagBeforeDiscardingClientQueueBeats = maxLagBeforeDiscardingClientQueueBeats;
		}
		public boolean isEnablePortFallback() {
			return this.enablePortFallback;
		}
		public void setEnablePortFallback(boolean enablePortFallback) {
			this.enablePortFallback = enablePortFallback;
		}
		public boolean isUseMachineHostname() {
            return this.useMachineHostname;
        }
		public void setUseMachineHostname(boolean useMachineHostname) {
            this.useMachineHostname = useMachineHostname;
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
		public int getMaxClientQueueSize() {
			return maxClientQueueSize;
		}
		public void setMaxClientQueueSize(final int maxClientQueueSize) {
			this.maxClientQueueSize = maxClientQueueSize;
		}
	}

	public static class FollowerConf {
		
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

	public static class DistributorConf {
		protected static final boolean RUN_CONSISTENCY_CHECK = false;
		private boolean runConsistencyCheck;
		protected static final boolean RELOAD_DUTIES_FROM_STORAGE = false;
		private boolean reloadDutiesFromStorage;
		protected static final int RELOAD_DUTIES_FROM_STORAGE_EACH_PERIODS = 10;
		private int reloadDutiesFromStorageEachPeriods;
		/* 10 seconds to let the Proctor discover all Followers before distributing */
		protected final static long START_DELAY_BEATS = 10;
		private long startDelayBeats;
		protected final static long DELAY_BEATS = 3;		
		private long delayBeats;
		protected static final int PLAN_EXPIRATION_BEATS = 10;
		private int planExpirationBeats;
		
		protected static final int PLAN_MAX_RETRIES = 3;
		private int planMaxRetries;
		
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
		public int getPlanMaxRetries() {
			return this.planMaxRetries;
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
		public int getPlanExpirationBeats() {
			return planExpirationBeats;
		}
		public void setPlanExpirationBeats(int planExpirationBeats) {
			this.planExpirationBeats = planExpirationBeats;
		}
		public void setPlanMaxRetries(int planMaxRetries) {
			this.planMaxRetries = planMaxRetries;
		}
		
	}

	public static class BalancerConf {
		public static final Strategy STRATEGY = Strategy.EVEN_WEIGHT;
		private Strategy strategy;

		public static final int EVEN_SIZE_MAX_DUTIES_DELTA_BETWEEN_SHARDS = 1;
		private int roundRobinMaxDutiesDeltaBetweenShards;
		
		public static final Balancer.PreSort EVEN_WEIGHT_PRESORT = Balancer.PreSort.WEIGHT;
		private Balancer.PreSort evenLoadPresort;
		
		public static final MaxUnit SPILL_OVER_MAX_UNIT = MaxUnit.USE_CAPACITY;
		private MaxUnit spillOverMaxUnit;
		public static final double SPILL_OVER_MAX_VALUE = 99999999999d;
		private double spillOverMaxValue;
		
		public static final Dispersion FAIR_WEIGHT_DISPERSION = Dispersion.EVEN;
		public static final PreSort FAIR_WEIGHT_PRESORT = PreSort.DATE;

		
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
		public Balancer.PreSort getEvenLoadPresort() {
			return this.evenLoadPresort;
		}
		public void setEvenLoadPresort(Balancer.PreSort fairLoadPresort) {
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

	public static class ProctorConf {
		/* each 3 seconds */
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
		protected static final int HEARTBEAT_LAPSE_BEATS = 15;
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

	private void init() {
		this.scheduler = new SchedulerConf();
		this.bootstrap = new BootstrapConf();
		this.broker = new BrokerConf();
		this.follower = new FollowerConf();
		this.distributor = new DistributorConf();
		this.proctor = new ProctorConf();
		this.balancer = new BalancerConf();
		this.consistency = new ConsistencyConf();		
	}
	public Config() {
		init();
		loadFromPropOrSystem(null);
	}
	public Config(final Properties prop) {
		init();
		loadFromPropOrSystem(prop);
	}
	public Config(final String zookeeperHostPort, final String brokerHostPort) {
		init();
		loadFromPropOrSystem(null);
		getBootstrap().setZookeeperHostPort(zookeeperHostPort);
		getBroker().setHostPort(brokerHostPort);
	}
	public Config(final String zookeeperHostPort) {
		init();
		loadFromPropOrSystem(null);
		getBootstrap().setZookeeperHostPort(zookeeperHostPort);
	}	
	
	private void loadFromPropOrSystem(Properties prop) {
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
		Defaulter.apply(prop, "proctor.", this.getProctor());
		logger.info("{}: Configuration: {} ", getClass().getSimpleName(), toJson());
	}

	public String toJson() {
		try {
			return objectMapper.writeValueAsString(this);
		} catch (JsonProcessingException e) {
			return "{\"error\":\"true\"}";
		}
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
	public static Config fromJsonFile(final File jsonFormatConfig) throws Exception {
		return objectMapper.readValue(jsonFormatConfig, Config.class);
	}

	@JsonIgnore
	public ShardIdentifier getLoggingShardId() {
		return this.resolvedShardId;
	}

	public void setResolvedShardId(ShardIdentifier resolvedShardId) {
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
	
	public long beatToMs(final long beats) {
		return bootstrap.getBeatUnitMs() * beats;
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

	public ProctorConf getProctor() {
		return this.proctor;
	}

	public void setProctor(ProctorConf proctor) {
		this.proctor = proctor;
	}

	public DateTime getLoadTime() {
		return this.loadTime;
	}

	public ShardIdentifier getResolvedShardId() {
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
