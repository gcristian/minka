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

import java.util.Properties;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.tilt.minka.api.Pallet.Storage;
import io.tilt.minka.core.leader.distributor.Balancer.BalanceStrategy;
import io.tilt.minka.core.leader.distributor.FairWorkloadBalancer.PreSortType;
import io.tilt.minka.core.leader.distributor.SpillOverBalancer.MaxValueUsage;
import io.tilt.minka.domain.ShardID;
import io.tilt.minka.utils.Defaulter;

/**
 * Global configuration with default values
 * 
 * @author Cristian Gonzalez
 * @since Nov 8, 2015
 */
@SuppressWarnings("unused")
public class Config extends Properties {

		private static final long serialVersionUID = 3653003981484000071L;

		private ShardID resolvedShardId;

		public ShardID getResolvedShardId() {
			return this.resolvedShardId;
		}

		public void setResolvedShardId(ShardID resolvedShardId) {
			this.resolvedShardId = resolvedShardId;
		}

		private final Logger logger = LoggerFactory.getLogger(getClass());
		public final DateTime loadTime = new DateTime(DateTimeZone.UTC);
		// ------------------------------------ common --------------------------------------------

		private static final String SERVICE_NAME_DEFAULT = ("default-name");
		private String serviceName;

		private static final String ZOOKEEPER_HOST_PORT_DEFAULT = "localhost:2181";
		private String zookeeperHostPort;

		private final static String BOOTSTRAP_READYNESS_RETRY_DELAY_MS_DEFAULT = "5000";
		private long bootstrapReadynessRetryDelayMs;

		/* not all shards might want to candiate for leader */
		private final static String BOOTSTRAP_PUBLISH_LEADER_CANDIDATURE_DEFAULT = "true";
		private boolean bootstrapPublishLeaderCandidature;

		private static final String BOOTSTRAP_LEADER_SHARD_ALSO_FOLLOWS_DEFAULT = "true";
		private boolean bootstrapLeaderShardAlsoFollows;

		/*
		 * 5 mins default retaining messages from followers to leader's partition
		 */
		private final static String QUEUE_PARTITION_RETENTION_LAPSE_MS_DEFAULT = "300000";
		private long queuePartitionRetentionLapseMs;
		private final static String QUEUE_INBOX_RETENTION_LAPSE_MS_DEFAULT = "300000";
		private long queueInboxRetentionLapseMs;
		private final static String QUEUE_USER_RETENTION_LAPSE_MS_DEFAULT = "300000";
		private long queueUserRetentionLapseMs;

		private final static String BROKER_SERVER_HOST_DEFAULT = "127.0.0.1:9090";
		private String brokerServerHost;
		private final static String BROKER_SERVER_CONNECTION_HANDLER_THREADS_DEFAULT = "10";
		private int brokerServerConnectionHandlerThreads;
		private final static String BROKER_MAX_RETRIES_DEFAULT = "300";
		private int brokerMaxRetries;
		private final static String BROKER_RETRY_DELAY_MS_DEFAULT = "3000";
		private int brokerRetryDelayMs;

		public int getBrokerRetryDelayMs() {
			return this.brokerRetryDelayMs;
		}

		public int getBrokerMaxRetries() {
			return this.brokerMaxRetries;
		}

		public int getBrokerServerConnectionHandlerThreads() {
			return this.brokerServerConnectionHandlerThreads;
		}

		public String getBrokerServerHost() {
			return this.brokerServerHost;
		}

		public static final String THREAD_NAME_COORDINATOR_IN_BACKGROUND = "Minka-Scheduler";
		public static final String THREAD_NAME_BROKER_SERVER_GROUP = "Minka-SocketServerThreadGroup";
		public static final String THREAD_NAME_BROKER_SERVER_WORKER = "Minka-SocketServerThreadWorker";
		public static final String THREAD_NANE_TCP_BROKER_CLIENT = "Minka-SocketClientThread";

		public static final long SEMAPHORE_UNLOCK_RETRY_DELAY_MS = 100l; //50l;
		public static final int SEMAPHORE_UNLOCK_MAX_RETRIES = 30;

		// ------------------------------------ follower --------------------------------------------
		private static final String FOLLOWER_SHARD_ID_SUFFIX_DEFAULT = "";
		private String followerShardIdSuffix;

		private static final String FOLLOWER_USE_NETWORK_INTERFASE_DEFAULT = "lo";
		private String followerUseNetworkInterfase;

		/* each half second */
		private static final String FOLLOWER_HEARTBEAT_START_DELAY_MS_DEFAULT = "1000";
		private static final String FOLLOWER_HEARTBEAT_DELAY_MS_DEFAULT = "2000";
		private long followerHeartbeatDelayMs;
		private long followerHeartbeatStartDelayMs;

		/*
		 * 10 seconds enough to start check and release duties if no HB in x time
		 */
		private static final String FOLLOWER_HEARTATTACK_CHECK_START_DELAY_MS_DEFAULT = "10000";
		private long followerHeartattackCheckStartDelayMs;
		private static final String FOLLOWER_HEARTATTACK_CHECK_DELAY_MS_DEFAULT = "10000";
		private long followerHeartattackCheckDelayMs;

		/* 20 seconds to let the leader be elected */
		private static final String FOLLOWER_CLEARANCE_CHECK_START_DELAY_MS_DEFAULT = "20000";
		private long followerClearanceCheckStartDelayMs;
		private static final String FOLLOWER_CLEARANCE_CHECK_DELAY_MS_DEFAULT = "10000";
		private long followerClearanceCheckDelayMs;

		/* 30 seconds old max for clearance before releasing duties */
		private static final String FOLLOWER_CLEARANCE_MAX_ABSENCE_MS_DEFAULT = "30000";
		private int followerClearanceMaxAbsenceMs;

		/* 10 seconds absence to instruct delegate to release duties */
		private static final String FOLLOWER_MAX_HEARTBEAT_ABSENCE_FOR_RELEASE_MS_DEFAULT = "10000";
		private long followerMaxHeartbeatAbsenceForReleaseMs;

		/* 10 errors tolerant for building HBs from followers */
		private static final String FOLLOWER_MAX_HEARTBEAT_BUILD_FAILS_BEFORE_RELEASING_DEFAULT = "1";
		private int followerMaxHeartbeatBuildFailsBeforeReleasing;

		public static final String TASK_NAME_FOLLOWER_POLICIES_CLEARANCE = "FollowerPolicyClearance";
		public static final String TASK_NAME_FOLLOWER_POLICIES_HEARTATTACK = "FollowerPolicyHeartattack";

		// ------------------------------------ leader --------------------------------------------

		private static final String DISTRIBUTOR_RUN_CONSISTENCY_CHECK_DEFAULT = "true";
		private boolean distributorRunConsistencyCheck;

		private static final String DISTRIBUTOR_RELOAD_DUTIES_FROM_STORAGE_DEFAULT = "false";
		private boolean distributorReloadDutiesFromStorage;
		private static final String DISTRIBUTOR_RELOAD_DUTIES_FROM_STORAGE_EACH_PERIODS_DEFAULT = "10";
		private int distributorReloadDutiesFromStorageEachPeriods;

		/*
		 * 10 seconds to let the Shepherd discover all Followers before
		 * Distributing
		 */
		private final static String DISTRIBUTOR_START_DELAY_MS_DEFAULT = "10000";
		private final static String DISTRIBUTOR_DELAY_MS_DEFAULT = "5000";
		private long distributorStartDelayMs;
		private long distributorDelayMs;

		private static final String DISTRIBUTOR_REALLOCATION_EXPIRATION_SEC_DEFAULT = "15";
		private int distributorReallocationExpirationSec;
		private static final String DISTRIBUTOR_REALLOCATION_MAX_RETRIES_DEFAULT = "3";
		private int distributorReallocationMaxRetries;

		private static final String BALANCER_EVEN_SIZE_MAX_DUTIES_DELTA_BETWEEN_SHARDS_DEFAULT = "1";
		private int balancerEvenSizeMaxDutiesDeltaBetweenShards;

		private static final String DISTRIBUTOR_BALANCER_STRATEGY_DEFAULT = "FAIR_LOAD";
		private BalanceStrategy distributorBalancerStrategy;

		private static final String BALANCER_FAIR_LOAD_PRESORT_DEFAULT = "WORKLOAD";
		private PreSortType balancerFairLoadPresort;

		private static final String BALANCER_SPILL_OVER_STRATEGY_DEFAULT = "WORKLOAD";
		private MaxValueUsage balancerSpillOverStrategy;

		private static final String BALANCER_SPILL_OVER_MAX_VALUE_DEFAULT = "0";
		private long balancerSpillOverMaxValue;

		/* each 3 seconds */
		private final static String SHEPHERD_START_DELAY_MS_DEFAULT = "500";
		private final static String SHEPHERD_DELAY_MS_DEFAULT = "2000";
		private long shepherdStartDelayMs;
		private long shepherdDelayMs;

		private static final String SHEPHERD_MAX_SHARD_JOINING_STATE_MS_DEFAULT = "15000";
		private int shepherdMaxShardJoiningStateMs;

		private static final String SHEPHERD_MIN_HEALTHLY_HEARTBEATS_FOR_SHARD_ONLINE_DEFAULT = "2";
		private int shepherdMinHealthlyHeartbeatsForShardOnline;

		private static final String SHEPHERD_MAX_ABSENT_HEARTBEATS_BEFORE_SHARD_GONE_DEFAULT = "5";
		private int shepherdMaxAbsentHeartbeatsBeforeShardGone;

		private static final String SHEPHERD_MAX_HEARTBEAT_RECEPTION_DELAY_FACTOR_FOR_SICK_DEFAULT = "3d";
		private double shepherdMaxHeartbeatReceptionDelayFactorForSick;

		private static final String SHEPHERD_MAX_SICK_HEARTBEATS_BEFORE_SHARD_QUARANTINE_DEFAULT = "15";
		private int shepherdMaxSickHeartbeatsBeforeShardQuarantine;

		private static final String SHEPHERD_MIN_SHARDS_ONLINE_BEFORE_SHARDING_DEFAULT = "1";
		private int shepherdMinShardsOnlineBeforeSharding;

		private static final String SHEPHERD_HEARTBEAT_MAX_BIGGEST_DISTANCE_FACTOR_DEFAULT = "2.5d";
		private double shepherdHeartbeatMaxBiggestDistanceFactor;

		private static final String SHEPHERD_HEARTBEAT_LAPSE_SEC_DEFAULT = "20";
		private int shepherdHeartbeatLapseSec;
		private static final String SHEPHERD_HEARTBEAT_MAX_DISTANCE_STANDARD_DEVIATION_DEFAULT = "4";
		private double shepherdHeartbeatMaxDistanceStandardDeviation;

		private static final String CLUSTER_HEALTH_STABILITY_DELAY_PERIODS_DEFAULT = "3";
		private int clusterHealthStabilityDelayPeriods;

		/* whether using PartitionMaster or PartitionDelegate */
		private final static String DUTY_STORAGE_DEFAULT = "CLIENT_DEFINED";
		private Storage dutyStorage;

		public Config(Properties p) {
			if (p == null) {
				logger.warn("{}: Using all DEFAULT values !", getClass().getSimpleName());
				p = new Properties();
			}

			Defaulter.apply(p, this);
		}

		@Override
		public String toString() {
			StringBuilder sb = new StringBuilder();
			sb.append("Config");
			return sb.toString();
		}

		public String getServiceName() {
			return this.serviceName;
		}

		public long getBootstrapReadynessRetryDelayMs() {
			return this.bootstrapReadynessRetryDelayMs;
		}

		public long getDistributorStartDelayMs() {
			return this.distributorStartDelayMs;
		}

		public long getDistributorDelayMs() {
			return this.distributorDelayMs;
		}

		public boolean getDistributorRunConsistencyCheck() {
			return this.distributorRunConsistencyCheck;
		}

		public boolean distributorReloadsDutiesFromStorage() {
			return this.distributorReloadDutiesFromStorage;
		}

		public int getDistributorReloadDutiesFromStorageEachPeriods() {
			return this.distributorReloadDutiesFromStorageEachPeriods;
		}

		public long getDistributionStartDelayMs() {
			return this.distributorStartDelayMs;
		}

		public String getFollowerShardIdSuffix() {
			return this.followerShardIdSuffix;
		}

		public String getFollowerUseNetworkInterfase() {
			return this.followerUseNetworkInterfase;
		}

		public long getFollowerHeartbeatDelayMs() {
			return this.followerHeartbeatDelayMs;
		}

		public long getFollowerHeartbeatStartDelayMs() {
			return this.followerHeartbeatStartDelayMs;
		}

		public long getFollowerHeartattackCheckStartDelayMs() {
			return this.followerHeartattackCheckStartDelayMs;
		}

		public long getFollowerHeartattackCheckDelayMs() {
			return this.followerHeartattackCheckDelayMs;
		}

		public long getFollowerClearanceCheckStartDelayMs() {
			return this.followerClearanceCheckStartDelayMs;
		}

		public long getFollowerClearanceCheckDelayMs() {
			return this.followerClearanceCheckDelayMs;
		}

		public int getFollowerClearanceMaxAbsenceMs() {
			return this.followerClearanceMaxAbsenceMs;
		}

		public long getFollowerMaxHeartbeatAbsenceForReleaseMs() {
			return this.followerMaxHeartbeatAbsenceForReleaseMs;
		}

		public String getZookeeperHostPort() {
			return this.zookeeperHostPort;
		}

		public int getFollowerMaxHeartbeatBuildFailsBeforeReleasing() {
			return this.followerMaxHeartbeatBuildFailsBeforeReleasing;
		}

		public int getShepherdMinHealthlyHeartbeatsForShardOnline() {
			return this.shepherdMinHealthlyHeartbeatsForShardOnline;
		}

		public int getShepherdMaxAbsentHeartbeatsBeforeShardGone() {
			return this.shepherdMaxAbsentHeartbeatsBeforeShardGone;
		}

		public double getShepherdMaxHeartbeatReceptionDelayFactorForSick() {
			return this.shepherdMaxHeartbeatReceptionDelayFactorForSick;
		}

		public int getShepherdMaxSickHeartbeatsBeforeShardQuarantine() {
			return this.shepherdMaxSickHeartbeatsBeforeShardQuarantine;
		}

		public int getShepherdMinShardsOnlineBeforeSharding() {
			return this.shepherdMinShardsOnlineBeforeSharding;
		}

		public BalanceStrategy getBalancerDistributionStrategy() {
			return this.distributorBalancerStrategy;
		}

		public long getShepherdStartDelayMs() {
			return this.shepherdStartDelayMs;
		}

		public long getShepherdDelayMs() {
			return this.shepherdDelayMs;
		}

		public boolean bootstrapLeaderShardAlsoFollows() {
			return this.bootstrapLeaderShardAlsoFollows;
		}

		public int getShepherdMaxShardJoiningStateMs() {
			return this.shepherdMaxShardJoiningStateMs;
		}

		public int getShepherdHeartbeatLapseSec() {
			return this.shepherdHeartbeatLapseSec;
		}

		public double getShepherdHeartbeatMaxDistanceStandardDeviation() {
			return this.shepherdHeartbeatMaxDistanceStandardDeviation;
		}

		public boolean bootstrapPublishLeaderCandidature() {
			return this.bootstrapPublishLeaderCandidature;
		}

		public Storage getDutyStorage() {
			return this.dutyStorage;
		}

		public double getShepherdHeartbeatMaxBiggestDistanceFactor() {
			return this.shepherdHeartbeatMaxBiggestDistanceFactor;
		}

		public long getQueuePartitionRetentionLapseMs() {
			return this.queuePartitionRetentionLapseMs;
		}

		public long getQueueInboxRetentionLapseMs() {
			return this.queueInboxRetentionLapseMs;
		}

		public long getQueueUserRetentionLapseMs() {
			return this.queueUserRetentionLapseMs;
		}

		public int getBalancerEvenSizeMaxDutiesDeltaBetweenShards() {
			return this.balancerEvenSizeMaxDutiesDeltaBetweenShards;
		}

		public int getDistributorReallocationExpirationSec() {
			return this.distributorReallocationExpirationSec;
		}

		public int getDistributorReallocationMaxRetries() {
			return this.distributorReallocationMaxRetries;
		}

		public PreSortType getBalancerFairLoadPresort() {
			return this.balancerFairLoadPresort;
		}

		public long getBalancerSpillOverMaxValue() {
			return this.balancerSpillOverMaxValue;
		}

		public MaxValueUsage getBalancerSpillOverStrategy() {
			return this.balancerSpillOverStrategy;
		}

		public int getClusterHealthStabilityDelayPeriods() {
			return this.clusterHealthStabilityDelayPeriods;
		}

}
