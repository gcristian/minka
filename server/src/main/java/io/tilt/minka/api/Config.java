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

import io.tilt.minka.api.config.BalancerConfiguration;
import io.tilt.minka.api.config.BootstrapConfiguration;
import io.tilt.minka.api.config.BrokerConfiguration;
import io.tilt.minka.api.config.ConsistencyConfiguration;
import io.tilt.minka.api.config.DistributorSettings;
import io.tilt.minka.api.config.FollowerSettings;
import io.tilt.minka.api.config.ProctorSettings;
import io.tilt.minka.api.config.SchedulerSettings;
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
		//objectMapper.configure(SerializationFeature.INDENT_OUTPUT, true);
		objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, true);

		final SimpleModule simpleModule = new SimpleModule("module-1");
		simpleModule.addSerializer(DateTime.class, new DateTimeSerializer());
		objectMapper.registerModule(simpleModule);
	}
	
	@JsonIgnore
	public final DateTime loadTime = new DateTime(DateTimeZone.UTC);
	@JsonIgnore
	private ShardIdentifier resolvedShardId;

	private SchedulerSettings scheduler;
	private BootstrapConfiguration bootstrap;
	private BrokerConfiguration broker;
	private FollowerSettings follower;
	private BalancerConfiguration balancer;
	private DistributorSettings distributor;
	private ProctorSettings proctor;
	private ConsistencyConfiguration consistency;

	private void init() {
		this.scheduler = new SchedulerSettings();
		this.bootstrap = new BootstrapConfiguration();
		this.broker = new BrokerConfiguration();
		this.follower = new FollowerSettings();
		this.distributor = new DistributorSettings();
		this.proctor = new ProctorSettings();
		this.balancer = new BalancerConfiguration();
		this.consistency = new ConsistencyConfiguration();		
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

	public BootstrapConfiguration getBootstrap() {
		return this.bootstrap;
	}

	public void setBootstrap(BootstrapConfiguration bootstrap) {
		this.bootstrap = bootstrap;
	}

	public BrokerConfiguration getBroker() {
		return this.broker;
	}

	public void setBroker(BrokerConfiguration broker) {
		this.broker = broker;
	}

	public FollowerSettings getFollower() {
		return this.follower;
	}

	public void setFollower(FollowerSettings follower) {
		this.follower = follower;
	}

	public DistributorSettings getDistributor() {
		return this.distributor;
	}

	public void setDistributor(DistributorSettings distributor) {
		this.distributor = distributor;
	}

	public ProctorSettings getProctor() {
		return this.proctor;
	}

	public void setProctor(ProctorSettings proctor) {
		this.proctor = proctor;
	}

	public DateTime getLoadTime() {
		return this.loadTime;
	}

	public ShardIdentifier getResolvedShardId() {
		return this.resolvedShardId;
	}

	public BalancerConfiguration getBalancer() {
		return this.balancer;
	}

	public void setBalancer(BalancerConfiguration balancer) {
		this.balancer = balancer;
	}

	public SchedulerSettings getScheduler() {
		return scheduler;
	}

	public void setScheduler(SchedulerSettings scheduler) {
		this.scheduler = scheduler;
	}

	public ConsistencyConfiguration getConsistency() {
		return this.consistency;
	}

	public void setConsistency(ConsistencyConfiguration consistency) {
		this.consistency = consistency;
	}

}
