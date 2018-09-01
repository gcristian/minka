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
package io.tilt.minka.core.monitor;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.io.FileUtils;

import com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.core.JsonGenerator.Feature;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

import io.tilt.minka.api.Config;
import io.tilt.minka.core.task.LeaderAware;
import io.tilt.minka.core.task.Scheduler;
import io.tilt.minka.core.task.Scheduler.Agent;
import io.tilt.minka.core.task.Scheduler.Frequency;
import io.tilt.minka.core.task.Scheduler.PriorityLock;
import io.tilt.minka.core.task.Semaphore.Action;

/**
 * Read only views built at request about the system state  
 * JSON format.
 * 
 * @author Cristian Gonzalez
 * @since Nov 6, 2016
 */
public class SystemStateMonitor {

	private final DistroJSONBuilder distro;
	private final FollowerJSONBuilder follower;
	private final CrossJSONBuilder cross;
	private final SchemeJSONBuilder scheme;
	private final LeaderAware leaderAware;
	private final Config config;
	
	private final Agent coredumper;
	
	static final ObjectMapper mapper; 
	static {
		mapper = new ObjectMapper();
		mapper.setVisibility(PropertyAccessor.ALL, Visibility.NONE);
		mapper.setVisibility(PropertyAccessor.FIELD, Visibility.ANY);
		//mapper.configure(SerializationFeature.INDENT_OUTPUT, true);
		mapper.configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, true);
		mapper.configure(SerializationFeature.FAIL_ON_SELF_REFERENCES, false);
		mapper.configure(SerializationFeature.WRITE_SINGLE_ELEM_ARRAYS_UNWRAPPED, false);
		mapper.configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);
		mapper.configure(Feature.WRITE_NUMBERS_AS_STRINGS, true);
	}

	private final Map<String, String> lastJsons = new HashMap<>();
	
	public SystemStateMonitor(
			final Config config,
			final DistroJSONBuilder leader, 
			final FollowerJSONBuilder follower, 
			final CrossJSONBuilder cross,
			final SchemeJSONBuilder scheme,
			final LeaderAware leaderAware, 
			final Scheduler scheduler) {
		super();
		this.config = config;
		this.distro = leader;
		this.follower = follower;
		this.cross = cross;
		this.leaderAware = leaderAware;
		this.scheme = scheme;
		this.coredumper = createStateMonitor(scheduler);
		
		if (config.getBootstrap().isEnableCoreDump()) {
			scheduler.schedule(coredumper);
		}

	}
	
	static String toJson(final Object o) {
		try {
			return mapper.writeValueAsString(o);
		} catch (JsonProcessingException e) {
			return "";
		}
	}

	public Agent createStateMonitor(final Scheduler scheduler) {
		return scheduler
				.getAgentFactory().create(
						Action.BOOTSTRAP_COREDUMP,
						PriorityLock.HIGH_ISOLATED,
						Frequency.PERIODIC, 
						() -> stateMonitor())
				.every(config.beatToMs(config.getBootstrap().getCoreDumpFrequency()))
				.build();
	}

	private boolean saveOnDiff(final String key, final Instant now, final String value) {
		boolean ret = false;
		final String last = lastJsons.get(key);
		if (last==null || !last.contentEquals(value)) {
			lastJsons.put(key, value);
			final StringBuilder filepath = new StringBuilder()
					.append(config.getBootstrap().getCoreDumpFilepath())
					.append("/minka-")
					.append(key)
					.append("-")
					.append(config.getBootstrap().getServerTag());
			if (!config.getBootstrap().isCoreDumpOverwrite()) {
				filepath.append("-").append(now.toString());
			}
			filepath.append(".json");
			try {
				FileUtils.writeStringToFile(new File(filepath.toString()), value, Charset.defaultCharset());
			} catch (IOException e) {
			}
			ret = true;
		}
		return ret;
	}
	
	protected void stateMonitor() {
		final Instant now = Instant.now();
		saveOnDiff("config", now, config.toJson());
		saveOnDiff("schedule", now, cross.scheduleToJson(false));
		saveOnDiff("shards", now, distro.shardsToJson());
		saveOnDiff("broker", now, cross.brokerToJson());
		saveOnDiff("partition", now, follower.partitionToJson(false));
		saveOnDiff("beats", now, follower.beatsToJson());
		
		if (leaderAware.imLeader()) {
			saveOnDiff("plans", now, distro.plansToJson(true));
			saveOnDiff("distro", now, distro.distributionToJson());
			saveOnDiff("scheme", now, scheme.schemeToJson(true));
			saveOnDiff("pallets", now, distro.palletsToJson());
		}
	}

}
