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
package io.tilt.minka.core.task.impl;

import java.util.function.Supplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.tilt.minka.api.Config;
import io.tilt.minka.spectator.Spectator;

/* to share one connection with different children */
public class SpectatorSupplier implements Supplier<Spectator> {

		private final Logger logger = LoggerFactory.getLogger(getClass());
		public static String MINKA_SUBDOMAIN = "minka";

		private final Config config;
		private Spectator spectator;

		public SpectatorSupplier(final Config config) {
			this.config = config;
		}

		/* stop using same instance and renew object */
		public void renew() {
			logger.info("{}: ({}) Resetting held reference", getClass().getSimpleName(), config.getLoggingShardId());
			this.spectator = null;
		}

		@Override
		public synchronized Spectator get() {
			if (spectator == null || !spectator.isConnected()) {
				logger.info("{}: ({}) Supplying a new Spectator client because current is: {}", getClass().getSimpleName(),
							config.getLoggingShardId(), spectator == null ? "Uinitialized" : "Disconnected");
				spectator = new Spectator(config.getBootstrap().getZookeeperHostPort(), config.getLoggingShardId().toString());
			}
			return spectator;
		}
}