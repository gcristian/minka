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
package io.tilt.minka.domain;

import java.util.Collection;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.tilt.minka.api.Config;
import io.tilt.minka.api.Duty;
import io.tilt.minka.api.Minka;
import io.tilt.minka.api.MinkaClient;
import io.tilt.minka.api.Pallet;
import io.tilt.minka.utils.LogUtils;

/**
 * Example of a very basic Application using Minka.
 * 
 * This will create a Minka server and load entities from a {@linkplain ClusterEmulatorProvider} 
 * 
 * @author Cristian Gonzalez
 * @since Nov 9, 2016
 */
public class BasicAppEmulator {

	private static final Logger logger = LoggerFactory.getLogger(BasicAppEmulator.class);

	// TreeSet() to enter them in the default minka order: comparator by id   
	private final Set<Duty<String>> runningDuties = new TreeSet<>();

	private Minka<String, String> server;
	private final Config config;
	private String shardId = "{NN}";
	
	public BasicAppEmulator(final Config config) throws Exception {
		super();
		this.config = java.util.Objects.requireNonNull(config);
	}
	
	public BasicAppEmulator() throws Exception {
		super();
		this.config = new Config();
	}
	
	public Minka<String, String> getServer() {
		return this.server;
	}

	public void close() {
		if (this.server != null) {
			this.server.destroy();
		}
	}
	
	public void start(final ClusterEmulatorProvider loader) throws Exception {
		java.util.Objects.requireNonNull(loader);

		final Set<Duty<String>> duties = new TreeSet<>(loader.loadDuties());
		final Set<Pallet<String>> pallets = new TreeSet<>(loader.loadPallets());

		logger.info("{} Loading {} duties: {}", shardId, duties.size(), toStringGroupByPallet(duties));

		server = new Minka<>(config);
		server.setLocationTag(server.getClient().getShardIdentity() +"-eltag");
		server.onPalletLoad(() -> pallets);
		server.onDutyLoad(()-> duties);
		
		server.onDutyCapture((final Set<Duty<String>> t) -> {
			logger.info(LogUtils.titleLine(LogUtils.HYPHEN_CHAR, "taking"));
			logger.info("{} # {}+ ({})", shardId, t.size(), toStringIds(t));
			runningDuties.addAll(t);
		});
		server.onPalletCapture((p)->logger.info("Taking pallet: {}", p.toString()));
		
		server.onDutyRelease((final Set<Duty<String>> entities)-> {
			logger.info(LogUtils.titleLine(LogUtils.HYPHEN_CHAR, "releasing"));
			logger.info("{} # -{} ({})", shardId, entities.size(), toStringIds(entities));
			runningDuties.removeAll(entities);
		});
		server.onPalletRelease((p)->logger.info("Releasing pallet: {}", p.toString()));
		
		server.onDutyReport(()-> {
			final long now = System.currentTimeMillis();
			if (timeToLog(now)) {
				final MinkaClient<String, String> client = server.getClient();
				lastPrint = now;
				lastSize = runningDuties.size();
				logger.info(LogUtils.titleLine(LogUtils.HYPHEN_CHAR, "running"));
				if (logger.isDebugEnabled()) {
					logger.debug("{}: {} {{}} # {} ({})", getClass().getSimpleName(), shardId, 
							client != null ? client.isCurrentLeader() ?  "leader" : "follower" : "?", 
							this.runningDuties.size(), toStringGroupByPallet(runningDuties));
				} else {
					logger.info("{}: {} {{}} # {} ", getClass().getSimpleName(), shardId, 
							client != null ? client.isCurrentLeader() ? "leader" : "follower" : "?", 
							this.runningDuties.size());
				}
			}
			return this.runningDuties;
		});
		
		for (final Pallet<String> pallet: pallets) {
			server.setCapacity(pallet, loader.loadShardCapacity(pallet, duties, server.getClient().getShardIdentity()));			
		}
		
		server.onDutyUpdate(d->{});
		server.onActivation(()-> {
			this.shardId = server.getClient().getShardIdentity();
			logger.info("{}: Activating", this.shardId);
		});
		server.onDeactivation(()->logger.info("de-activating"));
		server.onDutyUpdate(d->logger.info("receiving update: {}", d));
		server.onDutyTransfer((d, e)->logger.info("receiving transfer: {}", d));
		server.load();
	}
	
	public MinkaClient<String, String> getClient() {
		if (this.server != null) {
			return this.server.getClient();
		}
		return null;
	}
	
	
	
	private boolean timeToLog(long now) {
		return lastSize != runningDuties.size() || now - lastPrint > 60000 * 1;
	}


	private static void sleep(final int mins) throws InterruptedException {
		for (int i = 0; i < mins; i++) {
			Thread.sleep(60 * 1000l);
			for (int j = 0; j < 200000; j++)
				;
		}
	}
	
	private String toStringGroupByPallet(Set<Duty<String>> entities) {
		final StringBuilder sb = new StringBuilder();
		entities.stream().collect(Collectors.groupingBy(t -> t.getPalletId())).forEach((palletId, duties) -> {
			sb.append(" p").append(palletId).append("->").append(toStringIds(duties)).append(", ");
		});
		entities.forEach(e -> sb.append("p" + e.getPalletId() + "d" + e.getId() + ", "));
		return sb.toString();
	}

	private static String toStringIds(Collection<Duty<String>> entities) {
		final StringBuilder sb = new StringBuilder();
		entities.forEach(e -> sb.append("p" + e.getPalletId() + "d" + e.getId() + ", "));
		return sb.toString();
	}

	int lastSize;
	long lastPrint;


}