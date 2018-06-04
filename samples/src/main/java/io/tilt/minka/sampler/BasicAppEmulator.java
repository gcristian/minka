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
package io.tilt.minka.sampler;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.tilt.minka.api.Client;
import io.tilt.minka.api.Config;
import io.tilt.minka.api.Duty;
import io.tilt.minka.api.Pallet;
import io.tilt.minka.api.Server;
import io.tilt.minka.utils.LogUtils;

/**
 * Example of a very basic Application using Minka.
 * 
 * This will create a Minka server and load entities from a {@linkplain DummyDataProvider} 
 * 
 * @author Cristian Gonzalez
 * @since Nov 9, 2016
 */
public class BasicAppEmulator {

	private static final Logger logger = LoggerFactory.getLogger(BasicAppEmulator.class);

	// TreeSet() to enter them in the default minka order: comparator by id   
	private final Set<Duty<String>> runningDuties = new TreeSet<>();

	private Server<String, String> server;
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
	
	public Server<String, String> getServer() {
		return this.server;
	}
	
	public void launch(final DummyDataProvider loader) throws Exception {
		java.util.Objects.requireNonNull(loader);

		final Set<Duty<String>> duties = new TreeSet<>(loader.loadDuties());
		final Set<Pallet<String>> pallets = new TreeSet<>(loader.loadPallets());

		config.getBootstrap().setServerTag(server.getClient().getShardIdentity() +"-eltag");
		config.getBootstrap().setNamespace("basic-app-emulator");
		server = new Server<>(config);

		// data only needed for logging
		this.shardId = config.getResolvedShardId().getId();
		logger.info("{} Loading {} duties: {}", shardId, duties.size(), toStringGroupByPallet(duties));
		// optional info
		server.getEventMapper()
			.onPalletLoad(() -> pallets)
			.onLoad(()-> duties)
			.onCapture((final Set<Duty<String>> d) -> {
				// start tasks
				logger.info(LogUtils.titleLine(LogUtils.HYPHEN_CHAR, "taking"));
				logger.info("{} # {}+ ({})", shardId, d.size(), toStringIds(d));
				runningDuties.addAll(d);
			})
			.onPalletCapture((p)->logger.info("Taking pallet: {}", p.toString()))
			.onRelease((final Set<Duty<String>> d)-> {
				// stop tasks previously started
				logger.info(LogUtils.titleLine(LogUtils.HYPHEN_CHAR, "releasing"));
				logger.info("{} # -{} ({})", shardId, d.size(), toStringIds(d));
				runningDuties.removeAll(d);
			})
			.onPalletRelease((p)->logger.info("Releasing pallet: {}", p.toString()))
			.onActivation(()-> {
				// only for this emulator class: user would rarely need this event 
				this.shardId = server.getClient().getShardIdentity();
			})
			.onUpdate(d->logger.info("receiving update for: {}", d))
			.onTransfer((d, e)->logger.info("receiving transfer to: {}", d))
			.done()
			;
			
		for (final Pallet<String> pallet: pallets) {
			server.getEventMapper().setCapacity(pallet, loader.loadShardCapacity(pallet, duties, server.getClient().getShardIdentity()));			
		}
		
		
		
		
		afterLoad(pallets);
		
		
	}

	private void afterLoad(final Set<Pallet<String>> pallets) throws InterruptedException {
		Thread.sleep(20000);
		
		int i = 0;
		final List<Duty<String>> newones = new ArrayList<>();
		for (final Pallet<?> p: pallets) {
			final Duty<String> x = Duty.<String>builder("QK-" + i++, p.getId()).with("karajo-" + i).build();
			newones.add(x);
			server.getClient().add(x);
		}
		
		Thread.sleep(10000);
		for (Duty<String> d: newones) {
			server.getClient().remove(d);
		}
		newones.clear();
		
		Thread.sleep(15000);
		final Pallet<String> p = pallets.iterator().next();
		for (int k = 0 ; k < 10; k++) {
			final Duty<String> x = Duty.<String>builder("BF-" + k, p.getId()).with("BFart-" + k).build();
			newones.add(x);
			server.getClient().add(x);
		}
		
		Thread.sleep(10000);
		newones.forEach(d->server.getClient().remove(d));
	}
	
	public Client<String, String> getClient() {
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
		entities.stream()
			.collect(Collectors.groupingBy(t -> t.getPalletId()))
			.forEach((palletId, duties) -> sb
					.append(" p").append(palletId)
					.append("->").append(toStringIds(duties))
					.append(", "));
		entities.forEach(e -> sb
				.append("p").append(e.getPalletId())
				.append("d").append(e.getId()).append(", "));
		return sb.toString();
	}

	private static String toStringIds(Collection<Duty<String>> entities) {
		final StringBuilder sb = new StringBuilder();
		for (Duty d: entities) {
			sb.append("p").append(d.getPalletId())
				.append("d").append(d.getId())
				.append("(").append(d.get()).append(", ");
		}
		return sb.toString();
	}

	int lastSize;
	long lastPrint;


}