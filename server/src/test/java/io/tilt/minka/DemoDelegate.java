/**
 * Copyright (c) 2011-2015 Zauber S.A. -- All rights reserved
 */

package io.tilt.minka;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;
import java.util.TreeSet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.tilt.minka.api.Duty;
import io.tilt.minka.api.DutyBuilder;
import io.tilt.minka.api.Pallet;
import io.tilt.minka.api.Pallet.Storage;
import io.tilt.minka.core.leader.distributor.Balancer.BalanceStrategy;
import io.tilt.minka.api.PalletBuilder;
import io.tilt.minka.api.PartitionMaster;
import io.tilt.minka.api.PartitionService;
import jersey.repackaged.com.google.common.collect.Sets;

public class Delegado implements PartitionMaster<String, String>, Serializable {

		private static final long serialVersionUID = 1L;
		private final Logger logger = LoggerFactory.getLogger(getClass());

		private Set<Duty<String>> allOriginalDuties = new HashSet<>();
		private Set<Pallet<String>> allOriginalPallets = new HashSet<>();
		private Set<Duty<String>> runningDuties = Sets.newHashSet();

		private static int TOTAL_TASKS = 20;
		private static int TOTAL_PALLETS = 5;
		private PartitionService partitionService;
		private String id = "{NN}";

		public Delegado() {			
			final Random rnd = new Random();
			allOriginalPallets = new HashSet<>();
			for (int i = 0; i < TOTAL_PALLETS; i++) {
				logger.info("pallet: {}", i);
				allOriginalPallets.add(PalletBuilder.build("P"+String.valueOf(i), String.class, BalanceStrategy.FAIR_LOAD, 
						Storage.CLIENT_DEFINED, "payload"));
			}
			allOriginalDuties = new HashSet<>();
			for (int i = 0; i < TOTAL_TASKS; i++) {
				logger.info("duty: {}", i);
				allOriginalDuties.add(DutyBuilder.build(String.class, String.valueOf(i), 
						"P"+String.valueOf(rnd.nextInt(TOTAL_PALLETS-1))));
			}

		}

		protected PartitionService getPartitionService() {
			return this.partitionService;
		}

		public void setPartitionService(PartitionService partitionService) {
			this.partitionService = partitionService;
			this.id = partitionService.getShardIdentity();
		}

		@Override
		public void take(Set<Duty<String>> entities) {
			logger.info("{} taking: {}+ ({})", id, entities.size(), toStringIds(entities));
			if (runningDuties == null) {
				runningDuties = new HashSet<>();
			}
			runningDuties.addAll(entities);
		}

		@Override
		public void update(Set<Duty<String>> entities) {
			logger.info("{} updating: {} ({})", id, entities.size(), toStringIds(entities));
		}

		private String toStringIds(Set<Duty<String>> entities) {
			final Set<Long> set = new TreeSet<>();
			entities.forEach(e -> set.add(Long.parseLong(e.getId())));
			final StringBuilder sb = new StringBuilder();
			set.forEach(e -> sb.append(e).append(", "));
			return sb.toString();
		}

		@Override
		public void release(Set<Duty<String>> entities) {
			logger.info("{} releasing: -{} ({})", id, entities.size(), toStringIds(entities));
			runningDuties.removeAll(entities);
		}

		int lastSize;
		long lastPrint;

		@Override
		public Set<Duty<String>> reportTaken() {
			long now = System.currentTimeMillis();
			if (lastSize != runningDuties.size() || now - lastPrint > 20 * 1000) {
				lastPrint = now;
				lastSize = runningDuties.size();
				logger.info("{} running: {}{} ({})", id, this.runningDuties.size(),
							partitionService != null ? partitionService.isCurrentLeader() ? "*" : "" : "",
							toStringIds(runningDuties));
			}
			return this.runningDuties;
		}

		@Override
		public Set<Duty<String>> loadDuties() {
			return allOriginalDuties;
		}

		@Override
		public void activate() {
			logger.info("activating");
		}

		@Override
		public void deactivate() {
			logger.info("de-activating");
		}

		@Override
		public void receive(Set<Duty<String>> duty, Serializable clientPayload) {
			logger.info("receiving");
		}

		@Override
		public boolean isReady() {
			logger.info("delegado is ready");
			return true;
		}

		@Override
		public Set<Pallet<String>> loadPallets() {
			return allOriginalPallets;
		}
}