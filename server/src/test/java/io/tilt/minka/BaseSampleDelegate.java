/**
 * Copyright (c) 2011-2015 Zauber S.A. -- All rights reserved
 */

package io.tilt.minka;

import java.io.Serializable;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.tilt.minka.api.Duty;
import io.tilt.minka.api.Pallet;
import io.tilt.minka.api.PartitionMaster;
import io.tilt.minka.api.MinkaClient;
import jersey.repackaged.com.google.common.collect.Sets;

public class BaseSampleDelegate implements PartitionMaster<String, String>, Serializable {

		private static final long serialVersionUID = 1L;
		private final Logger logger = LoggerFactory.getLogger(getClass());

		private Set<Duty<String>> allOriginalDuties = new HashSet<>();
		private Set<Pallet<String>> allOriginalPallets = new HashSet<>();
		private Set<Duty<String>> runningDuties = Sets.newHashSet();

		//private MinkaClient minkaClient;
		private String shardId = "{NN}";

		public BaseSampleDelegate() {			
			allOriginalPallets = getPallets();
			logger.info("pallets: {}", allOriginalPallets);
			allOriginalDuties = getDuties();
			logger.info("duties: {}", allOriginalDuties);

		}
		
		protected Set<Pallet<String>> getPallets() {
			return new HashSet<>();
		}

		protected Set<Duty<String>> getDuties() {
			return new HashSet<>();
		}

		@Override
		public void take(Set<Duty<String>> entities) {
			logger.info("{} taking: {}+ ({})", shardId, entities.size(), toStringIds(entities));
			if (runningDuties == null) {
				runningDuties = new HashSet<>();
			}
			runningDuties.addAll(entities);
		}

		@Override
		public void update(Set<Duty<String>> entities) {
			logger.info("{} updating: {} ({})", shardId, entities.size(), toStringIds(entities));
		}

		private String toStringGroupByPallet(Set<Duty<String>> entities) {
			final StringBuilder sb = new StringBuilder();
			entities.stream()
				.collect(Collectors.groupingBy(t -> t.getPalletId()))
				.forEach((palletId, duties)-> {
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

		private static String toStringWithPalletIds(Collection<Duty<String>> entities) {
			final StringBuilder sb = new StringBuilder();
			entities.forEach(e -> sb.append("p" + e.getPalletId() + "d" + e.getId() + ", "));
			return sb.toString();
		}

		@Override
		public void release(Set<Duty<String>> entities) {
			logger.info("{} releasing: -{} ({})", shardId, entities.size(), toStringIds(entities));
			runningDuties.removeAll(entities);
		}

		int lastSize;
		long lastPrint;

		@Override
		public Set<Duty<String>> reportTaken() {
			final MinkaClient client = MinkaClient.getInstance();
			long now = System.currentTimeMillis();
			if (lastSize != runningDuties.size() || now - lastPrint > 20 * 1000) {
				lastPrint = now;
				lastSize = runningDuties.size();
				logger.info("{} {{}} running: {} ({})", shardId, client != null ? 
						client.isCurrentLeader() ? "l" : "f" : "?", 
						this.runningDuties.size(), toStringGroupByPallet(runningDuties));
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
			this.shardId = MinkaClient.getInstance().getShardIdentity();
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