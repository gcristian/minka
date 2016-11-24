/**
 * Copyright (c) 2011-2015 Zauber S.A. -- All rights reserved
 */

package io.tilt.minka.domain;

import java.io.Serializable;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.tilt.minka.api.Duty;
import io.tilt.minka.api.MinkaClient;
import io.tilt.minka.api.Pallet;
import io.tilt.minka.api.PartitionMaster;
import io.tilt.minka.utils.LogUtils;

/**
 * This's a sample delegate that only registers taken and released duties into a Set 
 *  
 * @author Cristian Gonzalez
 * @since Oct 24, 2016
 *
 */
public abstract class BaseSampleDelegate implements PartitionMaster<String, String>, Serializable {

	private static final long serialVersionUID = 1L;
	private static final Logger logger = LoggerFactory.getLogger(BaseSampleDelegate.class);

	// TreeSet() to enter them in the default minka order: comparator by id   
	private Set<Duty<String>> allOriginalDuties = new TreeSet<>();
	private Set<Pallet<String>> allOriginalPallets = new TreeSet<>();
	private Set<Duty<String>> runningDuties = new TreeSet<>();

	private String shardId = "{NN}";
	
	public BaseSampleDelegate() throws Exception {
		super();
		init();
		this.allOriginalDuties = buildDuties();
		this.allOriginalPallets = buildPallets();
		logger.info("{} Loading {} duties: {}", shardId, allOriginalDuties.size(), toStringGroupByPallet(allOriginalDuties));
	}

	public abstract Set<Duty<String>> buildDuties() throws Exception ;
	public abstract Set<Pallet<String>> buildPallets() throws Exception;
	public abstract void init() throws Exception ;
	
	public MinkaClient getMinkaClient() {
		return MinkaClient.getInstance();
	}
	
	@Override
	public void update(Duty<String> duty) {
		logger.info("{} updating: {}", shardId, duty.getId());
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

	private static String toStringWithPalletIds(Collection<Duty<String>> entities) {
		final StringBuilder sb = new StringBuilder();
		entities.forEach(e -> sb.append("p" + e.getPalletId() + "d" + e.getId() + ", "));
		return sb.toString();
	}

	@Override
	public void take(Set<Duty<String>> entities) {
		logger.info(LogUtils.titleLine(LogUtils.HYPHEN_CHAR, "taking"));
		logger.info("{} # {}+ ({})", shardId, entities.size(), toStringIds(entities));
		if (runningDuties == null) {
			runningDuties = new HashSet<>();
		}
		runningDuties.addAll(entities);
	}

	@Override
	public void release(Set<Duty<String>> entities) {
		logger.info(LogUtils.titleLine(LogUtils.HYPHEN_CHAR, "releasing"));
		logger.info("{} # -{} ({})", shardId, entities.size(), toStringIds(entities));
		runningDuties.removeAll(entities);
	}

	int lastSize;
	long lastPrint;

	@Override
	public Set<Duty<String>> reportTaken() {
		final MinkaClient client = MinkaClient.getInstance();
		long now = System.currentTimeMillis();
		if (lastSize != runningDuties.size() || now - lastPrint > 60000 * 1) {
			lastPrint = now;
			lastSize = runningDuties.size();
			logger.info(LogUtils.titleLine(LogUtils.HYPHEN_CHAR, "running"));
			if (logger.isDebugEnabled()) {
				logger.debug("{} {{}} # {} ({})", shardId, client != null ? client.isCurrentLeader() ? 
						"leader" : "follower" : "?", this.runningDuties.size(), toStringGroupByPallet(runningDuties));
			} else {
				logger.info("{} {{}} # {} ", shardId, client != null ? client.isCurrentLeader() ? 
						"leader" : "follower" : "?", this.runningDuties.size());
			}
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
	public void deliver(Duty<String> duty, Serializable clientPayload) {
		logger.info("receiving delivery: {}", duty.getId());
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

	@Override
	public double getTotalCapacity(Pallet<String> pallet) {
		return 0d;
	}


}