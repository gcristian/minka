/**
 * Copyright (c) 2011-2015 Zauber S.A. -- All rights reserved
 */

package io.tilt.minka.domain;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.tilt.minka.api.Duty;
import io.tilt.minka.api.MinkaClient;
import io.tilt.minka.api.MinkaContextLoader;
import io.tilt.minka.api.Pallet;
import io.tilt.minka.utils.LogUtils;

/**
 * A sample delegate that only logs what's being instructed by Minka.
 * Also shows using MinkaContextLoader to map events to consumers
 * instead of providing a spring bean representing a PartitionDelegate implementation class.
 * 
 * @author Cristian Gonzalez
 * @since Nov 9, 2016
 */
public abstract class AbstractMappingEventsApp {

	private static final Logger logger = LoggerFactory.getLogger(AbstractMappingEventsApp.class);

	// TreeSet() to enter them in the default minka order: comparator by id   
	private Set<Duty<String>> allOriginalDuties = new TreeSet<>();
	private Set<Pallet<String>> allOriginalPallets = new TreeSet<>();
	private Set<Duty<String>> runningDuties = new TreeSet<>();

	private MinkaContextLoader<String, String> loader;

	private String shardId = "{NN}";
	
	public abstract Set<Duty<String>> buildDuties() throws Exception ;
	public abstract Set<Pallet<String>> buildPallets() throws Exception;
	public abstract double getTotalCapacity(Pallet<String> pallet);
	
	public abstract void init() throws Exception ;
	
	public AbstractMappingEventsApp() throws Exception {
		super();
		this.allOriginalDuties = buildDuties();
		this.allOriginalPallets = buildPallets();
		this.runningDuties = new HashSet<>();
	}
	
	public void startClientApp() throws Exception {
		logger.info("{} Loading {} duties: {}", shardId, allOriginalDuties.size(), toStringGroupByPallet(allOriginalDuties));
		
		loader = new MinkaContextLoader<>();
		loader.onPalletLoad(() -> allOriginalPallets);
		loader.onDutyLoad(()->allOriginalDuties);
		
		loader.onTake((final Set<Duty<String>> t) -> {
			logger.info(LogUtils.titleLine(LogUtils.HYPHEN_CHAR, "taking"));
			logger.info("{} # {}+ ({})", shardId, t.size(), toStringIds(t));
			runningDuties.addAll(t);
		});
		loader.onTakePallet((p)->logger.info("Taking pallet: {}", p.toString()));
		
		loader.onRelease((final Set<Duty<String>> entities)-> {
			logger.info(LogUtils.titleLine(LogUtils.HYPHEN_CHAR, "releasing"));
			logger.info("{} # -{} ({})", shardId, entities.size(), toStringIds(entities));
			runningDuties.removeAll(entities);
		});
		loader.onReleasePallet((p)->logger.info("Releasing pallet: {}", p.toString()));
		
		loader.onReport(()-> {
			final long now = System.currentTimeMillis();
			if (timeToLog(now)) {
				final MinkaClient client = MinkaClient.getInstance();
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
		});
		
		for (final Pallet<String> pallet: allOriginalPallets) {
			loader.setCapacity(pallet, getTotalCapacity(pallet));			
		}
		
		loader.onUpdate(d->{});
		loader.onActivation(()-> {
			logger.info("activating");
			this.shardId = MinkaClient.getInstance().getShardIdentity();
		});
		loader.onDeactivation(()->logger.info("de-activating"));
		loader.onDeliver((d, e)->logger.info("receiving delivery: {}", d));
		loader.load();
	}
	
	public MinkaContextLoader<String, String> getLoader() {
		return this.loader;
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

	public MinkaClient getMinkaClient() {
		return MinkaClient.getInstance();
	}
	
	public Set<Duty<String>> getAllOriginalDuties() {
		return this.allOriginalDuties;
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