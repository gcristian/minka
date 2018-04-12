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
package io.tilt.minka.core.leader.distributor;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;

import io.tilt.minka.core.leader.PartitionTable;
import io.tilt.minka.core.leader.PartitionTable.Scheme;
import io.tilt.minka.core.leader.balancer.Balancer;
import io.tilt.minka.domain.EntityEvent;
import io.tilt.minka.domain.EntityJournal;
import io.tilt.minka.domain.EntityJournal.Log;
import io.tilt.minka.domain.Shard;
import io.tilt.minka.domain.ShardEntity;
import io.tilt.minka.domain.EntityState;
import io.tilt.minka.utils.CollectionUtils;
import io.tilt.minka.utils.LogUtils;

/**
 * Distribution changes require a consistent plan. 
 * Distribution in progress, created thru {@linkplain Migrator} 
 * indirectly by the {@linkplain Balancer} analyzing the {@linkplain Scheme}.
 * Composed of deliveries, migrations, deletions, creations, etc.
 *  
 * Such operations takes coordination to avoid parallelism and inconsistencies 
 * while they're yet to confirm, and needs to stay still while shards react, they also may fall.
 * 
 * No new distributions are made while this isn't finished
 * 
 * @author Cristian Gonzalez
 * @since Dec 11, 2015
 */
@JsonAutoDetect
@JsonPropertyOrder({"id", "created", "started", "elapsed", "ended", "deliveries", "pending"})
public class Plan implements Comparable<Plan> {

	protected static final Logger logger = LoggerFactory.getLogger(Plan.class);
	
	public static final int PLAN_UNKNOWN = -1;
	public static final int PLAN_WITHOUT = 0;
	
	private static final AtomicLong sequence = new AtomicLong();
	
	private final long id;
	private final Instant created;
    private final long maxMillis;
    private final long maxRetries;

	private final Map<EntityEvent, Map<Shard, List<ShardEntity>>> shippings;
	
	private Delivery lastDelivery;
	private int deliveryIdx;
	private Iterator<Delivery> iterator;
	private List<Delivery> deliveries;
	private Instant started;
	private Instant ended;
	private Result result = Result.RUNNING;
	private int retryCounter;

	protected Plan(final long id, final long maxMillis, final int maxRetries) {
		this.id = id;
		this.created = Instant.now();
		this.shippings = new HashMap<>();
		this.deliveries = new ArrayList<>();
		this.maxMillis = maxMillis;
		this.maxRetries = maxRetries;
	}

	public Plan(final long maxMillis, final int maxRetries) {
		this(sequence.incrementAndGet(), maxMillis, maxRetries);
	}

	public void discard() {
		sequence.decrementAndGet();
	}

	public static enum Result {
		/* still a running plan */
		RUNNING,
		/* resending deliveries for a nth time */
		RETRYING,
		/* all deliveries passed from enqueued to pending and confirmed */
		CLOSED_APPLIED,
		/* plan contains invalid shippings unable to deliver */
		CLOSED_ERROR,
		/* some deliveries became obsolete/impossible, rebuilding is required */
		CLOSED_OBSOLETE,
		/* some deliveries were never confirmed beyond retries/waiting limits */
		CLOSED_EXPIRED;

		public boolean isSuccess() {
			return this == CLOSED_APPLIED;
		}

		public boolean isClosed() {
			return this != RUNNING && this != RETRYING;
		}
	}

	public void obsolete() {
		this.result = Result.CLOSED_OBSOLETE;
		logger.warn("{}: Plan going {}", getClass().getSimpleName(), result);
		this.ended = Instant.now();
	}

	public Result getResult() {
		return this.result;
	}

	@JsonIgnore
	public List<Delivery> getAllPendings() {
		return deliveries.stream()
				.filter(d -> d.getStep() == Delivery.Step.PENDING)
				.collect(Collectors.toList());
	}
	
	@JsonIgnore
	public List<ShardEntity> getAllNonConfirmedFromAllDeliveries() {
		final List<ShardEntity> ret = new LinkedList<>();
		for (final Delivery delivery: deliveries) {
			for (final ShardEntity duty: delivery.getDuties()) {
				for (final Log log : duty.getJournal().getLogs(id)) {
					final EntityState ls = log.getLastState();
					if (ls==EntityState.PENDING ||
							ls == EntityState.DANGLING ||
							ls == EntityState.PREPARED ||
							ls == EntityState.MISSING ||
							ls == EntityState.STUCK) {
						ret.add(duty);
					}
				}
			}
		}
		return ret;
	}
		
	public Delivery getDelivery(final Shard shard) {
		return deliveries.stream()
				.filter(d -> d.getStep() == Delivery.Step.PENDING && d.getShard().equals(shard))
				.findFirst()
				.orElse(null);
	}

	private static List<EntityEvent> consistentEventsOrder = Arrays.asList(
			EntityEvent.REMOVE,
			EntityEvent.DETACH,
			EntityEvent.CREATE,
			EntityEvent.ATTACH);

	/**
	 * transforms shippings of transfers and overrides, into a consistent gradual change plan
	 * @return whether or not there're deliveries to distribute. */
	public boolean prepare() {
		this.started= Instant.now();
		int order = 0;
		for (final EntityEvent event: consistentEventsOrder) {
			if (shippings.containsKey(event)) {
				for (final Entry<Shard, List<ShardEntity>> e: shippings.get(event).entrySet()) {
					// one delivery for each shard
					if (!e.getValue().isEmpty()) {
						deliveries.add(new Delivery(e.getValue(), e.getKey(), event, order++, id));
					}
				}
			}
		}
		checkAllEventsPaired(deliveries, (unpaired)-> {
			this.result = Result.CLOSED_ERROR;
			this.ended = Instant.now();
			logger.error("{}: Invalid Plan with an operation unpaired: " + unpaired.toBrief());
		});
		this.shippings.clear();
		iterator = deliveries.iterator();
		return iterator.hasNext();
	}

	public Delivery next() {
		if (!hasNextParallel()) {
			throw new IllegalAccessError("no permission to advance forward");
		}
		lastDelivery = iterator.next();
		deliveryIdx++;
		return lastDelivery;
	}
	
	 /** @return whether or not all sent and pending deliveries were confirmed
	  * and following ENQUEUED deliveries can be requested */
	public boolean hasUnlatched() {
		return deliveries.stream().allMatch(d -> d.getStep() == Delivery.Step.DONE);

	}

	/** @return the inverse operation expected at another shard */
	private static EntityEvent inverse(final Delivery del) {
		// we care only movements
		switch (del.getEvent()) {
		case ATTACH:
			return EntityEvent.DETACH;
		case DETACH:
			return EntityEvent.ATTACH;
		default:
			return null;
		}
	}

	/**
	 * A last consistency check to avoid driving an invalid plan
	 * 
	 * @param deliveries2
	 * @throws Exception
	 *             last check for paired movement operations: a DETACH must be
	 *             followed by an ATTACH and viceversa
	 */
	private static void checkAllEventsPaired(
			final List<Delivery> deliveries, 
			final Consumer<ShardEntity> unpaired) {
		final Set<ShardEntity> alreadyPaired = new TreeSet<>();
		for (Delivery del : deliveries) {
			final EntityEvent inversion = inverse(del);
			if (inversion == null) {
				continue;
			}
			for (final ShardEntity entity : del.getDuties()) {
				final EntityJournal j = entity.getJournal();
				if (!alreadyPaired.contains(entity) && j
						.hasEverBeenDistributed() && entity.getLastEvent() != EntityEvent.REMOVE) {
					boolean pair = false;
					for (Delivery tmp : deliveries) {
						pair |= tmp.getDuties()
								.stream()
								.filter(e -> e.equals(entity) && e.getLastEvent() == inversion)
								.findFirst()
								.isPresent();
						if (pair) {
							alreadyPaired.add(entity);
							break;
						}
					}
					if (!pair) {
						// TODO no funciona xq planFactory.addMisingCrud reinicia con CREATE
						// avoid unpaired danglings/missing being a/dettached
						if (!wasRecentlyUnifinishedlyAllocated(j)) {
							unpaired.accept(entity);
						}
					}
				}
			}
		}
	}

	private static boolean wasRecentlyUnifinishedlyAllocated(final EntityJournal j) {
		boolean isLegitUnpair = false;
		int maxHistory = 2; // max history back will find a VALID CREATION in some steps
		for (final Iterator<Log> it = j.descendingIterator(); it.hasNext() && maxHistory > 0;) {
			final Log l = it.next();
			if (isLegitUnpair = (l.getEvent()==EntityEvent.CREATE
					// any suspicious state
					|| l.getLastState()==EntityState.MISTAKEN
					|| l.getLastState()==EntityState.PREPARED
					|| l.getLastState()==EntityState.MISSING
					|| l.getLastState()==EntityState.PENDING 
					|| l.getLastState()==EntityState.DANGLING)) {
				break;
			} else {
				maxHistory--;
			}
		}
		return isLegitUnpair;
	}
    
	private final ReentrantLock lock = new ReentrantLock(true);

	private boolean withLock(final Callable<Boolean> run) {
		try {
			if (!lock.tryLock(1, TimeUnit.SECONDS)) {
				return false;
			}
			return run.call();
		} catch (Exception e) {
			logger.error("unexpected", e);
			return false;
		} finally {
			lock.unlock();
		}
	}

    /** @return whether caller has permission to get next delivery   */
	public boolean hasNextParallel() {
		return withLock(() -> {
			if (started == null) {
				throw new IllegalStateException("Plan not prepared yet !");
			}
			// set done if none pending
			deliveries.forEach(d -> d.checkState());
			if (!iterator.hasNext()) {
				return false;
			}
			// there's more, but are they allowed right now ?
			if (lastDelivery == null) {
				// first time here
				return true;
			}
			if (deliveries.get(deliveryIdx).getEvent() == lastDelivery.getEvent()) {
				// identical future events are parallelized
				return true;
			}
			// other future events require all past confirmed
			for (int i = 0; i < deliveryIdx; i++) {
				final Delivery d = deliveries.get(i);
				if (d.getStep() == Delivery.Step.PENDING) {
					if (logger.isInfoEnabled()) {
						logger.info("{}: No more parallels: past deliveries yet pending", getClass().getSimpleName());
					}
					return false;
				}
			}
			return true;
		});
	}

	/** @return if all deliveries are fully confirmed */
	public void computeState() {
		if (this.result.isClosed()) {
			return;
		} else if (deliveries.isEmpty()) {
			throw new IllegalStateException("plan without deliveries cannot compute state");
		}
		boolean allDone = true;
		for (Delivery d : deliveries) {
			if (d.getStep() != Delivery.Step.DONE) {
				allDone = false;
				break;
			}
		}
		if (allDone) {
			this.result = Result.CLOSED_APPLIED;
			this.ended = Instant.now();
		} else {
			final Instant expiration = started.plusMillis(maxMillis);
			if (expiration.isBefore(Instant.now())) {
				if (retryCounter == this.maxRetries) {
					logger.warn("{}: Abandoning Plan expired ! (max secs:{}) ", getClass().getSimpleName(), maxMillis);
					this.result = Result.CLOSED_EXPIRED;
					this.ended = Instant.now();
				} else {
					retryCounter++;
					this.started = Instant.now();
					logger.warn("{}: ReSending Plan expired: Retry {} (max secs:{}) ",
							getClass().getSimpleName(), retryCounter, maxMillis);
					this.result = Result.RETRYING;
				}
			} else {
				if (logger.isInfoEnabled()) {
					logger.info("{}: Drive in progress waiting recent deliveries ({}'s to expire)",
							getClass().getSimpleName(),
							getId(),
							(expiration.toEpochMilli() - System.currentTimeMillis()) / 1000);
				}
				this.result = Result.RUNNING;
			}
		}
	}

	/** 
	 * declare a dettaching or attaching step to deliver on a shard
	 * 
	 * suppose all shards went offline except for shard 2
	 * 
	 * event:dettach->{
	 * 		{shard1:[duty1, duty2]}, 
	 * 		{shard3:[duty3]}
	 * }
	 * event:attach-> {
	 * 		{shard2:[duty1, duty2]}, 
	 * 		{shard2:[duty3]}
	 * }
	 */
	protected void ship(final Shard shard, final ShardEntity duty) {
		withLock(() -> {
			CollectionUtils.getOrPut(
					CollectionUtils.getOrPut(
							shippings,
							duty.getLastEvent(),
							() -> new HashMap<>()),
					shard,
					() -> new ArrayList<>())
					.add(duty);
			return false;
		});
	}
	
	@JsonIgnore
	public boolean areShippingsEmpty() {
		return shippings.isEmpty();
	}
		
	public LocalDateTime getStarted() {
		return LocalDateTime.ofInstant(started, ZoneId.systemDefault());
	}

	public long getId() {
		return this.id;
	}

	@java.lang.Override
	public int compareTo(Plan o) {
		return o.getCreation().compareTo(getCreation());
	}
	
	/////// serialization ///////
	
	@JsonProperty("elapsed-ms")
	private String getElapsed() {
		return LogUtils.humanTimeDiff(started.toEpochMilli(), 
				ended == null ? System.currentTimeMillis() : ended.toEpochMilli());
	}

	@JsonProperty("deliveries")
	private List<Delivery> getDeliveries() {
		return this.deliveries;
	}

	@JsonProperty("created")
	private String getCreation_() {
		return created.toString();
	}
	@JsonProperty("started")
	private String getStarted_() {
		if (started==null) {
			return "";
		}
		return started.toString();
	}
	@JsonProperty("ended")
	private String getEnded_() {
		if (ended==null) {
			return "";
		}
		return ended.toString();
	}
	
	@JsonIgnore
	public Instant getCreation() {
		return this.created;
	}

	public static void main(String[] args) throws InterruptedException {

		final CollectionUtils.SlidingSortedSet<Plan> set = CollectionUtils.sliding(5);
		for (int i = 0; i < 10; i++) {
			System.out.println(new DateTime(DateTimeZone.UTC));
			set.add(new Plan(i, 1, 1));
			Thread.sleep(200);
		}
		System.out.println("----------");
		for (Plan p : set.values()) {
			assert (p.getId() >= 5);
		}
	}

}
