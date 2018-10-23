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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.function.Supplier;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;

import io.tilt.minka.api.Config;
import io.tilt.minka.api.Duty;
import io.tilt.minka.core.leader.balancer.Balancer;
import io.tilt.minka.core.leader.data.CommittedState;
import io.tilt.minka.domain.CommitTree;
import io.tilt.minka.domain.CommitTree.Log;
import io.tilt.minka.domain.EntityEvent;
import io.tilt.minka.domain.EntityState;
import io.tilt.minka.domain.ShardEntity;
import io.tilt.minka.shard.Shard;
import io.tilt.minka.shard.ShardState;
import io.tilt.minka.utils.CollectionUtils;
import io.tilt.minka.utils.LogUtils;

/**
 * Plan of distribution changes, as a consequence of {@linkplain Balancer} recalculation, 
 * caused by CRUD operations to the {@linkplain CommittedState}. <br>
 * Contains a matrix of actions with minimum behaviour scoped to the pace of scheme change.
 * <br>
 * Composed of many {@linkplain Dispatch} objects sent to different {@linkplain Shard},
 * representing changes in progress of confirmation, which in turn forward to more dispatches. <br> 
 * The plan is driven by the {@linkplain Distributor}, and takes at most 4 steps in a consistent order. <br>
 * <br>
 * Built by the @{@linkplain ChangePlanFactory} and written by the {@linkplain Migrator}. <br>
 * The idea is to move duties from a place to another, the move is representdd as an {@linkplain EntityEvent}
 * removes are first, dettaches are before attaches, and attaches at last, 
 * same events on different shards are sent in parrallel. <br>
 * <br>
 * In case of fall of a shard, the plan gets obsolete and rebuilt according the new scheme, after a rebalance. <br>
 * A change plan only occurs when all the Shards in the cluster get {@linkplain ShardState} Online.   <br>
 * 
 * @author Cristian Gonzalez
 * @since Dec 11, 2015
 */
@JsonAutoDetect
@JsonPropertyOrder({"id", "created", "started", "elapsed", "ended", ChangePlan.FIELD_DISPATCHES, "pending"})
public class ChangePlan implements Comparable<ChangePlan> {

	protected static final Logger logger = LoggerFactory.getLogger(ChangePlan.class);
	public static final String FIELD_DISPATCHES = "dispatches";
	
	private final long id;
	private final Instant created;
    private final long maxMillis;
    private final long maxRetries;

	private final Map<EntityEvent, Map<Shard, List<ShardEntity>>> buildingDispatches;
	
	private Dispatch lastDelivery;
	private int deliveryIdx;
	private Iterator<Dispatch> iterator;
	private List<Dispatch> builtDispatches;
	private Instant started;
	private Instant ended;
	private ChangePlanState changePlanState = ChangePlanState.RUNNING;
	private int retryCounter;

	private static List<EntityEvent> consistentEventsOrder = Arrays.asList(
			EntityEvent.DROP,
			EntityEvent.DETACH,
			EntityEvent.STOCK,
			EntityEvent.ATTACH);

	ChangePlan(final long maxMillis, final int maxRetries) {
		this.created = Instant.now();
		this.buildingDispatches = new HashMap<>(consistentEventsOrder.size());
		this.builtDispatches = Collections.emptyList();
		this.maxMillis = maxMillis;
		this.maxRetries = maxRetries;
		// to avoid id retraction at leader reelection causing wrong journal order
		this.id = System.currentTimeMillis();
	}

	public long getId() {
		return this.id;
	}

	@JsonIgnore
	public Instant getCreation() {
		return this.created;
	}

	@java.lang.Override
	public int compareTo(ChangePlan o) {
		return o.getCreation().compareTo(getCreation());
	}

	public ChangePlanState getResult() {
		return this.changePlanState;
	}

	public Dispatch get(final Shard shard) {
		return builtDispatches.stream()
				.filter(d -> d.getStep() == Dispatch.Step.PENDING && d.getShard().equals(shard))
				.findFirst()
				.orElse(null);
	}

	void obsolete() {
		this.changePlanState = ChangePlanState.CLOSED_OBSOLETE;
		logger.warn("{}: ChangePlan going {}", getClass().getSimpleName(), changePlanState);
		this.ended = Instant.now();
	}
	
	boolean isDispatching(final ShardEntity duty, EntityEvent...anyOf) {
		return isDispatching_(duty, null, anyOf);
	}
	boolean isDispatching(final ShardEntity duty, final Shard shard, EntityEvent...anyOf) {
		return isDispatching_(duty, shard, anyOf);
	}
	private boolean isDispatching_(final ShardEntity duty, final Shard shard, final EntityEvent...anyOf) {
		for (EntityEvent ee: anyOf) {
			if (buildingDispatches.containsKey(ee)) {
				for (final Map.Entry<Shard, List<ShardEntity>> e: buildingDispatches.get(ee).entrySet()) {
					if ((shard==null || shard.equals(e.getKey()))
							&& e.getValue().contains(duty)) {
						return true;
					}
				}
			}
		}
		return false;
	}
	
	void onDispatchesFor(final EntityEvent event, final Shard shard, final BiConsumer<Shard, ShardEntity> c) { 
		final Map<Shard, List<ShardEntity>> map = buildingDispatches.get(event);
		if (map!=null) {
			for (Entry<Shard, List<ShardEntity>> x : map.entrySet()) {
				if (x!=null && (shard==null || shard.equals(x.getKey()))) {
					x.getValue().forEach(s->c.accept(x.getKey(), s));
				}
			}
		}
	}

	@JsonIgnore
	void onBuiltDispatches(final Predicate<Dispatch> test, final Consumer<Dispatch> d) {
		builtDispatches.stream()
				.filter(test)
				.forEach(d);
	}
	
	int findAllNonConfirmedFromAllDeliveries(final Consumer<ShardEntity> c) {
		int rescueCount = 0;
		for (final Dispatch dispatch: builtDispatches) {
			for (final ShardEntity duty: dispatch.getDuties()) {
				for (final Log log : duty.getCommitTree().getLogs(id)) {
					final EntityState ls = log.getLastState();
					if (ls==EntityState.PENDING ||
							ls == EntityState.DANGLING ||
							ls == EntityState.PREPARED ||
							ls == EntityState.MISSING ||
							ls == EntityState.STUCK) {
						c.accept(duty);
						rescueCount++;
					}
				}
			}
		}
		return rescueCount;
	}
	
	/**
	 * transforms shippings of transfers and overrides, into a consistent gradual change plan
	 * @return whether or not there're dispatches to distribute. */
	boolean build() {
		this.started= Instant.now();
		int order = 0;
		for (final EntityEvent event: consistentEventsOrder) {
			if (buildingDispatches.containsKey(event)) {
				for (final Entry<Shard, List<ShardEntity>> e: buildingDispatches.get(event).entrySet()) {
					// one delivery for each shard
					if (!e.getValue().isEmpty()) {
						if (builtDispatches.isEmpty()) {
							builtDispatches = new ArrayList<>();
						}
						builtDispatches.add(new Dispatch(e.getValue(), e.getKey(), event, order++, id));
					}
				}
			}
		}
		validateAllEventsPaired(builtDispatches, (unpaired)-> {
			this.changePlanState = ChangePlanState.CLOSED_ERROR;
			this.ended = Instant.now();
			logger.error("{}: Invalid ChangePlan with an operation unpaired: {}", getClass().getSimpleName(), 
					unpaired.toBrief());
		});
		this.buildingDispatches.clear();
		iterator = builtDispatches.iterator();
		return iterator.hasNext();
	}

	Dispatch next() {
		if (!hasNextParallel(null)) {
			throw new IllegalAccessError("no permission to advance forward");
		}
		lastDelivery = iterator.next();
		deliveryIdx++;
		return lastDelivery;
	}
	
	 /** @return whether or not all sent and pending dispatches were confirmed
	  * and following ENQUEUED dispatches can be requested */
	public boolean hasUnlatched() {
		return builtDispatches.stream().allMatch(d -> d.getStep() == Dispatch.Step.DONE);

	}

	/**
	 * A last consistency check to avoid driving an invalid plan
	 * 
	 * @param deliveries2
	 * @throws Exception
	 *             last check for paired movement operations: a DETACH must be
	 *             followed by an ATTACH and viceversa
	 */
	private static void validateAllEventsPaired(
			final List<Dispatch> dispatches, 
			final Consumer<ShardEntity> unpaired) {
		final Set<ShardEntity> alreadyPaired = new TreeSet<>();
		for (Dispatch del : dispatches) {
			
			if (del.getEvent().getType()!=EntityEvent.Type.CRUD) {
				continue;
			}
			
			final EntityEvent inversion = del.getEvent().toNegative();
			for (final ShardEntity entity : del.getDuties()) {
				final CommitTree j = entity.getCommitTree();
				if (!alreadyPaired.contains(entity) 
						&& j.hasEverBeenDistributed() 
						&& entity.getLastEvent() != EntityEvent.REMOVE) {
					boolean pair = false;
					for (Dispatch tmp : dispatches) {
						if (pair |= tmp.getDuties()
								.stream()
								.filter(e -> e.equals(entity) && e.getLastEvent() == inversion)
								.findFirst()
								.isPresent()) {
							alreadyPaired.add(entity);
							break;
						}
					}
					if (!pair) {
						// avoid unpaired danglings/missing being a/dettached
						if (!j.hasUnifinishedEvent(2, j.getLast().getEvent())) {
							unpaired.accept(entity);
						}
					}
				}
			}
		}
	}
    
    /** @return whether caller has permission to get next delivery   */
	boolean hasNextParallel(final Consumer<String> c) {
		if (started == null) {
			throw new IllegalStateException("ChangePlan not prepared yet !");
		}
		// set done if none pending
		builtDispatches.forEach(d -> d.calculateState(c));
		if (!iterator.hasNext()) {
			return false;
		}
		// there's more, but are they allowed right now ?
		if (lastDelivery == null) {
			// first time here
			return true;
		}
		if (builtDispatches.get(deliveryIdx).getEvent() == lastDelivery.getEvent()) {
				//|| lastDelivery.getEvent().getType()==EntityEvent.Type.REPLICA) {
			// identical future events are parallelized
			return true;
		}
		// other future events require all past confirmed
		for (int i = 0; i < deliveryIdx; i++) {
			final Dispatch d = builtDispatches.get(i);
			if (d.getStep() == Dispatch.Step.PENDING) {
				c.accept(String.format("%s: No more parallels: past dispatches yet pending", 
						getClass().getSimpleName()));
				return false;
			}
		}
		return true;
	}

	/** recalculates state according its dispatchs states */
	void calculateState() {
		if (this.changePlanState.isClosed()) {
			return;
		} else if (builtDispatches.isEmpty()) {
			throw new IllegalStateException("plan without dispatches cannot compute state");
		}
		boolean allDone = true;
		for (Dispatch d : builtDispatches) {
			if (d.getStep() != Dispatch.Step.DONE) {
				allDone = false;
				break;
			}
		}
		if (allDone) {
			this.changePlanState = ChangePlanState.CLOSED_APPLIED;
			this.ended = Instant.now();
		} else {
			final Instant expiration = started.plusMillis(maxMillis);
			if (expiration.isBefore(Instant.now())) {
				if (retryCounter == this.maxRetries) {
					logger.warn("{}: Abandoning ChangePlan expired ! (max secs:{}) ", getClass().getSimpleName(), maxMillis);
					this.changePlanState = ChangePlanState.CLOSED_EXPIRED;
					this.ended = Instant.now();
				} else {
					retryCounter++;
					this.started = Instant.now();
					logger.warn("{}: ReSending ChangePlan expired: Retry {} (max secs:{}) ",
							getClass().getSimpleName(), retryCounter, maxMillis);
					this.changePlanState = ChangePlanState.RETRYING;
				}
			} else {
				if (logger.isInfoEnabled()) {
					logger.info("{}: id:{} in progress ({}'s to expire)",
							getClass().getSimpleName(),
							getId(),
							secsToExpire(expiration));
				}
				this.changePlanState = ChangePlanState.RUNNING;
			}
		}
	}

	long secsToExpire(final Instant expiration) {
		return (expiration.toEpochMilli() - System.currentTimeMillis()) / 1000;
	}
	
	long secsToExpire(final Config config) {
		final Instant st = started.plusMillis(
				config.beatToMs(config.getDistributor().getPlanExpiration()));
		return (st.toEpochMilli() - System.currentTimeMillis()) / 1000;
	}

	/** @return the dispatching event for the duty if existing, else: NULL */
	public boolean hasDispatch(final Duty duty, final EntityEvent ee) {
		for(Dispatch d: builtDispatches) {
			for (ShardEntity e: d.getDuties()) {
				if (ShardEntity.qualifiedId(e.getDuty()).equals(e.getQualifiedId()) 
						&& ee==d.getEvent()) {
					return true;
				}
			}
		}
		return false;
	}
	
	/** @return TRUE if Duty is moving betweem Shards */
	public boolean hasMigration(final Duty duty) {
		boolean detaching = false;
		boolean attaching = false;
		for(Dispatch d: builtDispatches) {
			for (ShardEntity e: d.getDuties()) {
				if (ShardEntity.qualifiedId(duty).equals(e.getQualifiedId())) {
					detaching |=d.getEvent()==EntityEvent.DETACH;
					attaching |=d.getEvent()==EntityEvent.ATTACH;
					break;
				}
			}
		}
		return detaching && attaching;
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
	void dispatch(final Shard shard, final ShardEntity duty) {
		getOrPut(
			getOrPut(
				buildingDispatches,
				duty.getLastEvent(),
				() -> new HashMap<>(2)),
			shard,
			() -> new ArrayList<>())
			.add(duty);
	}
	
	static <K, V>V getOrPut(final Map<K, V> map, final K key, final Supplier<V> sup) {
		if (map == null || key == null || sup == null) {
			throw new IllegalArgumentException("null map key or supplier");
		}
		V v = map.get(key);
		if (v == null) {
			map.put(key, v = sup.get());
		}
		return v;
	}

	
	@JsonIgnore
	boolean dispatchesEmpty() {
		return buildingDispatches.isEmpty();
	}
	
	@JsonIgnore
	Instant getStarted() {
		return started;
	}
	
	/////// serialization ///////
	
	@JsonProperty("elapsed-ms")
	private String getElapsed() {
		if (started!=null) {
			return LogUtils.humanTimeDiff(started.toEpochMilli(), 
				ended == null ? System.currentTimeMillis() : ended.toEpochMilli());
		}
		return "";
	}

	@JsonProperty(FIELD_DISPATCHES)
	private List<Dispatch> getDispatches() {
		return this.builtDispatches;
	}

	@JsonProperty("created")
	private String getCreation_() {
		return created.toString();
	}
	@JsonProperty("summary")
	private String getSummary() {
		final StringBuilder sb = new StringBuilder();
		if (!builtDispatches.isEmpty()) {
			for (final EntityEvent event: consistentEventsOrder) {
				int size = 0 ;
				for (Dispatch d: builtDispatches) {
					size+= d.getEvent() == event ? d.getDuties().size() : 0;
				}
				if (size>0) {
					sb.append(event).append(":").append(size).append(", ");
				}
			}
		}
		return sb.toString();
	}
	
	public static void main(String[] args) throws InterruptedException {

		final CollectionUtils.SlidingSortedSet<ChangePlan> set = CollectionUtils.sliding(5);
		for (int i = 0; i < 10; i++) {
			System.out.println(new DateTime(DateTimeZone.UTC));
			set.add(new ChangePlan(1, 1));
			Thread.sleep(200);
		}
		System.out.println("----------");
		for (ChangePlan p : set.values()) {
			assert (p.getId() >= 5);
		}
	}

}
