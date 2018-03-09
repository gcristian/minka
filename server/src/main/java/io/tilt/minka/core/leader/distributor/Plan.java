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
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;
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
import io.tilt.minka.domain.EntityEvent;
import io.tilt.minka.domain.Shard;
import io.tilt.minka.domain.ShardEntity;
import io.tilt.minka.domain.EntityState;
import io.tilt.minka.utils.CollectionUtils;
import io.tilt.minka.utils.LogUtils;

/**
 * Acts as a driveable distribution in progress, created thru {@linkplain Migrator} 
 * indirectly by the {@linkplain Balancer} analyzing the {@linkplain PartitionTable}.
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

	static final Logger logger = LoggerFactory.getLogger(Plan.class);
	
	public static final int PLAN_UNKNOWN = -1;
	public static final int PLAN_WITHOUT = 0;
	
	private static final AtomicLong sequence = new AtomicLong();
	
	private final long id;
	private final Date created;

	private final Map<EntityEvent, Map<Shard, List<ShardEntity>>> shippings;
	
	private Delivery lastDelivery;
	private int deliveryIdx;
	private Iterator<Delivery> iterator;
	private List<Delivery> deliveries;
	private Date started;
	private Date ended;
	private Result result = Result.RUNNING;
	private int retryCounter;
    private final int maxSeconds;
    private final int maxRetries;
    
    protected Plan(final long id, final int maxSeconds, final int maxRetries) {
        this.id = id;
        this.created = new Date();
        this.shippings = new HashMap<>();
        this.deliveries = new ArrayList<>();
        this.maxSeconds = maxSeconds;
        this.maxRetries = maxRetries;
    }

    public Plan(final int maxSeconds, final int maxRetries) {
        this(sequence.incrementAndGet(), maxSeconds, maxRetries);
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
    	CLOSED_EXPIRED
    	;

    	public boolean isSuccess() {
    		return this == CLOSED_APPLIED;
    	}
    	public boolean isClosed() {
    	    return this != RUNNING && this !=RETRYING;
    	}
    }
    
    public void obsolete() {
    	this.result = Result.CLOSED_OBSOLETE;
    	logger.warn("{}: Plan going {}", getClass().getSimpleName(), result);
    	this.ended = new Date();
    }

    public Result getResult() {
		return this.result;
	}
    
	@JsonIgnore
	public List<Delivery> getAllPendings() {
	    return deliveries.stream()
	            .filter(d->d.getStep()==Delivery.Step.PENDING)
	            .collect(Collectors.toList());
	}
	
	@JsonIgnore
	public List<ShardEntity> getAllNonConfirmedFromAllDeliveries() {
		final List<ShardEntity> ret = new ArrayList<>();
		for (final Delivery delivery: deliveries) {
			for (final ShardEntity duty: delivery.getDuties()) {
				if (duty.getLastState()==EntityState.PENDING || 
				        duty.getLastState() == EntityState.PREPARED ||
				        duty.getLastState() == EntityState.MISSING ||
				        duty.getLastState() == EntityState.STUCK) {
					ret.add(duty);
				}
			}
		}
		return ret;
	}
		
	public Delivery getDelivery(final Shard shard) {
	    return deliveries.stream()
	        .filter(d->d.getStep()==Delivery.Step.PENDING && d.getShard().equals(shard))
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
		this.started= new Date();
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
		checkAllEventsPaired();
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
    private EntityEvent inverse(final Delivery del) {
        // we care only movements
        if (del.getEvent() == EntityEvent.ATTACH) {
            return EntityEvent.DETACH;
        } else if (del.getEvent() == EntityEvent.DETACH) {
            return EntityEvent.ATTACH;
        }
        return null;
    }
    
    /** @throws Exception last check for paired movement operations: 
     * a DETACH must be followed by an ATTACH and viceversa */
    private void checkAllEventsPaired() {
        final Set<ShardEntity> paired = new TreeSet<>();
        for (Delivery del : deliveries) {
            final EntityEvent toPairFirstTime = inverse(del);
            if (toPairFirstTime != null) {
                continue;
            }
            for (final ShardEntity entity : del.getDuties()) {
                if (!paired.contains(entity) 
                		&& entity.getLog().hasEverBeenDistributed() 
                		&& entity.getLastEvent()!=EntityEvent.REMOVE) {
                    boolean pair = false;
                    for (Delivery tmp : deliveries) {
                        pair |= tmp.getDuties().stream()
                                .filter(e -> e.equals(entity) 
                                		&& e.getLastEvent() == toPairFirstTime)
                                .findFirst()
                                .isPresent();
                        if (pair) {
                            paired.add(entity);
                            break;
                        }
                    }
                    if (!pair) {
                    	this.result = Result.CLOSED_ERROR;
                    	this.ended = new Date();
                        throw new IllegalStateException("Invalid Plan with an operation unpaired: " + entity.toBrief());
                    }
                }
            }
        }
    }
    
    private final ReentrantLock lock = new ReentrantLock(true);

    private boolean withLock(final Callable<Boolean> run) {
        try {
            if (!lock.tryLock(3, TimeUnit.SECONDS)) {
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
        return withLock(()-> {
            if (started==null) {
                throw new IllegalStateException("Plan not prepared yet !");
            }
            // set done if none pending
            deliveries.forEach(d->d.checkState());
            if (!iterator.hasNext()) {
                return false;
            } else {
    	        // there's more, but are they allowed right now ?
    			if (lastDelivery==null) {
    			    // first time here
    			    return true;
    			} else {	
    			    final EntityEvent nextDeliveryEvent = deliveries.get(deliveryIdx).getEvent();
    		        if (nextDeliveryEvent==lastDelivery.getEvent()) {
    		            // identical future events are parallelized
    		            return true;
    			    } else {
    			        // future events require all past confirmed
    			        for (int i = 0; i < deliveryIdx; i++) {
    			        	final Delivery d = deliveries.get(i);
    			            if (d.getStep() == Delivery.Step.PENDING) {
    			                logger.info("{}: No more parallels: past deliveries yet pending", getClass().getSimpleName());
    			                return false;
    			            }
    			        }
    			        return true;
    			    }
    			}
            }
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
        for (Delivery d: deliveries) {
            if (d.getStep() != Delivery.Step.DONE) {
                allDone = false;
                break;
            }
        }
        if (allDone) {
            this.result = Result.CLOSED_APPLIED;
            this.ended = new Date();
        } else {
            final Instant expiration = started.toInstant().plusSeconds(maxSeconds);
    		if (expiration.isBefore(Instant.now())) {
    			if (retryCounter == this.maxRetries) {
    				logger.info("{}: Abandoning Plan expired ! (max secs:{}) ", getClass().getSimpleName(), maxSeconds);
    				this.result = Result.CLOSED_EXPIRED;
    				this.ended = new Date();
    			} else {
    				retryCounter++;
    				this.started = new Date();
    				logger.info("{}: ReSending Plan expired: Retry {} (max secs:{}) ", getClass().getSimpleName(),
    						retryCounter, maxSeconds);
    				this.result = Result.RETRYING;
    			}
    		} else {
    			logger.info("{}: Drive in progress waiting recent deliveries ({}'s to expire)", getClass().getSimpleName(),
    					getId(), (expiration.toEpochMilli() - System.currentTimeMillis()) / 1000);
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
	    withLock(()-> {
    		CollectionUtils.getOrPut(
    			CollectionUtils.getOrPut(
    	            shippings, 
    		        duty.getLastEvent(), 
    		        ()->new HashMap<>()), 
    		    shard, 
    		    ()->new ArrayList<>())
    	    .add(duty);
    		return false;
	    });
	}
	
	@JsonIgnore
	public boolean areShippingsEmpty() {
		return shippings.isEmpty();
	}
		
	public Date getStarted() {
		return this.started;
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
		return LogUtils.humanTimeDiff(started.getTime(), 
				ended == null ? System.currentTimeMillis() : ended.getTime());
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
	public Date getCreation() {
		return this.created;
	}

	public static void main(String[] args) throws InterruptedException {
        
	    final CollectionUtils.SlidingSortedSet<Plan> set = CollectionUtils.sliding(5);
	    for (int i = 0; i < 10; i++) {
	        System.out.println(new DateTime(DateTimeZone.UTC));
	        set.add(new Plan(i, 1,1));
	        Thread.sleep(200);
	    }
	    System.out.println("----------");
	    for (Plan p : set.values()) {
	        assert(p.getId()>=5);
	    }
    }

}
