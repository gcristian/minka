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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;
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
import io.tilt.minka.domain.ShardEntity.State;
import io.tilt.minka.utils.LogUtils;
import io.tilt.minka.utils.SlidingSortedSet;

/**
 * Acts as a driveable distribution in progress, created thru {@linkplain Migrator} 
 * indirectly by the {@linkplain Balancer} analyzing the {@linkplain PartitionTable}.
 * Composed of deliveries, migrations, deletions, creations, etc.
 * s 
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
	
	private static List<EntityEvent> consistentEventsOrder = Arrays.asList(
	        EntityEvent.REMOVE, 
	        EntityEvent.DETACH, 
	        EntityEvent.CREATE, 
	        EntityEvent.ATTACH);
	
	private static final AtomicLong sequenceNumberator = new AtomicLong();
	private final long id;
	private final DateTime created;
	private final Map<EntityEvent, Map<Shard, List<ShardEntity>>> shippings;
	
	private Delivery lastDelivery;
	private int deliveryIdx;
	private Iterator<Delivery> iterator;
	private List<Delivery> deliveries;
	private DateTime started;
	private DateTime ended;
	private int retryCounter;

	@JsonIgnore
	public List<Delivery> getAllPendings() {
	    return deliveries.stream()
	            .filter(d->d.checkStatus()==Delivery.Status.PENDING)
	            .collect(Collectors.toList());
	}
	
	@JsonIgnore
	public List<ShardEntity> getAllPendingsFromAllDeliveries() {	    
		final List<ShardEntity> ret = new ArrayList<>();
		for (final Delivery delivery: deliveries) {
			for (final ShardEntity duty: delivery.getDuties()) {
				if (duty.getState()==State.PENDING) {
					ret.add(duty);
				}
			}
		}
		return ret;
	}
	
	private String getElapsed() {
	    return LogUtils.humanTimeDiff(getStarted().getMillis(), 
	            getEnded()==null ? System.currentTimeMillis() : getEnded().getMillis());
	}
	
	@JsonProperty("deliveries")
	private List<Delivery> getDeliveries() {
		return this.deliveries;
	}
	
	public Plan(final long id) {
	    this.id = id;
        this.created = new DateTime(DateTimeZone.UTC);      
        this.shippings = new HashMap<>();
        this.deliveries = new ArrayList<>();
	}
	public Plan() {
		this(sequenceNumberator.incrementAndGet());
	}

	public Delivery getDelivery(final Shard shard) {
	    return deliveries.stream()
	        .filter(d->d.checkStatus()==Delivery.Status.PENDING && d.getShard().equals(shard))
	        .findFirst()
	        .orElse(null);
	}
	
	/**
	 * transforms shippings of transfers and overrides, into a consistent gradual change plan
	 * @return whether or not there're deliveries to distribute. */
	public boolean prepare() {
		this.started= new DateTime(DateTimeZone.UTC);
		int order = 0;
		for (final EntityEvent event: consistentEventsOrder) {
			final Map<Shard, List<ShardEntity>> map = getOrPut(shippings, event, ()->new HashMap<>());
			for (final Entry<Shard, List<ShardEntity>> e: map.entrySet()) {
				deliveries.add(new Delivery(e.getValue(), e.getKey(), event, order++));
			}
		}
		//checkAllEventsPaired();
		this.shippings.clear();
		iterator = deliveries.iterator();
		return iterator.hasNext();
	}

	public Delivery next() {
		if (!hasNextAvailable()) {
			throw new IllegalAccessError("no permission to advance forward");
		}
		if (lastDelivery!=null) {
		    lastDelivery.checkStatus();
		}
		lastDelivery = iterator.next();
		deliveryIdx++;
		return lastDelivery;
	}
	
	 /** @return whether or not all sent and pending deliveries were confirmed
	  * and following enqueued deliveries can be requested */
    public boolean hasUnlatched() {
        return !deliveries.stream()
                .filter(d -> d.checkStatus() != Delivery.Status.ENQUEUED)
                .findFirst()
                .isPresent();
    }

    /** @return the inverse operation expected at another shard */
    private EntityEvent inverse(Delivery del) {
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
            final EntityEvent toPair = inverse(del);
            if (toPair == null) {
                continue;
            }
            for (ShardEntity entity : del.getDuties()) {
                if (!paired.contains(entity)) {
                    boolean pair = false;
                    for (Delivery tmp : deliveries) {
                        pair |= tmp.getDuties().stream()
                                .filter(e -> e.equals(entity) && e.getDutyEvent() == toPair)
                                .findFirst()
                                .isPresent();
                        if (pair) {
                            paired.add(entity);
                            break;
                        }
                    }
                    if (!pair) {
                        throw new IllegalStateException("Invalid Plan with an operation unpaired: " + entity.toBrief());
                    }
                }
            }
        }
    }

    /** @return whether caller has permission to get next delivery   */
    public boolean hasNextAvailable() {
        if (started==null) {
            throw new IllegalStateException("Plan not prepared yet !");
        }
        if (!iterator.hasNext()) {
            deliveries.forEach(d->d.checkStatus());
            return false;
        } 
        // there's more, but are they allowed right now ?
		if (lastDelivery==null) {
		    // first time here
		    return true;
		}		
	    final EntityEvent nextDeliveryEvent = deliveries.get(deliveryIdx).getEvent();
        if (nextDeliveryEvent==lastDelivery.getEvent()) {
            
            deliveries.forEach(d->d.checkStatus());
            // same future events (attach/dettach) are parallelized
            return true;
	    }
        // different future events require bookkeeper's confirmation (in stage)
        for (Delivery d: deliveries) {
            if (d.checkStatus() == Delivery.Status.PENDING) {
                logger.info("{}: Past Deliveries yet unconfirmed", getClass().getSimpleName());
                return false;
            }
        }
        return true;
	}
    
    /** @return if all deliveries are fully confirmed */ 
    public boolean hasFinalized() {
        for (Delivery d: deliveries) {
            if (d.checkStatus() != Delivery.Status.CONFIRMED) {
                return false;
            }
        }
        return true;
    }

	public void incrementRetry() {
		retryCounter++;
	}
	
	public int getRetryCount() {
		return retryCounter;
	}
	

	public void close() {
		if (hasNextAvailable()) {
			throw new IllegalStateException("plan not closeable last delivery unconfirmed !");
		}
		this.ended = new DateTime(DateTimeZone.UTC);
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
		
	/* declare a dettaching or attaching step to deliver on a shard */
	public synchronized void ship(final Shard shard, final ShardEntity duty) {
		final List<ShardEntity> list = getOrPut(
				getOrPut(shippings, duty.getDutyEvent(), ()->new HashMap<>()), 
				shard, ()->new ArrayList<>());
		list.add(duty);
	}

	public boolean isClosed() {
		return this.ended !=null;
	}
	
	@JsonIgnore
	public boolean areShippingsEmpty() {
		return shippings.isEmpty();
	}
	
	public DateTime getEnded() {
		return this.ended;
	}
	
	public DateTime getStarted() {
		return this.started;
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
		}return ended.toString();
	}

	@JsonIgnore
	public DateTime getCreation() {
		return this.created;
	}

	public long getId() {
		return this.id;
	}

	@Override
	public int compareTo(Plan o) {
		return o.getCreation().compareTo(getCreation());
	}
	
	public static void main(String[] args) throws InterruptedException {
        
	    final SlidingSortedSet<Plan> set = new SlidingSortedSet<>(5);
	    for (int i = 0; i < 10; i++) {
	        System.out.println(new DateTime(DateTimeZone.UTC));
	        set.add(new Plan(i));
	        Thread.sleep(200);
	    }
	    System.out.println("----------");
	    for (Plan p : set.values()) {
	        assert(p.getId()>=5);
	    }
    }

}
