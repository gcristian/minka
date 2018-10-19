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


import static java.util.Objects.requireNonNull;

import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

import com.fasterxml.jackson.annotation.JsonIgnore;

import io.tilt.minka.domain.CommitTree.Log.StateStamp;
import io.tilt.minka.shard.Shard;
import io.tilt.minka.shard.ShardIdentifier;

/**
 * Master CRUD operations and state registry for entities. Living within {@linkplain ShardEntity} instances.
 * Facility methods for a single point of access for {@linkplain Log} instances.
 *  
 * Deliveries are organized so events occurr in this order:
 * 
 * "plan-id:N": {
 *		"shard-id:X": {
 * 			"event-K": {
 * 				"prepared": $date,
 *	 			"pending": $date,
 * 				"ack": $date,
 *	 			"committed": $date
 * 			}	
 *  	},
 * }
 *  
 *  @author Cristian Gonzalez
 *  @since  Jun 5, 2018
*/
public class CommitTree implements Serializable {

	final static SimpleDateFormat sdf = new SimpleDateFormat("yyMMdd:hhmmss.SSS");
	@JsonIgnore
	private boolean sliding;
		
	private static final long serialVersionUID = 9044973294863922841L;

	// a plan is intended but is not known in the context used
	public static final int PLAN_UNKNOWN = -1;
	// an action out of Factory does not apply for PID (creation, deletion, etc)
	public static final int PLAN_NA = 0; 
	// wildcard to get the last plan created
	public static final int PLAN_LAST = -2;

	static final int MAX_PLAN_HISTORY = 99;
	static final int MAX_EVENTS = EntityEvent.values().length;
	static final int MAX_SHARD_HISTORY = 99;
	static final String SHARD_NOID = "noid";
	
	@JsonIgnore		
	private final 
		LimMap<Long, // plan ids order from older (head) to newest (tail)
			InsMap<String,  // shard ids insertion ordered (planned events)
				LimMap<EntityEvent, Log>>> 	// events sorted by consistent order
					tree = new LimMap<>(
							MAX_PLAN_HISTORY, 
							(Comparator<Long> & Serializable) Long::compare);


	void addEvent_(
			final EntityEvent event, 
			final EntityState state, 
			final String shardid, 
			final long planid) {
					
		long pid = 0;
		if (planid > 0) {
			pid = planid;
		} else if (tree.isEmpty()) {
			pid = 0;
		} else {
			pid = tree.lastKey();
		}
		tree.getOrPut(
				pid, 
				()-> new InsMap<String, LimMap<EntityEvent, Log>>(MAX_SHARD_HISTORY))
			.getOrPut(
				shardid, 
				()-> new LimMap<EntityEvent, Log>(
					MAX_EVENTS,
					(Comparator<EntityEvent> & Serializable)
					(a, b)-> Integer.compare(a.getOrder(), b.getOrder())))
			.getOrPut(
					event, 
					()-> new Log(new Date(), event, shardid, planid))
			.addState(state);
	}
	
	<I,O> O onLogs(final Function<Log, O> fnc) {
		O o = null;
		if (!tree.isEmpty()) {
			for (InsMap<String, LimMap<EntityEvent, Log>> byPlan: tree.values()) {
				for (LimMap<EntityEvent, Log> byShard: byPlan.values()) {
					for (final Log byEvent: byShard.values()) {
						o = fnc.apply(byEvent);
						if (o!=null) {
							return o;
						}
					}
				}
			}
		}
		return o;
	}
	
	Log getCreation() {
		if (!tree.isEmpty()) {
			return tree.firstEntry().getValue()
					.getFirst().entrySet().iterator().next()
					.getValue();
		}
		return null;
	}

	Log find_(
			final long planid, 
			final String shardid, 
			final Consumer<Log> c, 
			final EntityEvent...events) { 
		
		//System.out.println(planid);
		final InsMap<String, LimMap<EntityEvent, Log>> shards = 
				planid == PLAN_LAST ? tree.lastEntry().getValue() : tree.get(planid);
		if (shards!=null) {
			final LimMap<EntityEvent, Log> logs = shardid == null ? 
					shards.getLast() : shards.get(shardid);
			if (logs!=null) {
				if (events!=null && events.length>0) {
					for (EntityEvent ee: events) {
						final Log x = logs.get(ee);
						if (x!=null) {
							if (c!=null) {
								c.accept(x);
							} else {
								return x;
							}
						}
					}
				} else {
					final Log last = logs.lastEntry().getValue();
					if (last!=null) {
						if (c!=null) {
							c.accept(last);
						} else {
							return last;
						}
					}
				}
			}
		}
		return null;
	}
	
	public Date getCreationTimestamp() {
		return getCreation().getHead();
	}
	
	Log findWithLimitForEvent(
			final long timestampLimit,
			final Consumer<Log> c, 
			final EntityEvent...events) { 
		Log ret = null;
		for (Long pid: tree.descendingKeySet()) {
			if (pid>=timestampLimit) {
				for (LimMap<EntityEvent, Log> m: tree.get(pid).values()) {
					for (EntityEvent ee: events) {
						final Log l = m.get(ee);
						if (l!=null) {
							if (c!=null) {
								c.accept(l);
							} else {
								ret = l;
							}
						}
					}
				}
			} else {
				break;
			}
		}
		return ret;
	}

	public boolean hasUnifinishedEvent(
			int maxPlansBack,
			final EntityEvent ee) { 
		boolean isLegitUnpair = false;
		for (Long pid: tree.descendingKeySet()) {
			if (maxPlansBack-- < 0) {
				break;
			}
			for (LimMap<EntityEvent, Log> m: tree.get(pid).values()) {
				final Log l = m.get(ee);
				if (l!=null) {
					if (isLegitUnpair = (
							// any suspicious state
							l.getLastState()==EntityState.PREPARED
							|| l.getLastState()==EntityState.MISSING
							|| l.getLastState()==EntityState.PENDING 
							|| l.getLastState()==EntityState.DANGLING)) {
						break;
					}
				}
			}
		}
		return isLegitUnpair;
	}

	public Log getPreviousLog(final String shardid) {
		if (!tree.isEmpty()) {
			final LimMap<EntityEvent, Log> sh = tree.lastEntry().getValue().get(shardid);
			if (sh!=null) {
				final Iterator<EntityEvent> it = sh.descendingKeySet().iterator();
				if (it.hasNext()) {
					it.next();
				}
				if (it.hasNext()) {
					return sh.get(it.next());
				}
			}
		}
		return null;
	}
	
	Log getFirstLog() {
		if (tree.firstEntry()!=null) {
			final LimMap<EntityEvent, Log> first = tree.firstEntry().getValue().getFirst();
			if (first!=null) {
				if (first.firstEntry()!=null) {
					return first.firstEntry().getValue();
				}
			}
		}
		return null;
	}

	public void addEvent(
			final EntityEvent event, 
			final EntityState state, 
			final ShardIdentifier shardid, 
			final long planid) {
		addEvent_(requireNonNull(event), requireNonNull(state), requireNonNull(shardid.getId()), planid);
	}
	
	public void addEvent(
			final EntityEvent event, 
			final EntityState state, 
			final String shardid, 
			final long planid) {
		addEvent_(requireNonNull(event), requireNonNull(state), requireNonNull(shardid), planid);
	}

	public Log getLast() {
		return find_(PLAN_LAST, null, null, null);		
	}
	
	public Log getFirst() {
		return getFirstLog();
	}
	
	public List<Log> filterLogs(final long pid, final String shardid, final EntityEvent ee) {
		final List<Log> r = new LinkedList<>();
		find_(pid, shardid, r::add, ee);
		return r;
	}

	public List<Log> getLogs(final long planId) {
        final List<Log> r = new LinkedList<>();
        find_(planId, null, r::add, (EntityEvent[])null);
        return r;
    }
	
	/** 
	 * if a log with a given state exists matching shard and event 
	 * and there is no other later than found one: in a diferent shard for the same event,
	 * Nor has the passed shard a commit with a negative event on a later plan
	 * 
	 * @return TRUE if the shard has a Durable state;
	 */
	public boolean isDurable(final String shardid, EntityEvent ee, EntityState es) {
		long lastPlan = -1;
		long lastNegativePlan = -1;
		for (Map.Entry<Long, InsMap<String , LimMap<EntityEvent, Log>>> byPlan: tree.entrySet()) {
			// make use of the INSertion order map
			for (Map.Entry<String, LimMap<EntityEvent, Log>> e: byPlan.getValue().entrySet()) {
				final Log log = e.getValue().get(ee);
				if (log!=null 
						&& log.getLastState()==es 
						&& e.getKey().equals(shardid)) {
					lastPlan = byPlan.getKey();
				} else {
					final Log inverse = e.getValue().get(ee.toNegative());
					if (inverse!=null 
							&& inverse.getLastState()==es
							&& e.getKey().equals(shardid)) {
						lastNegativePlan = byPlan.getKey();
					}
				}
			}
		}
		return lastPlan > lastNegativePlan;
	}

	public boolean hasEverBeenDistributed() {
	    onLogs(log-> {
	        if (log.getEvent()==EntityEvent.ATTACH) {
	            for (final StateStamp ds: log.getStates()) {
	                if (ds.getState()==EntityState.COMMITED) {
	                    return true;
	                }
	            }
	        }
	        return null;
	    });
	    return false;
	}

	/** @return a Log representing the event occurred after the minTimestamp arg. */
	public Log existsWithLimit(final EntityEvent ee, final long minTimestamp) {
		return findWithLimitForEvent(minTimestamp, null, ee);
	}
	public void filterWithLimit(final EntityEvent ee, final long minTimestamp, final Consumer<Log> c) {
		findWithLimitForEvent(minTimestamp, c, ee);
	}

	/** 
	 * @return reading from latest to earliest a Log that matches given:
	 * plan version, target shard and specific events, or any if null
	 **/
	public Log findOne(final long planid, final ShardIdentifier shardid, final EntityEvent...events) {
		return findOne(planid, shardid.getId(), events);
	}
	
	private Log findOne(final long planid, final String shardid, final EntityEvent...events) {
		return find_(planid, shardid, null, events);
	}
	
	/**
	 * @return all events of the last plan and given @param shardid
	 */
	public Collection<Log> findAll(final ShardIdentifier shardid) {
		final List<Log> r = new LinkedList<>();
		find_(PLAN_LAST, shardid.getId(), r::add, EntityEvent.values());
		return r;
	}
	public Collection<Log> findAll(final long planid, final ShardIdentifier shardid, final EntityEvent...events) {
		final List<Log> r = new LinkedList<>();
		find_(planid, shardid.getId(), r::add, events);
		return r;
	}
		@JsonIgnore
	protected List<String> getStringHistory() {
		
		
		final Map<StateStamp, String> ordered = new TreeMap<>(new StateStamp.DateComparer());
		onLogs(el-> {
			for (final StateStamp ts : el.getStates()) {
				ordered.put(ts, String.format("%s %s: %s (%s) at %s",
								el.getPlanId(),
								el.getEvent(),
								el.getTargetId(),
								ts.getState(),
								sdf.format(ts.getDate())));
			}
			return null;
		});
		return new ArrayList<>(ordered.values());
	}
	
	/**
	 * Registry of an {@linkplain EntityEvent} with a list of {@linkplain StateStamp}, 
	 * for a given conmposed key of {@linkplain ShardEntity}, {@linkplain Plan}, {@linkplain Shard}
	 * Recording any distribution related operation information.
	 */
	public static class Log implements Serializable {

		private static final long serialVersionUID = -8873965041941783628L;

		private final Date head;
		private final EntityEvent event;
		private final LinkedList<StateStamp> states = new LinkedList<>();
		private final String targetId;
		private final long planId;

		private Log(final Date head, final EntityEvent event, final String targetId, final long planId) {
			super();
			this.head = head;
			this.event = event;
			this.targetId = targetId;
			this.planId = planId;
		}

		public Date getHead() {
			return this.head;
		}

		public EntityEvent getEvent() {
			return this.event;
		}

		public String getTargetId() {
			return this.targetId;
		}

		public long getPlanId() {
			return this.planId;
		}

		// ---------------------------------------------------------------------------------------------------

		public List<StateStamp> getStates() {
			return Collections.unmodifiableList(states);
		}

		public EntityState getLastState() {
			return this.states.getLast().getState();
		}

		public boolean matches(final Log log) {
			return matches(log.getEvent(), log.getTargetId(), log.getPlanId());
		}

		/**
		 * @return whether passed log belongs to the same shard, version plan and type of event
		 */
		public boolean matches(final EntityEvent event, final String targetId, final long planId) {
			return event == this.event && targetId.equals(this.targetId) && planId == this.planId;
		}

		public void addState(final EntityState state) {
			this.states.add(new StateStamp(new Date(), state));
		}

		@Override
		public String toString() {
			final StringBuilder sb = new StringBuilder(15 + 3 + 13 + 15 + 25)
					.append("p:").append(planId).append(' ')
					.append("dt:").append(sdf.format(head)).append(' ')
					.append("ev:").append(event).append(' ')
					.append("sh:").append(targetId).append(' ')

			;
			return sb.toString();
		}

		@Override
		public int hashCode() {
			final int prime = 31;
			int res = 1;
			res *= prime + ((head == null ) ? 0 : head.hashCode());
			res *= prime + ((targetId == null ) ? 0 : targetId.hashCode());
			res *= prime + planId;
			res *= prime + ((event == null ) ? 0 : event.hashCode());			
			return res;
		}
		@Override
		public boolean equals(final Object obj) {
			if (obj != null && obj!=this && obj instanceof Log) {
				return false;
			} else if (obj == this) {
				return true;
			} else {
				final Log o = (Log)obj;
				return head.equals(o.getHead())
						&& targetId.equals(o.getTargetId())
						&& planId==o.getPlanId()
						&& event==o.getEvent();
			}
		}
	    
		/**
		 * A recording timestamp for a reached state 
		 */
		public static class StateStamp implements Serializable {
			private static final long serialVersionUID = -3611519717574368897L;

			public static class DateComparer implements Comparator<StateStamp>, Serializable {
				private static final long serialVersionUID = 3709876521530551544L;

				@Override
				public int compare(final StateStamp o1, final StateStamp o2) {
					if (o1 == null || o2 == null) {
						return o1 == null && o2 != null ? -1 : o2 == null ? 0 : 1;
					} else {
						return o1.getDate().compareTo(o2.getDate());
					}
				}
			}

			private final Date date;
			private final EntityState state;

			public StateStamp(final Date date, final EntityState state) {
				super();
				this.date = date;
				this.state = state;
			}

			public Date getDate() {
				return this.date;
			}

			public EntityState getState() {
				return this.state;
			}

			@Override
			public int hashCode() {
				final int prime = 31;
				int res = 1;
				res *= prime + ((date == null ) ? 0 : date.hashCode());
				res *= prime + ((state== null ) ? 0 : state.hashCode());
				return res;
			}

			@Override
			public String toString() {
				return new StringBuilder(8 + 17 + 10)
						.append("dt:").append(date).append(' ')
						.append("st:").append(state).toString();
			}

			@Override
			public boolean equals(final Object obj) {
				if (obj == null || !(obj instanceof StateStamp)) {
					return false;
				} else if (obj == this) {
					return true;
				} else {
					final StateStamp ts = (StateStamp) obj;
					return ts.getDate().equals(date) && ts.getState().equals(state);
				}
			}
		}

	}

	

	interface Common<K, V> extends Map<K, V> {
		default V getOrPut(final K key, final Supplier<V> defaultValue) {
			V v = get(key);
			if (v==null) {
				put(key, v = defaultValue.get());
			}
			return v;
		}

	}

	/** a limited map */
	public static class LimMap<K, V> extends TreeMap<K, V> implements Common<K, V>, Serializable {
		private static final long serialVersionUID = 9044973294863922841L;
		private final int limit;
		
		@Override
		public V put(final K key, final V value) {
			while (size() > limit) { 
				remove(firstKey());
			}
			return super.put(key, value);
		}
		
		public LimMap(final int limit, final Comparator<K> c) {
			super(c);
			this.limit = limit;
		}
	}
	
	/** a limited and insertion order map */
	public static class InsMap<K, V> extends LinkedHashMap<K, V> implements Common<K, V>, Serializable {
		
		private static final long serialVersionUID = -315653595943790784L;
		private final int limit;
		
		@Override
		public V put(final K key, final V value) {
			// TODO delete keys if needed
			return super.put(key, value);
		}
		
		InsMap(final int limit) {
			super();
			this.limit = limit;
		}
		
		V getLast() {
			final Iterator<java.util.Map.Entry<K, V>> it = entrySet().iterator();
			V last = null;
			while (it.hasNext()){
				last = it.next().getValue();
			}
			return last;
		}
		
		V getFirst() {
			final Iterator<java.util.Map.Entry<K, V>> it = entrySet().iterator();
			if (it.hasNext()){
				return it.next().getValue();
			}
			return null;
		}
		
	}

	public LimMap<Long, InsMap<String, LimMap<EntityEvent, Log>>> getInnerMap() {
		return tree;
	}

}