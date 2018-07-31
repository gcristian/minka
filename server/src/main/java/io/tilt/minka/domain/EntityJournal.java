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

import io.tilt.minka.domain.EntityJournal.Log.StateStamp;
import io.tilt.minka.shard.ShardIdentifier;

/**
 * Operation and state registry for entities. Living within {@linkplain ShardEntity} instances.
 * Facility methods for a single point of access for {@linkplain Log} instances.
 *  
 * Deliveries are organized so events occurr in this order:
 * 
 * 	 event		state		shard	plan id
 * 	 =====		=========	=====	=======
 * 
 * log1:  
 *   DETTACH 	PREPARED	9000	 1
 *   DETTACH 	PENDING		9000	 1
 *   DETTACH 	COMMITED	9000	 1
 *   
 * log2:
 * 	 ATTACH 	PREPARED	9001	 1
 *   ATTACH 	PENDING		9001	 1
 *   ATACH 		COMMITED	9001	 1
 *  
 *  @author Cristian Gonzalez
 *  @since  Oct 15, 2017
*/
public class EntityJournal implements Serializable {
	
	private static final long serialVersionUID = -3304164448469652337L;
	
	final static SimpleDateFormat sdf = new SimpleDateFormat("yyMMdd:hhmmss.SSS");
	@JsonIgnore
	private boolean sliding;

	@JsonIgnore
	//private final LinkedList<Log> logs = new LinkedList<>();
	private final CommitTree tree = new CommitTree();

	public static class CommitTree implements Serializable {
		
		private static final long serialVersionUID = 9044973294863922841L;
		
		static final int MAX_PLANS = 99;
		static final int MAX_EVENTS = EntityEvent.values().length;
		static final int MAX_SHARDS = 99;
		static final String SHARD_NOID = "noid";
		
		private final 
			LimMap<Long, // plan ids order from older (head) to newest (tail)
				InsMap<String,  // shard ids unordered
					LimMap<EntityEvent, Log>>> 	// events orderd by apparition from older (head) to newest (tail) 
						eventsByPlan = new LimMap<>(
								MAX_PLANS, 
								(Comparator<Long> & Serializable) Long::compare);
		
		void addEvent(
				final EntityEvent event, 
				final EntityState state, 
				final String shardid, 
				final long planid) {
							
			eventsByPlan.getOrPut(
				planid > 0 ? planid : eventsByPlan.isEmpty() ? 0 : eventsByPlan.lastKey(), 
				()-> new InsMap<String, LimMap<EntityEvent, Log>>(MAX_SHARDS)
				).getOrPut(
					shardid, 
					()-> new LimMap<EntityEvent, Log>(
						MAX_EVENTS,
						(Comparator<EntityEvent> & Serializable)
						(a, b)-> Integer.compare(a.getOrder(), b.getOrder())
					)).getOrPut(
						event, 
						()-> new Log(new Date(), event, shardid, planid))
					.addState(state);
		}
		
		<I,O> O onLogs(final Function<Log, O> fnc) {
			O o = null;
			if (!eventsByPlan.isEmpty()) {
				for (InsMap<String, LimMap<EntityEvent, Log>> byPlan: eventsByPlan.values()) {
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

		Log find_(
				final long planid, 
				final String shardid, 
				final Consumer<Log> c, 
				final EntityEvent...events) {
			
			final InsMap<String, LimMap<EntityEvent, Log>> shards = 
					planid == 0 ? eventsByPlan.lastEntry().getValue() : eventsByPlan.get(planid);
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
		

		Log getPreviousLog(final String shardid) {
			if (!eventsByPlan.isEmpty()) {
				final LimMap<EntityEvent, Log> sh = eventsByPlan.lastEntry().getValue().get(shardid);
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
			if (eventsByPlan.firstEntry()!=null) {
				final LimMap<EntityEvent, Log> first = eventsByPlan.firstEntry().getValue().getFirst();
				if (first!=null) {
					if (first.firstEntry()!=null) {
						return first.firstEntry().getValue();
					}
				}
			}
			return null;
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
	static class LimMap<K, V> extends TreeMap<K, V> implements Common<K, V>, Serializable {
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
	static class InsMap<K, V> extends LinkedHashMap<K, V> implements Common<K, V>, Serializable {
		
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

	public CommitTree getTree() {
		return tree;
	}


	public void addEvent(
			final EntityEvent event, 
			final EntityState state, 
			final ShardIdentifier shardid, 
			final long planid) {
		tree.addEvent(requireNonNull(event), requireNonNull(state), requireNonNull(shardid.getId()), planid);
	}
	
	public void addEvent(
			final EntityEvent event, 
			final EntityState state, 
			final String shardid, 
			final long planid) {
		tree.addEvent(requireNonNull(event), requireNonNull(state), requireNonNull(shardid), planid);
	}

	public Log getLast() {
		return tree.find_(0, null, null, null);		
		//return tree2.findLast();
	}
	
	public Log getFirst() {
		return tree.getFirstLog();
	}
	
	@JsonIgnore
	/** @return an unmodifiable list */
	/*
	public List<Log> getLogs() {
		return unmodifiableList(logs);
	}
	*/
	
	public List<Log> filterLogs(final long pid, final String shardid, final EntityEvent ee) {
		final List<Log> r = new LinkedList<>();
		tree.find_(pid, shardid, r::add, ee);
		return r;
	}
	
	public Log getPreviousLog(final String shardid) {
		return tree.getPreviousLog(shardid);
	}

	public List<Log> getLogs(final long planId) {
        final List<Log> r = new LinkedList<>();
        tree.find_(planId, null, r::add, (EntityEvent[])null);
        return r;
    }

	public boolean hasEverBeenDistributed() {
	    tree.onLogs(log-> {
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

	/** 
	 * @return reading from latest to earliest a Log that matches given:
	 * plan version, target shard and specific events, or any if null
	 **/
	public Log findFirst(final long planid, final ShardIdentifier shardid, final EntityEvent...events) {
		return findFirst(planid, shardid.getId(), events);
	}
	
	private Log findFirst(final long planid, final String shardid, final EntityEvent...events) {
		return tree.find_(planid, shardid, null, events);
	}
	
	/**
	 * @return all events of the last plan and given @param shardid
	 */
	public Collection<Log> findAll(final ShardIdentifier shardid) {
		final List<Log> r = new LinkedList<>();
		tree.find_(0, shardid.getId(), r::add, EntityEvent.values());
		return r;
	}
	public Collection<Log> findAll(final long planid, final ShardIdentifier shardid, final EntityEvent...events) {
		final List<Log> r = new LinkedList<>();
		tree.find_(planid, shardid.getId(), r::add, events);
		return r;
	}
	
	/** 
	 * Descends in time (logs are stacked) until an older plan than current is reached. 
	 * @return a Log matching plan + event + shard, whereas those args. specified or any if omitted 
	 */
	/*
	private Collection<Log> find_(
			// 0: only the last one
			final long planid, 
			final String shardid, 
			// if not provided then first apparition is valid and returned
			final EntityEvent...events) {
		
		Collection<Log> ret = null;
		long lastpid = -1;
		for (final Iterator<Log> it = logs.descendingIterator(); it.hasNext();) {
			final Log log = it.next();
			// stop when no pid is provided (0) and decended more than one plan back 
			if (planid == 0 && lastpid>-1 && lastpid !=log.getPlanId()) {
				break;
			}
			lastpid = log.getPlanId();
			if ((planid == 0 || log.getPlanId() == planid) 
					&& (shardid == null || log.getTargetId().equals(shardid))) {
				for (EntityEvent ee : events) {
					if (log.getEvent() == ee) {
						if (ret==null) {
							ret = new LinkedList<>();
						}
						ret.add(log);
					}
				}
				break;
			} else if (planid > log.getPlanId()) {
				// requested older than read: avoid phantom events
				break;
			}
		}
		return ret == null ? Collections.emptyList() : ret;
	}
*/
	
	
	@JsonIgnore
	protected List<String> getStringHistory() {
		
		
		final Map<StateStamp, String> ordered = new TreeMap<>(new StateStamp.DateComparer());
		tree.onLogs(el-> {
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
	
	
	/*
	
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

	

}