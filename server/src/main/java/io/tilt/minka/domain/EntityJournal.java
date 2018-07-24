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


import static java.util.Collections.unmodifiableList;
import static java.util.Objects.requireNonNull;

import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import com.fasterxml.jackson.annotation.JsonIgnore;

import io.tilt.minka.api.config.DistributorSettings;
import io.tilt.minka.domain.EntityJournal.Log.StateStamp;
import io.tilt.minka.shard.Shard;
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
	
	protected static final long serialVersionUID = 1L;
	
	
	final static SimpleDateFormat sdf = new SimpleDateFormat("yyMMdd:hhmmss.SSS");
	@JsonIgnore
	private boolean sliding;

	@JsonIgnore
	private final LinkedList<Log> logs = new LinkedList<>();

	public final static String NOT_APPLIABLE = "N/A";
	
	/**
	 * Add a new state to an existing key: event + shard + plan.
	 * Create a new Log if the key not exists already.
	 * 
	 * @param event		create, attach, dettach, remove
	 * @param state		pending, confirmed
	 * @param shardid	shard1, shard2
	 * @param planid	version of plan
	 */
	public void addEvent(
		final EntityEvent event, 
		final EntityState state, 
		final ShardIdentifier shardid, 
		final long planid) {

		requireNonNull(event);
		requireNonNull(state);
		
		// look up the right log
		final String shid = shardid != null ? shardid.getId() : null;
		Log log = (planid > 0) ? findFirst(planid, shid, event) : null;
		if (log == null) {
			logs.add(log = new Log(new Date(), event, shid == null ? NOT_APPLIABLE : shid, planid));
			if (sliding || (sliding = logs.size() == DistributorSettings.MAX_JOURNAL_SIZE)) {
				logs.removeFirst();
			}
		}
		log.addState(state);
	}
	
	public int eventSize() {
		return this.logs == null ? 0 : this.logs.size();
	}
	
	public Log getLast() {
		return this.logs.getLast();
	}
	
	public Log getFirst() {
		return this.logs.getFirst();
	}
	
	@JsonIgnore
	/** @return an unmodifiable list */
	public List<Log> getLogs() {
		return unmodifiableList(logs);
	}

	public Iterator<Log> descendingIterator() {
		return logs.descendingIterator();
	}

	public List<Log> getLogs(final long planId) {
        return unmodifiableList(logs.stream()
        	.filter(et->et.getPlanId()==planId)
        	.collect(Collectors.toList()));
    }

	public boolean hasEverBeenDistributed() {
	    for (final Log log: logs) {
	        if (log.getEvent()==EntityEvent.ATTACH) {
	            for (final StateStamp ds: log.getStates()) {
	                if (ds.getState()==EntityState.COMMITED) {
	                    return true;
	                }
	            }
	        }
	    }
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
		final Collection<Log> r = find_(planid, shardid, events);
		if (r!=null && !r.isEmpty()) {
			return r.iterator().next();
		}
		return null;
	}
	
	/**
	 * @return all events of the last plan and given @param shardid
	 */
	public Collection<Log> findAll(final ShardIdentifier shardid) {
		return find_(0, shardid.getId(), EntityEvent.values());
	}
	public Collection<Log> findAll(final long planid, final ShardIdentifier shardid, final EntityEvent...events) {
		return find_(planid, shardid.getId(), events);
	}
	
	/** 
	 * Descends in time (logs are stacked) until an older plan than current is reached. 
	 * @return a Log matching plan + event + shard, whereas those args. specified or any if omitted 
	 */
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
		return ret;
	}

	@JsonIgnore
	protected List<String> getStringHistory() {
		final Map<StateStamp, String> ordered = new TreeMap<>(new StateStamp.DateComparer());
		for (Log el : logs) {
			for (final StateStamp ts : el.getStates()) {
				ordered.put(ts, String.format("%s %s: %s (%s) at %s",
								el.getPlanId(),
								el.getEvent(),
								el.getTargetId(),
								ts.getState(),
								sdf.format(ts.getDate())));
			}
		}
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
	
	

}