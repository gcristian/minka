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
package io.tilt.minka.core.task;

import static io.tilt.minka.core.task.Semaphore.Action.ANY;
import static io.tilt.minka.core.task.Semaphore.Action.BOOTSTRAP;
import static io.tilt.minka.core.task.Semaphore.Action.DISTRIBUTOR;
import static io.tilt.minka.core.task.Semaphore.Action.HEARTBEAT_REPORT;
import static io.tilt.minka.core.task.Semaphore.Action.INSTRUCT_DELEGATE;
import static io.tilt.minka.core.task.Semaphore.Action.LEADERSHIP;
import static io.tilt.minka.core.task.Semaphore.Action.PARTITION_TABLE_UPDATE;
import static io.tilt.minka.core.task.Semaphore.Action.PROCTOR;
import static io.tilt.minka.core.task.Semaphore.Action.SHUTDOWN;
import static io.tilt.minka.core.task.Semaphore.Hierarchy.CHILD;
import static io.tilt.minka.core.task.Semaphore.Hierarchy.PARENT;
import static io.tilt.minka.core.task.Semaphore.Hierarchy.SIBLING;
import static java.util.Arrays.asList;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * A simple schema to define rules and control permissions on actions.
 * Allowing consistency on threaded actions over shared data, etc. 
 * Also giving a functional level of abstraction. 
 * 
 * @author Cristian Gonzalez
 * @since Nov 27, 2015
 *
 */
public interface Semaphore extends Service {

	/*
	 * Ask permission for an action and acquire it at the same time if granted
	 */
	Permission acquire(Action a);

	/* Idem but block caller's thread */
	Permission acquireBlocking(Action a);

	/* Release acquired permission for an action. */
	void release(Action a);

	/**
	 * Events may mean an action that revokes all other in taking place (GRANT)
	 * during the operation and in any distant future (DENY).
	 * Or simply a temporal denial while another finishes and turns on permissions (RETRY)  
	 */
	enum Permission {
		/* you may ask after in hope for a different answer */
		RETRY,
		/* a definitive valid forever answer */
		DENIED,
		/* a definitive valid answer for this time */
		GRANTED;
	}

	enum Scope {
		/* at the shard's threads level only */
		LOCAL,
		/* distributed at the minka ensemble */
		GLOBAL
	}

	/*
	 * describe hierarchy relations between actions, such will deny or allow
	 * those related in lower or upper level's action to run,
	 */
	enum Hierarchy {
		/*
		 * doesnt lock related actions TODO wait some time and interrupt them
		 * on expiration, then allow depender to run
		 */
		PARENT,
		/*
		 * allowed to run if related actions are unlocked then lock/released
		 * them altogether with depender or wait them to be released if locked
		 */
		SIBLING,
		/*
		 * doesnt lock anything, this allows the action to run when related is
		 * locked
		 */
		CHILD,;
	}

	/*
	 * an action's hierarchy relation to other actions later translated to
	 * tasks, permissions, retries, etc
	 */
	interface Rule {
		Action getAction();

		List<Action> getRelated(Hierarchy level);
	}

	/* a task requiring some level of orchestration */
	enum Action {
		/* mostly to control concurrency on this */
		BOOTSTRAP(Scope.LOCAL),
		/* to stop other services to keep running when this happens */
		SHUTDOWN(Scope.LOCAL), DISTRIBUTE_ENTITY(Scope.LOCAL), LEADERSHIP(Scope.GLOBAL),
		/*
		 * both to avoid inconsistent behaviour and to simplify synchronization
		 * of shared data both modify the partition table from different
		 * methods
		 */
		DISTRIBUTOR(Scope.LOCAL), PROCTOR(Scope.LOCAL),
		/*
		 * we cannot hope for the PartitionDelegate to be thread-safe this
		 * include Take, Release & Update: they mutually exclude-'emselves one
		 * another
		 */
		INSTRUCT_DELEGATE(Scope.LOCAL),
		/* shard's enforcement to take rights on an assigned duty */
		RESERVE_DUTY(Scope.GLOBAL),
		/*
		 * in order to avoid heartbeat intoxication with concurrent delegate
		 * instructions
		 */
		HEARTBEAT_REPORT(Scope.LOCAL),
		/* this event occurs as HBs come by without scheduling */
		PARTITION_TABLE_UPDATE(Scope.LOCAL),

		CLUSTER_COMPLETE_SHUTDOWN(Scope.GLOBAL), CLUSTER_COMPLETE_REBALANCE(Scope.GLOBAL),

		/*
		 * for hierarchy matters - an action that represent all others to the
		 * one compared
		 */
		ANY(Scope.LOCAL),

		/* ----- without permissions required ------- */

		DISCARD_OBSOLETE_CONNECTIONS(Scope.LOCAL), 
		FOLLOWER_POLICIES_CLEARANCE(Scope.LOCAL), 
		
		BROKER_INCOMING_MESSAGE(Scope.LOCAL), 
		BROKER_CLIENT_START(Scope.LOCAL), 
		BROKER_SERVER_START(Scope.LOCAL), 
		BOOTSTRAP_BULLETPROOF_START(Scope.LOCAL), 
		BOOTSTRAP_LEADERSHIP_CANDIDATURE(Scope.GLOBAL),

		;
		final Scope scope;

		public Scope getScope() {
			return this.scope;
		}

		Action(Scope type) {
			this.scope = type;
		}
	}

	public static RuleFactory builder(Action action) {
		return new RuleFactory(action);
	}

	public static class RuleFactory implements Rule {
		private final Action action;
		private final Map<Hierarchy, List<Action>> relatedByHierarchy;


		private RuleFactory(Action action) {
			this.relatedByHierarchy = new HashMap<>(Hierarchy.values().length);
			for (Hierarchy level : Hierarchy.values()) {
				relatedByHierarchy.put(level, new ArrayList<>());
			}
			this.action = action;
		}

		public RuleFactory add(final Hierarchy level, final List<Action> dependants) {
			relatedByHierarchy.get(level).addAll(dependants);
			return this;
		}

		@Override
		public List<Action> getRelated(final Hierarchy level) {
			return this.relatedByHierarchy.get(level);
		}

		@Override
		public Action getAction() {
			return action;
		}
	}

	/*
	 * Not all combinations are declared as Minka knows what situations are really expected
	 * So this's a neat declared coordination behaviour instead of scattering condition checks
	 * and let the action tasks ignore about a state context of sibling threads among the pool
	 *  
	 * relations are not declared in both sides, A Parent of B, implicitly means B Child to A
	 * with exception to ANY for which childhood doesnt apply unless explicitly declared so.
	 * 
	 * warning: beware with deadlock definitions !! (keep a tree locking hiearchy)
	 */
	default List<Rule> getLockingRules() {
		final List<Rule> rules = new ArrayList<>();

		/* these two are mutually excluded of running simultaneously */
		rules.add(builder(BOOTSTRAP)
				.add(PARENT, asList(ANY)));
		rules.add(builder(SHUTDOWN)
				.add(PARENT, asList(ANY)));

		// At Leader's
		rules.add(builder(LEADERSHIP)
				.add(CHILD, asList(Action.BOOTSTRAP)));
		rules.add(builder(PROCTOR)
				.add(SIBLING, asList(DISTRIBUTOR, PARTITION_TABLE_UPDATE)));
		rules.add(builder(DISTRIBUTOR)
				.add(SIBLING, asList(PROCTOR, PARTITION_TABLE_UPDATE)));
		rules.add(builder(PARTITION_TABLE_UPDATE)
				.add(SIBLING, asList(PROCTOR, DISTRIBUTOR)));

		// At Follower's
		rules.add(builder(INSTRUCT_DELEGATE)
				.add(SIBLING, asList(HEARTBEAT_REPORT)));
		rules.add(builder(HEARTBEAT_REPORT)
				.add(SIBLING, asList(INSTRUCT_DELEGATE)));
		

		return rules;
	}
}
