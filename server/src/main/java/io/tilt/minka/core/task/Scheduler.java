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

import java.util.Map;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * A simple agenda to coordinate tasks of Minka
 * Every action being taken by Minka is an Agent that runs in context of a Schedule
 * deliberately sorted to allow a clean read+write of shared data
 * 
 * @author Cristian Gonzalez
 * @since Nov 27, 2015
 *
 */
public interface Scheduler extends Semaphore {

	/* basic timed unit of work for a thread pool */
	/* for better traceability, isolation, metrics, data output exposure */
	public interface TimedTask {
		Action getAction();

		void execute();

		Runnable getTask();

		//void cancel();
		long getLastTimestamp();

		long getLastSuccessfulTimestamp();

		long getLastSuccessfulDuration();

		Exception getLastException();

		default <T> T getResult() {
			return null;
		}
	}

	/* the way this task will be trated by the scheduler at the semaphore */
	public enum PriorityLock {
		/* no locks will be acquired to run this */
		HIGH_ISOLATED,
		/* high priority blocks the caller thread until permission is given */
		MEDIUM_BLOCKING,
		/* low priority will run only if permission is immediately granted */
		LOW_ON_PERMISSION,

		HIGH_DISABLING_SLAVES, HIGH_ASKING_MASTER,;
	}

	/* so coordination internals dont spread along the codebase */
	SynchronizedFactory getFactory();

	AgentFactory getAgentFactory();

	/* run this in the caller's thread */
	void run(Synchronized synchro);

	/* schedule this to run in the pool */
	void schedule(Agent agent);

	/* stop the agent of running */
	boolean stop(Synchronized agent);

	/* forward to execute it now, leaving future schedules intact */
	void forward(Agent agent);

	/* query */
	Agent get(Action action);

	Map<Synchronized, ScheduledFuture<?>> getFutures();

	ScheduledThreadPoolExecutor getExecutor();

	Map<Action, Agent> getAgents();

	/* a task that needs synchronization with other tasks */
	public interface Synchronized extends TimedTask {
		PriorityLock getPriority();
		void flagEnqueued();
		int getLastQueueWait();
		int getAccumulatedWait();
		long getAccumulatedDuration();
		long getCounter();
	}

	public interface SynchronizedFactory {
		Synchronized build(Action action, PriorityLock priority, Runnable task);
	}

	/* for agents only */
	public enum Frequency {
		ONCE, 
		ONCE_DELAYED, 
		PERIODIC,
	}

	/* a repetitive timed task */
	public interface Agent extends Synchronized {
		Frequency getFrequency();
		long getDelay();
		TimeUnit getTimeUnit();
		long getPeriodicDelay();
	}

	public interface AgentFactory {
		AgentFactory create(Action action, PriorityLock priority, Frequency frequency, Runnable task);
		AgentFactory every(long periodicDelay);
		AgentFactory delayed(long firstDelayMs);
		Agent build();
	}

	

}
