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
package io.tilt.minka.core.task.impl;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.Validate;
import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.tilt.minka.core.task.Scheduler;
import io.tilt.minka.core.task.Scheduler.PriorityLock;
import io.tilt.minka.core.task.Scheduler.Synchronized;
import io.tilt.minka.core.task.Scheduler.SynchronizedFactory;
import io.tilt.minka.core.task.Semaphore.Action;

public class SynchronizedFactoryImpl implements Synchronized, SynchronizedFactory {

	private final Logger logger = LoggerFactory.getLogger(getClass());

	private final Action action;
	private final PriorityLock priority;
	private final Runnable task;

	private long creationTimestamp;
	private long lastTimestamp;
	private long lastSuccessfulTimestamp;
	private long lastSuccessfulDuration;
	private long accumulatedDuration;

	private int accumulatedWait;
	private int lastQueueWait;
	private long lastEnqueued;
	private long counter;

	private Exception lastException;

	public SynchronizedFactoryImpl() {
		this.action = null;
		this.priority = null;
		this.task = null;
	}

	protected SynchronizedFactoryImpl(final Action action, final PriorityLock priority, final Runnable task) {
		Validate.notNull(action);
		Validate.notNull(priority);
		Validate.isTrue(task != null);
		this.action = action;
		this.priority = priority;
		this.task = task;
		this.creationTimestamp = System.currentTimeMillis();
	}

	public Synchronized build(final Action action, final PriorityLock priority, final Runnable task) {
		return new SynchronizedFactoryImpl(action, priority, task);
	}

	@Override
	public void execute() {
		final long start = System.currentTimeMillis();
		lastQueueWait = (int) (start - lastEnqueued);
		accumulatedWait += lastQueueWait;
		try {
			counter++;
			task.run();
			lastSuccessfulTimestamp = start;
			lastSuccessfulDuration = System.currentTimeMillis() - start;

			if (logger.isInfoEnabled()) {
				log(start);
			}
		} catch (Exception e) {
			Scheduler.logger.error("Untrapped exception on Task: {}", action.name(), e);
			this.lastException = e;
		} finally {
			this.lastTimestamp = start;
			accumulatedDuration += lastSuccessfulDuration;
		}
	}

	private void log(final long start) {
		boolean frequent = false;
		if (this.getClass().equals(SynchronizedAgentFactoryImpl.class)) {
			SynchronizedAgentFactoryImpl x = (SynchronizedAgentFactoryImpl) this;
			frequent = x.getFrequency() == Scheduler.Frequency.PERIODIC;
		}
		long waitedBorn = start - creationTimestamp;
		String name = task.toString();
		name = name.substring(name.lastIndexOf('.'));
		final long lastRunDiff = start - lastTimestamp;
		logger.info("Task: t: {} {} acc.w: {} lq.w: {} [#{}] {} {}", lastSuccessfulDuration, 
				frequent ? "lrd: " + lastRunDiff : "w.b: " + waitedBorn, 
				accumulatedWait, lastQueueWait, counter,
				StringUtils.substring(action.name(), 0, 15),
				name);
	}

	@Override
	public Action getAction() {
		return action;
	}

	@Override
	public PriorityLock getPriority() {
		return priority;
	}

	@Override
	public int hashCode() {
		return new HashCodeBuilder().append(getAction()).toHashCode();
	}

	@Override
	public boolean equals(Object obj) {
		if (obj != null && obj instanceof Synchronized) {
			Synchronized ag = (Synchronized) obj;
			return new EqualsBuilder().append(ag.getAction(), getAction()).isEquals();
		} else {
			return false;
		}
	}

	@Override
	public String toString() {
		String cname = getTask().getClass().getSimpleName();
		final int p = cname.indexOf('$');
		cname = p > 0 ? cname.substring(p) : cname;
		StringBuilder sb = new StringBuilder().append("Ag:")
				.append(getAction())
				.append(",")
				.append("T:")
				.append(cname);
		return sb.toString();
	}

	@Override
	public int getLastQueueWait() {
		return lastQueueWait;
	}

	@Override
	public int getAccumulatedWait() {
		return accumulatedWait;
	}

	@Override
	public long getAccumulatedDuration() {
		return accumulatedDuration;
	}

	@Override
	public long getCounter() {
		return counter;
	}

	@Override
	public long getLastSuccessfulTimestamp() {
		return lastSuccessfulTimestamp;
	}

	@Override
	public long getLastSuccessfulDuration() {
		return this.lastSuccessfulDuration;
	}

	@Override
	public long getLastTimestamp() {
		return lastTimestamp;
	}

	@Override
	public Exception getLastException() {
		return this.lastException;
	}

	@Override
	public Runnable getTask() {
		// TODO Auto-generated method stub
		return task;
	}

	@Override
	public void flagEnqueued() {
		lastEnqueued = System.currentTimeMillis();
	}

	/*
	 * @Override public void cancel() { if (task!=null) { task.run(); } else if
	 * (callable!=null) { this.result = callable.call(); } }
	 */

}
