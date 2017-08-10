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

import org.apache.commons.lang.Validate;
import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

import io.tilt.minka.core.task.Scheduler;
import io.tilt.minka.core.task.Scheduler.PriorityLock;
import io.tilt.minka.core.task.Scheduler.Synchronized;
import io.tilt.minka.core.task.Scheduler.SynchronizedFactory;
import io.tilt.minka.core.task.Semaphore.Action;

public class SynchronizedFactoryImpl implements Synchronized, SynchronizedFactory {
	private final Action action;
	private final PriorityLock priority;
	private final Runnable task;
	private long lastExecutionTimestamp;
	private long lastSuccessfulExecutionTimestamp;
	private long lastSuccessfulExecutionLapse;

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
	}

	public Synchronized build(final Action action, final PriorityLock priority, final Runnable task) {
		return new SynchronizedFactoryImpl(action, priority, task);
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
		StringBuilder sb = new StringBuilder().append("Ag:").append(getAction())
				.append(",").append("T:").append(cname);
		return sb.toString();
	}

	@Override
	public void execute() {
		final long start = System.currentTimeMillis();
		this.lastExecutionTimestamp = start;
		try {
			task.run();
			this.lastSuccessfulExecutionTimestamp = start;
			this.lastSuccessfulExecutionLapse = System.currentTimeMillis() - start;
		} catch (Exception e) {
			Scheduler.logger.error("Untrapped exception running synchronized action", e);
			this.lastException = e;
		} finally {
			this.lastExecutionTimestamp = start;
		}
	}

	@Override
	public long getLastSuccessfulExecutionTimestamp() {
		return lastSuccessfulExecutionTimestamp;
	}

	@Override
	public long getLastSuccessfulExecutionLapse() {
		return this.lastSuccessfulExecutionLapse;
	}

	@Override
	public long getLastExecutionTimestamp() {
		return lastExecutionTimestamp;
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

	/*
	 * @Override public void cancel() { if (task!=null) { task.run(); }
	 * else if (callable!=null) { this.result = callable.call(); } }
	 */

}
