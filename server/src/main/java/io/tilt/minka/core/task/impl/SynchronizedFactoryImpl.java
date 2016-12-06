
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
		StringBuilder sb = new StringBuilder().append("A:").append(getAction()).append(",").append("T:")
			.append(getTask().getClass().getSimpleName());
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
