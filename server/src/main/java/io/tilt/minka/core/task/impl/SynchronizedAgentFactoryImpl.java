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

import java.util.concurrent.TimeUnit;

import org.apache.commons.lang.Validate;

import io.tilt.minka.core.task.Scheduler.Agent;
import io.tilt.minka.core.task.Scheduler.AgentFactory;
import io.tilt.minka.core.task.Scheduler.Frequency;
import io.tilt.minka.core.task.Scheduler.PriorityLock;
import io.tilt.minka.core.task.Semaphore.Action;

public class SynchronizedAgentFactoryImpl extends SynchronizedFactoryImpl implements Agent, AgentFactory {

	private long delay;
	private long periodicDelay;
	private final Frequency frequency;

	public SynchronizedAgentFactoryImpl() {
		super();
		this.frequency = null;		
	}
	
	protected SynchronizedAgentFactoryImpl(
			final Action action, 
			final PriorityLock priority, 
			final Frequency frequency,
			final Runnable task) {
		super(action, priority, task);
		Validate.notNull(frequency);
		this.frequency = frequency;
	}

	public AgentFactory create(
			final Action action, 
			final PriorityLock priority,
			final Frequency frequency, 
			final Runnable task) {
		return new SynchronizedAgentFactoryImpl(action, priority, frequency, task);
	}

	public AgentFactory every(long periodicDelay) {
		this.periodicDelay = periodicDelay;
		return this;
	}

	public AgentFactory delayed(long firstDelayMs) {
		this.delay = firstDelayMs;
		return this;
	}
	
	public Agent build() {
		return this;
	}

	@Override
	public long getPeriodicDelay() {
		return this.periodicDelay;
	}

	@Override
	public long getDelay() {
		return this.delay;
	}

	@Override
	public Frequency getFrequency() {
		return frequency;
	}

	@Override
	public String toString() {				
		StringBuilder sb = new StringBuilder()
				.append(super.toString())
				.append("(F: ").append(getFrequency())
				.append(",").append(getDelay())
				.append(",").append(getPeriodicDelay())
				.append(",P: ").append(getPriority()).append(")");
		return sb.toString();
	}

	@Override
	public TimeUnit getTimeUnit() {
		return TimeUnit.MILLISECONDS;
	}
}