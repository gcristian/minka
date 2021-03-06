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

import static io.tilt.minka.core.task.Semaphore.Permission.GRANTED;
import static io.tilt.minka.core.task.Semaphore.Permission.RETRY;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;

import org.apache.commons.lang.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

import io.tilt.minka.api.Config;
import io.tilt.minka.api.config.SchedulerSettings;
import io.tilt.minka.core.task.Scheduler;
import io.tilt.minka.shard.ShardIdentifier;

/**
 * @author Cristian Gonzalez
 * @since Dec 28, 2015
 */
public class SchedulerImpl extends SemaphoreImpl implements Scheduler {

	private final Logger logger = LoggerFactory.getLogger(getClass());

	/* for scheduling and stopping tasks */
	private final Map<Synchronized, ScheduledFuture<?>> futuresBySynchro;
	private final Map<Synchronized, Runnable> runnablesBySynchro;
	private final Map<Synchronized, Callable<?>> callablesBySynchro;
	private final Map<Action, Agent> agentsByAction;
	private final AgentFactory agentFactory;
	private final SynchronizedFactory syncFactory;
	private final String logName;

	private ScheduledThreadPoolExecutor executor;

	public SchedulerImpl(
			final Config config, 
			final SpectatorSupplier supplier, 
			final ShardIdentifier shardId, 
			final AgentFactory agentFactory,
			final SynchronizedFactory syncFactory) {
		super(config, supplier, shardId.toString());
		this.logName = shardId.toString();
		this.agentFactory = agentFactory;
		this.syncFactory = syncFactory;		
		this.executor = buildExecutor(config); 
		this.futuresBySynchro = new HashMap<>();
		this.runnablesBySynchro = new HashMap<>();
		this.callablesBySynchro = new HashMap<>();
		this.agentsByAction = new HashMap<>();
	}
	
	public ScheduledThreadPoolExecutor buildExecutor(final Config config) {
		final ScheduledThreadPoolExecutor exec = new ScheduledThreadPoolExecutor(
				config.getScheduler().getMaxConcurrency(),
				new ThreadFactoryBuilder()
					.setNameFormat(SchedulerSettings.THREAD_NAME_SCHEDULER + "-%d")
					.build(), 
				(r, x) -> logger.error("{}: ({}) Rejecting task {} (no capacity)", 
						getName(), logName, r.toString()))  ;
		
		exec.setRemoveOnCancelPolicy(true);
		exec.setContinueExistingPeriodicTasksAfterShutdownPolicy(false);
		exec.setExecuteExistingDelayedTasksAfterShutdownPolicy(false);
		
		exec.allowCoreThreadTimeOut(false);
		exec.prestartAllCoreThreads();
		
		return exec;
		
	}
	@Override
	public Map<Synchronized, ScheduledFuture<?>> getFutures() {
		return futuresBySynchro;
	}

	@Override
	public Agent get(Action action) {
		return agentsByAction.get(action);
	}

	@Override
	public void forward(Agent agent) {
		logger.info("{}: ({}) Forwarding task's execution ", getName(), logName, agent);
        if (stop(agent, false)) {
        	schedule(getAgentFactory()
    			.create(
					agent.getAction(), 
					agent.getPriority(), 
					agent.getFrequency(), //Frequency.ONCE,
					agent.getTask())
				.build());
        }
	}
	
	@Override
	public boolean stop(final Synchronized synchro) {
		return stop(synchro, true);
	}

	private boolean stop(final Synchronized synchro, final boolean withFire) {
		Validate.notNull(synchro);
		final Callable<?> callable = this.callablesBySynchro.get(synchro);
		final Runnable runnable = this.runnablesBySynchro.get(synchro);
		ScheduledFuture<?> future = this.futuresBySynchro.get(synchro);
		boolean dequeued = false;
		if (runnable != null) {
			logger.warn("{}: ({}) Removing synchronized {} from registry", getName(), logName, synchro);
			runnablesBySynchro.remove(synchro);
			dequeued = this.executor.remove(runnable);
		} else if (callable != null) {
			logger.warn("{}: ({}) Removing synchronized {} from registry", getName(), logName, synchro);
			callablesBySynchro.remove(synchro);
		} else {
			logger.error("{}: ({}) Runnable/Callable {} not found, finished or never scheduled", getName(), logName,
				synchro.getAction().name());
			return false;
		}
		boolean cancelled = false;
		if (future != null) {
			cancelled = future.cancel(withFire);
			logger.warn("{}: ({}) Stopping - Task {} ({}) Cancelled = {}, Dequeued = {}", getName(), logName,
				synchro.getAction().name(), runnable.getClass().getSimpleName(), cancelled, dequeued);
		} else {
			logger.error("{}: ({}) Stopping - Task {} ({}) Not found !!", getName(), logName, synchro.getAction().name(),
				runnable.getClass().getSimpleName());
		}
		this.executor.purge();
		agentsByAction.remove(synchro.getAction());
		return cancelled;
	}

	@Override
	public void run(Synchronized synchro) {
		runSynchronized(synchro);
	}

	@Override
	public void schedule(final Agent agent) {
		Validate.notNull(agent);
		Runnable runnable = null;
		ScheduledFuture<?> future = null;
		logger.debug("{}: ({}) Saving Agent = {} ", getName(), logName, agent.toString());
		if (agent.getFrequency() == Frequency.PERIODIC) {
			future = executor.scheduleWithFixedDelay(
					runnable = () -> runSynchronized(agent), 
					agent.getDelay(),
					agent.getPeriodicDelay(), 
					agent.getTimeUnit());
		} else if (agent.getFrequency() == Frequency.ONCE) {
			executor.execute(runnable = () -> runSynchronized(agent));
		} else if (agent.getFrequency() == Frequency.ONCE_DELAYED) {
			future = executor.schedule(runnable = () -> runSynchronized(agent), agent.getDelay(), MILLISECONDS);
		}

		futuresBySynchro.put(agent, future);
		runnablesBySynchro.put(agent, runnable);
		agentsByAction.put(agent.getAction(), agent);
	}
	
	

	/**
	 * Executes a lambda before acquiring a service permission, then it releases it.
	 * It loops in the specified case.  
	 */
	@SuppressWarnings("unchecked")
	private <R> R runSynchronized(final Synchronized sync) {
		try {
			sync.flagEnqueued();
			Validate.notNull(sync);
			if (sync.getPriority() == PriorityLock.HIGH_ISOLATED) {
				call(sync, false);
				return (R) new Boolean(true);
			}

			final boolean untilGrant = sync.getPriority() == PriorityLock.MEDIUM_BLOCKING;
			int retries = 0;
			while (!Thread.interrupted()) {
				final Permission p = untilGrant ? acquireBlocking(sync.getAction()) : acquire(sync.getAction());
				if (logger.isDebugEnabled()) {
					logger.debug("{}: ({}) {} operation {} to {}", getName(), logName, 
							sync.getAction(), p, sync.getTask().getClass().getSimpleName());
				}
				if (p == GRANTED) {
					return call(sync, true);
				} else if (p == RETRY && untilGrant) {
					if (retries++ < getConfig().getScheduler().getSemaphoreUnlockMaxRetries()) {
						if (logger.isDebugEnabled()) {
							logger.warn("{}: ({}) Sleeping while waiting to acquire lock: {}", getName(), logName, sync.getAction());
						}
						// TODO: WTF -> LockSupport.parkUntil(Config.SEMAPHORE_UNLOCK_RETRY_DELAY_MS);
						try {
							Thread.sleep(getConfig().getScheduler().getSemaphoreUnlockRetryFrequencyMs());
							continue;
						} catch (InterruptedException e) {
							logger.error("{}: ({}) While sleeping for unlock delay", getName(), logName, e);
							break;
						}
					} else {
						logger.warn("{}: ({}) Coordination starved ({}) for action: {} too many retries ({})", getName(), logName, p, sync
								.getAction(), retries);
						break;
					}
				} else {
					logger.error("{}: Unexpected situation !", getName());
					break;
				}
			}
		} catch (Exception e) {
			logger.error("{}: Unexpected ", getName(), e);
		}
		return null;
	}

	@SuppressWarnings("unchecked")
	private <R> R call(final Synchronized sync, boolean inSync) {
		try {
			if (sync.getTask() instanceof Runnable) {
				if (logger.isDebugEnabled()) {
					logger.debug("{}: ({}) Executing {}", getName(), logName, sync.toString().toLowerCase());
				}
				sync.execute();
				return (R) new Boolean(true);
			} else if (sync.getTask() instanceof Callable) {
				R call = ((Callable<R>) sync.getTask()).call();
				return call;
			} else {
				logger.error("{}: ({}) Cannot execute: {} task: {} IS NOT RUNNABLE NOR CALLABLE", getName(), logName,
					sync.getTask().getClass().getName(), sync.getAction());
			}
		} catch (Throwable t) {
			logger.error("{}: ({}) Untrapped task's exception while executing: {} task: {}", getName(), logName,
				sync.getTask().getClass().getName(), sync.getAction(), t);
		} finally {
			try {
				if (inSync) {
					release(sync.getAction());
				}
			} catch (Throwable t2) {
				logger.error("{}: ({}) Untrapped task's exception while Releasing: {} task: {}", 
						getName(), logName, sync.getTask().getClass().getName(), sync.getAction(), t2);
			}
		}
		return null;
	}

	@Override
	public void start() {
		super.start();
	}

	@Override
	public void stop() {
		this.executor.shutdownNow();
		super.stop();
	}

	@Override
	public Map<Action, Agent> getAgents() {
		return agentsByAction;
	}
	
	@Override
	public boolean inService() {
		return !this.executor.isShutdown();
	}

	@Override
	public SynchronizedFactory getFactory() {
		return this.syncFactory;
	}

	@Override
	public ScheduledThreadPoolExecutor getExecutor() {
		return executor;
	} 
	
	@Override
	public AgentFactory getAgentFactory() {
		return this.agentFactory;
	}

}
