/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.tilt.minka.business.impl;


import static io.tilt.minka.business.Coordinator.logger;
import static io.tilt.minka.business.Semaphore.Permission.GRANTED;
import static io.tilt.minka.business.Semaphore.Permission.RETRY;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;

import org.apache.commons.lang.Validate;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

import io.tilt.minka.api.Config;
import io.tilt.minka.business.Coordinator;
import io.tilt.minka.business.Semaphore;
import io.tilt.minka.business.Coordinator.Frequency;
import io.tilt.minka.business.Coordinator.PriorityLock;
import io.tilt.minka.business.Coordinator.Synchronized;
import io.tilt.minka.business.Coordinator.SynchronizedAgent;
import io.tilt.minka.business.Coordinator.SynchronizedAgentFactory;
import io.tilt.minka.business.Semaphore.Action;
import io.tilt.minka.business.Semaphore.Permission;
import io.tilt.minka.business.Semaphore.Rule;
import io.tilt.minka.domain.ShardID;

/**
 * @author Cristian Gonzalez
 * @since Dec 28, 2015
 */
public class CoordinatorImpl extends SemaphoreImpl implements Coordinator {

    private static final int MAX_CONCURRENT_THREADS = 10;
    
    /* for scheduling and stopping tasks */
    private Map<Synchronized, ScheduledFuture<?>> futuresBySynchro;
    private Map<Synchronized, Runnable> runnablesBySynchro;
    private Map<Synchronized, Callable<?>> callablesBySynchro;
    private Map<Action, SynchronizedAgent> agentsByAction;
    
    /* for local scope actions */ 
	private final Map<Action, Rule> rules;
	private final ShardID shardId;
	
	/* for global scope actions */    
    private ScheduledThreadPoolExecutor executor;
    
    public CoordinatorImpl(final Config config, final SpectatorSupplier supplier, final ShardID shardId) {
	    super(config, supplier, shardId);
	    this.shardId = shardId;
	    this.executor = new ScheduledThreadPoolExecutor(MAX_CONCURRENT_THREADS, 
	            new ThreadFactoryBuilder().setNameFormat(Config.THREAD_NAME_COORDINATOR_IN_BACKGROUND).build());
        executor.setRemoveOnCancelPolicy(true);
        executor.setContinueExistingPeriodicTasksAfterShutdownPolicy(false);
        executor.setExecuteExistingDelayedTasksAfterShutdownPolicy(false);
        
		this.rules = new HashMap<>();
		getLockingRules().forEach(rule->this.rules.put(rule.getAction(), rule));
		
		this.futuresBySynchro = new HashMap<>();
		this.runnablesBySynchro = new HashMap<>();
		this.callablesBySynchro = new HashMap<>();
		this.agentsByAction= new HashMap<>();
	}
	

    @Override
    public SynchronizedAgent get(Action action) {
        return agentsByAction.get(action);
    }
    
    @Override
    public void forward(SynchronizedAgent agent) {
        stop(agent);
        schedule(SynchronizedAgentFactory.build(agent.getAction(), agent.getPriority(), 
                Frequency.ONCE, agent.getTask()));
        schedule(agent); // go back to old
    }

    @Override
    public void stop(final Synchronized synchro) {
        stop(synchro, true);
    }
    
    private void stop(final Synchronized synchro, final boolean withFire) {
        Validate.notNull(synchro);
        final Callable<?> callable = this.callablesBySynchro.get(synchro);
        final Runnable runnable = this.runnablesBySynchro.get(synchro);
        ScheduledFuture<?> future = this.futuresBySynchro.get(synchro);
        boolean dequeued = false;
        
        if (runnable!=null) {
            runnablesBySynchro.remove(synchro);
            dequeued = this.executor.remove(runnable);            
        } else if (callable!=null) {
            callablesBySynchro.remove(synchro);
        } else {
            logger.error("{}: ({}) Runnable/Callable {} not found, finished or never scheduled", 
                    getClass().getSimpleName(), shardId, synchro.getAction().name());
            return;
        }
        
        if (future!=null) {
            boolean cancelled = future.cancel(withFire);
            logger.debug("{}: ({}) Stopping - Task {} ({}) Cancelled = {}, Dequeued = {}", getClass().getSimpleName(), 
                    shardId, synchro.getAction().name(), runnable.getClass().getSimpleName(), cancelled, dequeued);
        } else {
            logger.error("{}: ({}) Stopping - Task {} ({}) Not found !!", getClass().getSimpleName(), 
                    shardId, synchro.getAction().name(), runnable.getClass().getSimpleName());
        }
        this.executor.purge();
        agentsByAction.remove(synchro.getAction());
    }
    
    @Override
    public void run(Synchronized synchro) {
        runSynchronized(synchro);
    }

    @Override
    public void schedule(final SynchronizedAgent agent) {
        Validate.notNull(agent);
        Runnable runnable = null;
        ScheduledFuture<?> future = null;
        logger.debug("{}: ({}) Saving Agent = {} ", getClass().getSimpleName(), shardId, agent.toString());
        if (agent.getFrequency() == Frequency.PERIODIC) {
            future = executor.scheduleWithFixedDelay(runnable = ()-> runSynchronized(agent), agent.getDelay(), 
                    agent.getPeriodicDelay(), agent.getTimeUnit());
        } else if (agent.getFrequency() == Frequency.ONCE) {
            executor.execute(runnable = ()-> runSynchronized(agent));
        } else if (agent.getFrequency() == Frequency.ONCE_DELAYED) {
            future = executor.schedule(runnable = ()-> runSynchronized(agent), agent.getDelay(), MILLISECONDS);
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
    private <R>R runSynchronized(final Synchronized sync) {
        Validate.notNull(sync);
        final boolean untilGrant = sync.getPriority() == PriorityLock.MEDIUM_BLOCKING; 
        if (sync.getPriority()== PriorityLock.HIGH_ISOLATED) {
            sync.execute();
            return (R) new Boolean(true);
        }
        
        int retries = 0;
        while (!Thread.interrupted()) {
            final Permission p = untilGrant ? acquireBlocking(sync.getAction()) : acquire(sync.getAction());
            if (logger.isDebugEnabled()) {
                logger.debug("{}: ({}) {} operation {} to {}", getClass().getSimpleName(), shardId,  
                        sync.getAction(), p, sync.getTask().getClass().getSimpleName());
            }
            if (p == GRANTED) {
                try {
                    if (sync.getTask() instanceof Runnable) {
                        sync.execute();
                        return (R) new Boolean(true);
                    } else if (sync.getTask() instanceof Callable ) {
                        // TODO
                        R call = ((Callable<R>)sync.getTask()).call();
                        return call;
                    }
                } catch (Exception e) {
                    logger.error("{}: ({}) Untrapped task's exception while executing: {} task: {}", 
                            getClass().getSimpleName(), shardId, sync.getTask().getClass().getName(), sync.getAction(), e);
                } finally {
                    release(sync.getAction());
                }
                break;
            } else if (p == RETRY && untilGrant) {
                if (retries++ < Config.SEMAPHORE_UNLOCK_MAX_RETRIES) {
                    if (logger.isDebugEnabled()) {
                        logger.debug("{}: Sleeping while waiting to acquire lock: {}", 
                            getClass().getSimpleName(), shardId, sync.getAction());
                    }
                    // TODO: WTF -> LockSupport.parkUntil(Config.SEMAPHORE_UNLOCK_RETRY_DELAY_MS);
                    try {
                        Thread.sleep(Config.SEMAPHORE_UNLOCK_RETRY_DELAY_MS);
                    } catch (InterruptedException e) {
                        logger.error("{}: ({}) While sleeping for unlock delay", getClass().getSimpleName(), shardId, e);
                    }
                } else {
                    logger.warn("{}: ({}) Coordination starved ({}) for action: {} too many retries ({})", 
                            getClass().getSimpleName(), shardId, p, sync.getAction(), retries);
                    /*throw new RuntimeException("Coordination starved for action: " + dispatch.getClass() 
                        + " too many retries");*/
                }
            } else {
                break;
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
    public boolean inService() {
        return !this.executor.isShutdown();
    }

}
