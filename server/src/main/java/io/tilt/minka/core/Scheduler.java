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
package io.tilt.minka.business;

import java.util.concurrent.TimeUnit;

import org.apache.commons.lang.Validate;
import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Centralization of background one-shot and scheduled tasks.
 * Also friendler retrying mechanism for the Semaphore 
 * 
 * @author Cristian Gonzalez
 * @since Nov 27, 2015
 *
 */
public interface Coordinator extends Semaphore {

    final Logger logger = LoggerFactory.getLogger(Coordinator.class);

    /* basic timed unit of work for a thread pool */
    /* for better traceability, isolation, metrics, data output exposure */
    public interface TimedTask {
        Action getAction();
        void execute();
        Runnable getTask();
        //void cancel();
        long getLastExecutionTimestamp();
        long getLastSuccessfulExecutionTimestamp();
        long getLastSuccessfulExecutionLapse();
        Exception getLastException();
        default <T>T getResult() {
            return null;
        }
    }

    /* the way this task will be trated by the coordinator at the semaphore */
    public enum PriorityLock {
        /* no locks will be acquired to run this */
        HIGH_ISOLATED,
        /* high priority blocks the caller thread until permission is given */
        MEDIUM_BLOCKING,
        /* low priority will run only if permission is immediately granted */
        LOW_ON_PERMISSION,
        
        HIGH_DISABLING_SLAVES,
        HIGH_ASKING_MASTER,
        ;
    }
    
    /* run this in the caller's thread */
    void run(Synchronized synchro);
    /* schedule this to run in the pool */
    void schedule(SynchronizedAgent agent);
    /* stop the agent of running */
    void stop(Synchronized agent);
    /* forward to execute it now, leaving future schedules intact */
    void forward(SynchronizedAgent agent);
    /* query */
    SynchronizedAgent get(Action action);
    
    /* a task that needs synchronization with other tasks */
    public interface Synchronized extends TimedTask {
        PriorityLock getPriority();
    }
    
    /* for agents only */
    public enum Frequency {
        ONCE,
        ONCE_DELAYED,
        PERIODIC,
    }

    /* a repetitive timed task */
    public interface SynchronizedAgent extends Synchronized {
        Frequency getFrequency();
        long getDelay();
        TimeUnit getTimeUnit();
        long getPeriodicDelay();
    }
    
    

    public static class SynchronizedFactory implements Synchronized {
        private final Action action;
        private final PriorityLock priority;
        private final Runnable task;
        private Object result;
        private long lastExecutionTimestamp;
        private long lastSuccessfulExecutionTimestamp;
        private long lastSuccessfulExecutionLapse;

        private Exception lastException;
        
        protected SynchronizedFactory(
                final Action action, 
                final PriorityLock priority, 
                final Runnable task) {
            Validate.notNull(action);
            Validate.notNull(priority);
            Validate.isTrue(task!=null);
            this.action = action;
            this.priority = priority;
            this.task = task;
        }
        
        public static SynchronizedFactory build(
                final Action action, 
                final PriorityLock priority, 
                final Runnable task) {
            return new SynchronizedFactory(action, priority, task);
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
            if (obj!=null && obj instanceof Synchronized) {
                Synchronized ag = (Synchronized)obj;
                return new EqualsBuilder().append(ag.getAction(), getAction()).isEquals();
            } else {
                return false;
            }
        }
        
        @Override
        public String toString() {
            return new ToStringBuilder(getAction(), ToStringStyle.DEFAULT_STYLE)
                .append("priority", getPriority())
                .toString();
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
                logger.error("Untrapped exception running synchronized action", e);
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
        
        /*@Override
        public void cancel() {
            if (task!=null) {
                task.run();
            } else if (callable!=null) {
                this.result = callable.call();
            }
        }*/

    }
    
    public static class SynchronizedAgentFactory extends SynchronizedFactory implements SynchronizedAgent {

        private long delay;
        private long periodicDelay;
        private final Frequency frequency;
        
        protected SynchronizedAgentFactory(
                final Action action, 
                final PriorityLock priority, 
                final Frequency frequency, 
                final Runnable task) {
            super(action, priority, task);
            Validate.notNull(frequency);
            this.frequency = frequency;
        }
        
        public static SynchronizedAgentFactory build(
                final Action action, 
                final PriorityLock priority, 
                final Frequency frequency, 
                final Runnable task) {
            return new SynchronizedAgentFactory(action, priority, frequency, task);
        }
        
        public SynchronizedAgentFactory every(long periodicDelay) {
            this.periodicDelay = periodicDelay;
            return this;
        }
        
        public SynchronizedAgentFactory delayed(long firstDelayMs) {
            this.delay = firstDelayMs;
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
            return new ToStringBuilder(getAction().name(), ToStringStyle.DEFAULT_STYLE)
                .append("frequency", getFrequency())
                .append("delay", getDelay())
                .append("period", getPeriodicDelay())
                .append("priority", getPriority())
                .append("task", getTask().getClass().getSimpleName())
                .toString();
        }

        @Override
        public TimeUnit getTimeUnit() {
            return TimeUnit.MILLISECONDS;
        }
    }

    
 }
