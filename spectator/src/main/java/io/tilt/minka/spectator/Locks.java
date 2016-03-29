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
package io.tilt.minka.spectator;

import java.io.IOException;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang.Validate;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.leader.LeaderLatch;
import org.apache.curator.framework.recipes.leader.LeaderLatch.CloseMode;
import org.apache.curator.framework.recipes.leader.LeaderLatchListener;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Over-curation of apache curator Locking recipes for over-simplified usage
 * 
 * @author Cristian Gonzalez
 * @since Dec 24, 2015
 *
 */
public class Locks extends Spectator {

    private static final String LOCK_PREFIX = "/lock:";
    private static final String LATCH_PREFIX = "/latch:";
    private static final Logger logger = LoggerFactory.getLogger(Locks.class);

    public Locks() {
        super();
    }
    
    public Locks(final String zookeeperHostPort) {
        super(zookeeperHostPort, null);
    }
    
    public Locks(final String zookeeperHostPort, String logId) {
        super(zookeeperHostPort, logId);
    }
    
    /**
     * Use the zookeper's host and port from System environment or properties
     */
    public Locks(Spectator spec) {
        super(spec, spec.getLogId());
    }
    
    /**
     * Participate 'ethernally' of an election among a cluster of leader candidates for a given duty
     * Like owning a lock, but the process hops between candidates until a leader for that duty is elected, 
     * while a leader is needed, if no other candidate exists, the leader is the first taking the lock.
     * 
     * Runs in a new thread always.
     * Idempotently.
     * 
     * @return      whether the listener participates in the leader meeting or not. True if it was done before.
     */
    public boolean runWhenLeader(final String ensembleName, final ServerCandidate server) {
        boolean participating = checkExistance(LATCH_PREFIX + ensembleName, server);
        if (!participating) {
            participating = createLeaderLatch(LATCH_PREFIX + ensembleName, server);
        }
        return participating;
    }
    

    /**
     * If you plan to orderly stop leader candidates instead of leaving that work to Spectator/Zookeeper
     * Then you can stop your candidates manually.
     * @param ensembleName
     * @param notifyCandidate
     */
    public void stopCandidateOrLeader(final String ensembleName, boolean notifyCandidate) {
        Iterator<UserInstanceObject> x = getUserMap().get(LATCH_PREFIX + ensembleName).iterator();
        while(x.hasNext()) {
            UserInstanceObject uio = x.next();
            if (uio.getServerCandidate()!=null) {
                try {
                    uio.setCallbackListeners(notifyCandidate);
                    uio.getLatch().close();
                } catch (IOException e) {
                    logger.error("{}: ({}) Unexpected while trying to stop candidate", getClass().getSimpleName(), 
                            getLogId(), e);
                }
            }
        }
    }

    

    /**
     * A race between runner candidates, first acquiring the lock will disable others to run.
     * i.e. run only if other duty with same Lock Is Not also running.
     * 
     * @param lockName      a name unique within the timewindow the runner intends to race for run
     * @param runnable      this is executed within the same caller's thread
     * @return              whether or not the runner acquired the lock and run  
     */
    public boolean runOnLockRace(final String lockName, final Runnable runnable) {
        return createLock(lockName, runnable, true, 0, TimeUnit.MICROSECONDS);
    }

    /**
     * A race between runner candidates, first acquiring the lock will disable others to run within the 
     * time window since run call and lock-wait time.
     * i.e. run only if no other duty is present or present withing a time window.
     * 
     * @param lockName      a name unique within the timewindow the runner intends to race for run
     * @param runnable      this is executed within the same caller's thread
     * @param lockWaitMs    time to wait for the lock to be released if already acquired
     * @return              whether or not the runner acquired the lock and run  
     */
    public boolean runOnLockRace(final String lockName, final Runnable runnable, final long lockWaitMs) {
        return createLock(lockName, runnable, true, lockWaitMs, TimeUnit.MILLISECONDS);
    }

    /**
     * The runner candidate waits for the lock to be released by others if already acquired
     * 
     * @param lockName      a name unique within the timewindow the runner intends to race for run
     * @param runnable      this is executed within the same caller's thread
     * @return              whether or not the runner acquired the lock and run  
     */
    public boolean runWhenUnlocked(final String lockName, final Runnable runnable) {
        return createLock(lockName, runnable, false, 0, TimeUnit.MILLISECONDS);
    }

    private boolean createLeaderLatch(final String ensembleName, final ServerCandidate server) {
        boolean participating = false;
        try {
            final CuratorFramework client = getClient();
            if (client==null) {
                logger.error("{}: ({}) Cannot use Distributed utilities before setting Zookeeper connection",
                    getClass().getSimpleName(), getLogId());
                return false;
            }
            
            final String latchNameObj = ensembleName;
            // let the notification option available to further client's manual close's option
            final LeaderLatch latch = new LeaderLatch(client, latchNameObj, 
                    String.valueOf(server.hashCode()), CloseMode.NOTIFY_LEADER);
            UserInstanceObject uio = new UserInstanceObject(latch, server);
            latch.addListener(new LeaderLatchListener() {
                public void notLeader() {
                    logger.info("Locks: ({}) LeaderLatch on: {} removing leadership from: {}", getLogId(), 
                        latchNameObj, server.getClass().getName());
                    if (uio.callbackListeners()) {
                        server.stop();
                    } else {
                        logger.info("Locks: ({}) Avoiding calling back user's listeners", getLogId());
                    }
                }
                public void isLeader() {
                    logger.info("Locks: ({}) LeaderLatch on: {} giving leadership to: {}",  
                            getLogId(), latchNameObj, server.getClass().getSimpleName());
                    server.start();
                }
            });
            // save it for future ref.
            add(latchNameObj, uio);
            latch.start(); // participate of election
            participating = true;
            if (logger.isDebugEnabled()) {
                logger.debug("{}: ({}) Participating of election process in lock {} ", getClass().getSimpleName(), 
                        getLogId(), latchNameObj);
            }
        } catch (Exception e) {
            if (isStarted() && isConnected()) {
                logger.error("{}: ({}) Unexpected while registering: {} with leader listener: {}", 
                        getClass().getSimpleName(), getLogId(), server.getClass().getSimpleName(), ensembleName, e);
            } else {
                logger.error("{}: ({}) Zookeeper Disconection: while registering: {} with leader listener: {}", 
                        getClass().getSimpleName(), getLogId(), server.getClass().getSimpleName(), ensembleName, e.getMessage());
            }
        }
        return participating;
    }
    
    public boolean acquireDistributedLock (
            final String lockName,  
            final boolean lockExclusively, 
            final long lockWait, 
            final TimeUnit lockWaitTimeUnit) {
        
        Validate.notNull(lockName, "the duty Name is required");
        
        boolean run = false;
        final CuratorFramework client = getClient();
        if (client==null) {
            logger.error("{}: ({}) Cannot use Distributed utilities before setting Zookeeper connection", 
                getClass().getSimpleName(), getLogId());
            return false;
        }
                
        final String lockNameObj = LOCK_PREFIX + lockName;
        try {
            InterProcessMutex mutex = new InterProcessMutex(client, lockNameObj);
            if (lockExclusively) {
                if (run = mutex.acquire(lockWait, lockWaitTimeUnit)) {
                    run = true;
                    final UserInstanceObject uo = new UserInstanceObject(mutex, null);
                    add(lockNameObj, uo);
                } else {
                    logger.warn(lockNameObj, "lock was already acquired");
                    run = false;
                }
            } else {
                mutex.acquire(); // wait until others release
                run = true;
            }
        } catch (Exception e) {
            if (isStarted() && isConnected()) {
                logger.error("{}: ({}) Unexpected while registering leader listener: {}", getClass().getSimpleName(), 
                        getLogId(), lockNameObj, e);
            } else {
                logger.error("{}: ({}) Zookeper Disconnection: while registering leader listener: {}", 
                    getClass().getSimpleName(), getLogId(), lockNameObj, e.getMessage());
            }
        }
        return run;
    }
    
    public boolean releaseDistributedLock(final String lockName) {
        final String lockNameObj = LOCK_PREFIX + lockName;
        Set<UserInstanceObject> set = getUserMap().get(lockNameObj);
        if (set==null) {
            logger.error("{}: ({}) Lock: {} is unknown or never acquired", getClass().getSimpleName(), getLogId(), lockName);
            return false;
        }
        
        for (UserInstanceObject uio: set) {
            try {
                final InterProcessMutex mutex = uio.getMutex();
                if (uio.getMutex()!=null) {
                    mutex.release();
                } else {
                    logger.error("{}: ({}) Lock: {} jas mo mutex object, never acquired ?", getClass().getSimpleName(), 
                            getLogId(), lockName);
                }
                remove(lockNameObj, uio);
            } catch (Exception e) {
                if (isStarted() && isConnected()) {
                    logger.error("{}: ({}) Unexpected while releasing mutex: {}", getClass().getSimpleName(), 
                            getLogId(), lockNameObj, e);
                } else {
                    logger.error("{}: ({}) Zookeper Disconnection: while releasing mutex: {}", 
                        getClass().getSimpleName(), getLogId(), lockNameObj, e.getMessage());
                }
            }
        }
        return true;
    }

    private boolean createLock(
            final String lockName, 
            final Runnable runnable, 
            final boolean lockExclusively, 
            final long lockWait, 
            final TimeUnit lockWaitTimeUnit) {
        
        Validate.notNull(lockName, "the duty Name is required");
        Validate.notNull(runnable, "the duty Listener is required");
        
        boolean run = false;
        final CuratorFramework client = getClient();
        if (client==null) {
            logger.error("{}: ({}) Cannot use Distributed utilities before setting Zookeeper connection", 
                getClass().getSimpleName(), getLogId());
            return false;
        }
        
        if (checkExistance(LOCK_PREFIX + lockName, runnable)) {
            throw new RuntimeException("Cannot create lock with existing name: " + lockName + 
                    " and the same listener! : " + runnable.toString());
        }

        InterProcessMutex mutex = null;
        final UserInstanceObject uo = new UserInstanceObject(mutex, runnable);
        
        final String lockNameObj = LOCK_PREFIX + lockName;
        try {
            add(lockNameObj, uo);
            mutex = new InterProcessMutex(client, lockNameObj);
            if (lockExclusively) {
                // i.e.: if other process has the lock: EXIT FAST
                if (run = mutex.acquire(lockWait, lockWaitTimeUnit)) {
                    runnable.run();
                } else {
                }
            } else {
                mutex.acquire(); // wait until others release
                runnable.run();
                run = true;
            }
        } catch (Exception e) {
            if (isStarted() && isConnected()) {
                logger.error("{}: ({}) Unexpected while registering leader listener: {}", 
                    getClass().getSimpleName(), getLogId(), lockNameObj, e);
            } else {
                logger.error("{}: ({}) Zookeper Disconnection: while registering leader listener: {}", 
                    getClass().getSimpleName(), getLogId(), lockNameObj, e.getMessage());
            }
        } finally {
            try {
                if (mutex !=null && mutex.isAcquiredInThisProcess()) {
                    mutex.release();
                }
                remove(lockNameObj, uo);
                // just for consistency
                //runnable.stop();
            } catch (Exception e) {
                if (isStarted() && isConnected()) {
                    logger.error("{}: ({}) Unexpected while releasing mutex: {}", 
                        getClass().getSimpleName(), getLogId(), lockNameObj, e);
                } else {
                    logger.error("{}: ({}) Zookeper Disconnection: while releasing mutex: {}", 
                        getClass().getSimpleName(), getLogId(), lockNameObj, e.getMessage());
                }
            }
        }
        return run;
    }
    
    
}
