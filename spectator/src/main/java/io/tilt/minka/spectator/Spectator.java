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

import static org.apache.curator.framework.state.ConnectionState.LOST;
import static org.apache.curator.framework.state.ConnectionState.SUSPENDED;

import java.io.Closeable;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.commons.lang.Validate;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.api.UnhandledErrorListener;
import org.apache.curator.framework.imps.CuratorFrameworkState;
import org.apache.curator.framework.recipes.leader.LeaderLatch.State;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.framework.state.ConnectionStateListener;
import org.apache.curator.retry.RetryNTimes;
import org.apache.zookeeper.ZooKeeper.States;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Coordination utilities for distributed servers, extending Apache Curator recipes.
 * 
 * A publish/subscribe distributed message queue 
 * A leader election (thru Apache Curator)
 * A named locking facility to synchronize distributed processors. (thru Apache Curator)
 * 
 * @author Cristian Gonzalez
 * @since Oct 7, 2015
 */
public class Spectator implements Closeable {
   
    private static final Logger logger = LoggerFactory.getLogger(Spectator.class);
    
    private final String SHUTDOWN_THREAD_NAME = "Spectator-Shutdown";    
    public final String ZOOKEEPER_CONNECT_STRING = "zookeeper.connect";
    private final String SPECTATOR_ZK_SUBDOMAIN_PATH= "spectator-domain";

    // retry during 30 mins, each 5 seconds
    public final static int CONNECTION_RETRY_ATTEMPTS_DEFAULT = 3;
    public final static int CONNECTION_RETRY_DELAY_MS_DEFAULT = 1000;
    public final static int QUEUE_RETENTION_DAYS_DEFAULT = 5;
    
    private static CuratorFramework sharedClient;
    private Map<String, Set<UserInstanceObject>> mapObjects;
    private String zookeeperHostPort;
    private static int connectionRetryAttempts = CONNECTION_RETRY_ATTEMPTS_DEFAULT ;
    private static int connectionRetryDelayMs = CONNECTION_RETRY_DELAY_MS_DEFAULT;
    private static int queueRetentionDays = QUEUE_RETENTION_DAYS_DEFAULT;
    
    private ReentrantLock shutdownLock;
    private boolean shutdownDone;
    private final String logId;
    
    public Spectator(Spectator spec) {
        this(spec.getZookeeperHostPort(), spec.getLogId());
    }
    
    public Spectator() {
        this("", null);
    }
    
    protected String getLogId() {
        return this.logId;
    }

    public Spectator(Spectator spec, String logId) {
        this(spec.getZookeeperHostPort(), logId);
        boolean started = isStarted();
        sharedClient = spec.getClient();
        if (started) {
            hookListeners();
        }
        
    }
    
    public Spectator(final String hostPortArg, String logId) {
        this.logId = logId == null ? String.valueOf(hashCode()) : logId; 
        this.shutdownLock = new ReentrantLock(true);
        // initialize user objects map
        this.mapObjects = new ConcurrentHashMap<>();
        setHostPort(hostPortArg);
        
        // ensure finalization
        Runtime.getRuntime().addShutdownHook(new Thread(()->leaveAllMeetings(true), SHUTDOWN_THREAD_NAME));
    }

    private void setHostPort(final String hostPortArg) {
        String hostPort = null;
        if (hostPortArg == null || hostPortArg.isEmpty()) {
            // obtain ZK connection params
            hostPort = System.getProperty(ZOOKEEPER_CONNECT_STRING);
            if (hostPort == null || hostPort.isEmpty()) {
                hostPort = System.getenv(ZOOKEEPER_CONNECT_STRING);
            }
        } else {
            hostPort = hostPortArg;
        }
        if (hostPort != null) {
            zookeeperHostPort = hostPort;
            // initialize connection
            getClient();
        } else {
            logger.warn("{}: ({}) Spectator will wait for zookeeper connect string be statically set !", 
                    getClass().getSimpleName(), logId);
        }
    }
    
    protected Map<String, Set<UserInstanceObject>> getUserMap() {
        return mapObjects;
    }
    
    /**
     * Manually close all spectator objects.
     * Dont use this for JVM destruction method. 
     * instead use {@linkplain destroy()}
     * 
     * @param closeUserListeners    Use true for user listeners to be called back   
     *   
     */
    public void close(boolean closeUserListeners) {
        // avoid calling user's listeners
        leaveAllMeetings(closeUserListeners);
    }
    
    public void close() {
        close(true);
    }
    
    /**
     * Close all spectator objects.
     * All user listeners will be called back
     */
    public void destroy() {
        leaveAllMeetings(true);
    }

    protected boolean checkExistance(final String name, final Object listener) {
        Validate.notNull(name, "the duty Name is required");
        Validate.notNull(listener, "the duty Listener is required");
        if (mapObjects.containsKey(name)) {
            for(UserInstanceObject uio: mapObjects.get(name)) {
                if (uio!=null) {
                    if ((uio.getServerCandidate()!=null && uio.getServerCandidate().equals(listener)) || 
                            (uio.getConsumer()!=null && uio.getConsumer().equals(listener))) {
                        logger.warn("{}: ({}) Attempting to overwrite a Name ({}) with the same listener", 
                                getClass().getSimpleName(), logId, name);
                        return true;
                    } else {
                        /*throw new RuntimeException("Name (" + name + 
                                ") already exists with a different listener instance");
                                */
                        // TODO quizas debiera tirarla
                        return false;
                    }
                }
            }
        }
        return false;
    }
    

    /** 
     * stop participating leader election, locks and queueing nodes 
     * called at:  
     * 1) jvm shutdown, 
     * 2) zookeeper's connection lost/suspended
     * @param closeUserObjects  when call by the user he doesnt need to be called back 
     */
    private void leaveAllMeetings(final boolean closeUserObjects) {
        if (!shutdownDone && !shutdownLock.isHeldByCurrentThread() && shutdownLock.tryLock()) {
            if (mapObjects.isEmpty()) {
                logger.warn("{}: ({}) No user listeners to stop/close !?", getClass().getSimpleName(), logId);
            } else {
                logger.info("{}: ({}) Closing all spectator objects and stopping user listeners...", 
                    getClass().getSimpleName(), logId);        
                for (Entry<String, Set<UserInstanceObject>> entry: mapObjects.entrySet()) {
                    String name = entry.getKey();
                    logger.warn("{}: ({}) Leaving {} ", getClass().getSimpleName(), logId, name);
                    for (UserInstanceObject uio: entry.getValue()) {
                        discard(name, uio, closeUserObjects);
                    }
                }
            }
            /*if (isStarted()) {
                sharedClient.close();
            }*/
            shutdownDone = true;
        } else {
            logger.warn("{}: ({}) Unable to obtain shutdown locks to discard spectator objects", 
                    getClass().getSimpleName(), logId);
        }
    }

    private void hookListeners() {
        sharedClient.getConnectionStateListenable().addListener(
            new ConnectionStateListener() {
            @Override
            public void stateChanged(CuratorFramework client, ConnectionState newState) {
                logger.info("{}: ({}) Connection state perceived as {} ", Spectator.class.getSimpleName(), logId, newState);
                if (newState == SUSPENDED) {
                    // only leave at lost
                    leaveAllMeetings(true);
                } else if (newState == LOST) {
                    leaveAllMeetings(true);
                } else if (newState == ConnectionState.RECONNECTED) {
                    // the latch recreates the znode and re-adds my previous listener ??
                } else if (newState == ConnectionState.READ_ONLY) {
                    // i'm not allowing entering read only mode
                }
            }
        });
        sharedClient.getUnhandledErrorListenable().addListener(new UnhandledErrorListener() {
            @Override
            public void unhandledError(String message, Throwable e) {
                logger.error("{}: ({}) Curator drops an Unhandled Error: {} ", getClass().getSimpleName(), logId, message, e);
            }
        });
    }
    
    private void discard(String name, UserInstanceObject uio, final boolean closeUserObjects) {
        try {
            // play friendly and avoid recursiveness
            if (uio.getServerCandidate()!=null) {
                uio.setCallbackListeners(closeUserObjects);
            }
            // in case of leader latch only
            if (uio.getLatch()!=null && uio.getLatch().getState() == State.STARTED) {
                uio.getLatch().close();
            }
            
            // in case of Locks: distributed mutex mostly
            if (uio.getMutex()!=null && uio.getMutex().isAcquiredInThisProcess()) {
                uio.getMutex().release();
            }
            // in case of Queues: subscription mostly
            if (uio.getNode()!=null) {
                uio.getNode().close();
            }
            
        } catch (Exception e) {
            if (isStarted() && isConnected()) {
                logger.error("{}: ({}) Unexpected while discarding object: {} during leaving all meetings", 
                        getClass().getSimpleName(), logId, name, e);
            } else {
                logger.error("{}: ({}) Zookeeper Disconection: while discarding object: {} during leaving all meetings", 
                        getClass().getSimpleName(), logId, name, e.getMessage());
            }
        } finally {
            // call client's emergency callback to warn about dead ZK !! 
            if (uio.getConnectionLostCallback()!=null) {
                logger.info("{}: ({}) Calling connection lost callback: {}", getClass().getSimpleName(), logId, 
                        uio.getConnectionLostCallback().getClass().getName());
                uio.getConnectionLostCallback().run();
            } else {
                logger.warn("{}: ({}) Connection lost callback is NULL ", getClass().getSimpleName(), logId);
            }
        }
        
    }

    protected long elapsed(final long now) {
        return System.currentTimeMillis() - now;
    }
    
    protected void remove(final String name, final UserInstanceObject uio) {
        Set<UserInstanceObject> set = mapObjects.get(name);
        if (set != null) {
            set.remove(uio);
        }
    }
    
    protected void add(final String name, final UserInstanceObject uo) {
        Set<UserInstanceObject> set = mapObjects.get(name);
        if (set == null) {
            mapObjects.put(name, set = new HashSet<UserInstanceObject>());
        }
        set.add(uo);
    }
    
    public boolean isConnected() {
        try {
            return sharedClient.getZookeeperClient().isConnected()
                && sharedClient.getZookeeperClient().getZooKeeper().getState()==States.CONNECTED;
        } catch (Exception e) {
        }
        return false;
    }
    
    protected boolean isStarted() {
        try {
            return sharedClient !=null && sharedClient.getState() == CuratorFrameworkState.STARTED;
        } catch (Exception e) {
            logger.error("{}: ({}) Unexpected while checking is connected", getClass().getSimpleName(), logId, e);
        }
        return false;
    }

    protected CuratorFramework getClient() {
        if (!isStarted()) {
            if (zookeeperHostPort == null) {
                logger.error("{}: ({}) Zookeeper's connect string ({}) unresolved. Must set system property, "
                        + "environment variable or use static class setter", getClass().getSimpleName(), logId, 
                        ZOOKEEPER_CONNECT_STRING );
            } else {
                synchronized (Spectator.class) {
                    if (!isStarted()) {
                        logger.info("{}: ({}) Spectator connecting to zookeeper...", getClass().getSimpleName(), logId);
                        RetryPolicy retryPolicy = new RetryNTimes(connectionRetryAttempts, connectionRetryDelayMs);
                        sharedClient = CuratorFrameworkFactory.builder()
                                .namespace(SPECTATOR_ZK_SUBDOMAIN_PATH)
                                .connectString(zookeeperHostPort)
                                .retryPolicy(retryPolicy)
                                .build();
                        hookListeners();
                        try {
                            sharedClient.blockUntilConnected(10, TimeUnit.SECONDS);
                        } catch (InterruptedException e) {
                            // TODO Auto-generated catch block
                            e.printStackTrace();
                        }
                        sharedClient.start();
                    }
                }
            }
        }
        return sharedClient;
    }

    public void setConnectionLostCallback(final String ensembleName, final Runnable callback) {
        final Set<UserInstanceObject> uios = this.mapObjects.get(ensembleName);
        for (UserInstanceObject uio: uios) {
            uio.setConnectionLostCallback(callback);
        }
    }
    
    /**
     * Global to all ensembles generated within this instance
     * @param callback
     */
    public void setConnectionLostCallback(final Runnable callback) {        
        for (Collection<UserInstanceObject> coll: this.mapObjects.values()) {
            for (UserInstanceObject uio: coll) {
                uio.setConnectionLostCallback(callback);
            }
        }
    }
    
    /**
     * @return  If not manually set defaults to @see {@link Spectator.CONNECTION_RETRY_ATTEMPTS_DEFAULT}
     */
    public int getConnectionRetryAttempts() {
        return connectionRetryAttempts;
    }

    /**
     * In case ZK connection fails, set your own number of retries
     * @param connectionRetryAttempts
     */
    public void setConnectionRetryAttempts(int connectionRetryAttempts) {
        Spectator.connectionRetryAttempts = connectionRetryAttempts;
        if (isStarted()) {
            sharedClient.getZookeeperClient().setRetryPolicy(
                    new RetryNTimes(connectionRetryAttempts, connectionRetryDelayMs));
        }
    }

    /**
     * @return  If not manually set defaults to @see {@link Spectator.CONNECTION_RETRY_DELAY_MS_DEFAULT}
     */
    public int getConnectionRetryDelayMs() {
        return connectionRetryDelayMs;
    }

    /**
     * In case ZK connection fails, set your own delay between retries
     * @param connectionRetryDelayMs
     */
    public void setConnectionRetryDelayMs(int connectionRetryDelayMs) {
        Spectator.connectionRetryDelayMs = connectionRetryDelayMs;
        if (isStarted()) {
            sharedClient.getZookeeperClient().setRetryPolicy(
                    new RetryNTimes(connectionRetryAttempts, connectionRetryDelayMs));
        }
    }

    /**
     * @return  If not manually set defaults to @see {@link Spectator.QUEUE_RETENTION_DAYS_DEFAULT}
     */
    public static int getQueueRetentionDays() {
        return queueRetentionDays;
    }

    /**
     * Messages on queues deletion occurs by default to @see {@link Spectator.QUEUE_RETENTION_DAYS_DEFAULT}
     * @param queueRetentionDays
     */
    public void setQueueRetentionDays(int queueRetentionDays) {
        Spectator.queueRetentionDays = queueRetentionDays;
    }

    /**
     * @return  current zookeeper's connect string manually set or get as system property/environment var.
     */
    public String getZookeeperHostPort() {
        return zookeeperHostPort;
    }

    /**
     * When zookeeper's connect string not provided as System property or environment variable
     * You must manually set the value, or nothing will work.
     * 
     * @param zookeeperHostPortArg  in the format "hostname1:port,hostname2:port"
     */
    public void setZookeeperHostPort(String zookeeperHostPortArg) {
        zookeeperHostPort = zookeeperHostPortArg;
    }

}