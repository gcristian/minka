/**
 *  Copyright (c) 2011-2015 Zauber S.A.  -- All rights reserved
 */

package io.tilt.minka.spectator;

import java.util.concurrent.locks.LockSupport;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.tilt.minka.spectator.Locks;
import io.tilt.minka.spectator.ServerCandidate;

/* clase tonta para darle nombre a un lider - solo para testear */
public class Candidate implements ServerCandidate {
    
    private static final Logger logger = LoggerFactory.getLogger(Candidate.class);
    
    private boolean iAmTheLeader;
    private final String name;
    private final long lifeTime;
    private boolean iHaveServed;
    private Thread currentThread;
    private Locks locks;
    private final String clusterName;
    
    public Candidate(final String clusterName, final String name, final long lifeTime) {
        this.name = name;
        this.lifeTime = lifeTime;
        locks = new Locks("localhost:2181");
        //locks.setZookeeperHostPort();
        locks.runWhenLeader(clusterName, this);
        this.clusterName = clusterName;
    }
    
    @Override
    public void start() {
        this.currentThread = Thread.currentThread();
        iAmTheLeader = true;
        this.iHaveServed = true;
        new Thread(()->whileLeading()).start();
    }
    
    private void whileLeading() {
        while (this.iAmTheLeader) {
            LockSupport.parkUntil(300);
            logger.info("I'm " +  this.name + ", the Leader");
            // We must remain in the scope while we lead
            // If we exit, leader listeners in other servers will get the start() call
        }
        logger.info("Exiting leadership as requested");
    }
    
    public String getName() {
        return this.name;
    }

    @Override
    public void stop() {
        logger.info("Got Stop: " + this.name);
        this.iAmTheLeader = false;
        this.locks.stopCandidateOrLeader(clusterName, false);        
    }
    
    protected boolean iHaveServed() {
        return this.iHaveServed;
    }

    @Override
    public String toString() {            
        return this.name;
    }
    public boolean isiAmTheLeader() {
        return this.iAmTheLeader;
    }

    public Thread getCurrentThread() {
        return this.currentThread;
    }
}