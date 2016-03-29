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

import java.util.function.Consumer;

import org.apache.curator.framework.recipes.cache.NodeCache;
import org.apache.curator.framework.recipes.cache.TreeCache;
import org.apache.curator.framework.recipes.leader.LeaderLatch;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;

class UserInstanceObject {
        
    /* for distributed sharing lock */
    private InterProcessMutex mutex;
    private Runnable runnable;
    
    /* for leader election */
    private ServerCandidate candidate;
    private LeaderLatch latch;
        
    /* for queues */
    private NodeCache node;
    private TreeCache tree;
    private Consumer<MessageMetadata> consumer;
    private boolean callbackUserListeners;
    private Runnable connectionLostCallback;
    
    protected UserInstanceObject(final LeaderLatch leaderLatch, final ServerCandidate server) {
        super();
        this.latch = leaderLatch;
        this.candidate = server;
        this.callbackUserListeners = true;
    }
    protected UserInstanceObject(final InterProcessMutex mutex, final Runnable runnable) {
        super();
        this.mutex = mutex;
        this.runnable = runnable;
    }
    protected UserInstanceObject(final NodeCache node, final Consumer<MessageMetadata> consumer) {
        super();
        this.node = node;
        this.consumer = consumer;
    }
    protected UserInstanceObject(final TreeCache tree, final Consumer<MessageMetadata> consumer ) {
        super();
        this.tree = tree;
        this.consumer = consumer ;
    }
    public Runnable getConnectionLostCallback() {
        return this.connectionLostCallback;
    }
    public void setConnectionLostCallback(Runnable connectionLostCallback) {
        this.connectionLostCallback = connectionLostCallback;
    }
    protected LeaderLatch getLatch() {
        return this.latch;
    }
    protected ServerCandidate getServerCandidate() {
        return this.candidate;
    }
    protected InterProcessMutex getMutex() {
        return this.mutex;
    }
    protected NodeCache getNode() {
        return this.node;
    }
    protected Consumer<MessageMetadata> getConsumer() {
        return this.consumer;
    }
    protected TreeCache getTree() {
        return this.tree;
    }
    protected Runnable getRunnable() {
        return this.runnable;
    }
    protected boolean callbackListeners() {
        return callbackUserListeners;
    }
    protected void setCallbackListeners(boolean value) {
        this.callbackUserListeners = value;
    }
}