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

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import org.apache.commons.lang.SerializationUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.imps.CuratorFrameworkState;
import org.apache.curator.framework.recipes.cache.NodeCacheListener;
import org.apache.curator.framework.recipes.cache.TreeCacheEvent;
import org.apache.curator.framework.recipes.cache.TreeCacheListener;
import org.apache.zookeeper.KeeperException.NoNodeException;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

/**
 * @author Cristian Gonzalez
 * @since Oct 15, 2015
 */
public class PublishSubscribeQueue implements TreeCacheListener, NodeCacheListener, Runnable {

    private static final Logger logger = LoggerFactory.getLogger(PublishSubscribeQueue.class);

    private long lastCtimeRead;
    private final String name;
    private final ScheduledExecutorService retentionExecutor;
    private CuratorFramework client;
    private final long retentionMs;
    private final String logId;
    
    private final Consumer<MessageMetadata> userListener;
    
    protected PublishSubscribeQueue(
            final String name, 
            final Consumer<MessageMetadata> consumer, 
            final long retentionMs, 
            final CuratorFramework client, 
            final String logId) {
        
        this.name = name;
        this.userListener = consumer;
        this.retentionMs = retentionMs;
        this.client = client;
        this.logId = logId;
        this.retentionExecutor = Executors.newSingleThreadScheduledExecutor(
                new ThreadFactoryBuilder().setNameFormat("SPECTATOR-Queues-Retainer").build());
        
        start(retentionMs);
    }

    protected void stop() {
        this.retentionExecutor.shutdownNow();
    }
    
    protected void start(final long retentionMs) {
        this.retentionExecutor.scheduleWithFixedDelay(this, 1000l, Math.max(retentionMs, 1000l), TimeUnit.MILLISECONDS);
    }
    
    @Override
    /* for other types of queues */
    public void nodeChanged() throws Exception {
        readMessages(false, this.client);
    }
    
    private void checkRetention() {
        if (client.getState()!= CuratorFrameworkState.STARTED) {
            return;
        }
        final Stat parentStat = new Stat();
        final String queuePath = Queues.QUEUE_PREFIX + this.name;
        
        try {
            logger.info("{}: ({}) Queue: {} checking retention...", getClass().getSimpleName(), logId, name); 
            if (client.checkExists().forPath(queuePath)!=null) {
                List<String> childrenNodePaths = client.getChildren().storingStatIn(parentStat).forPath(queuePath);
                logger.info("{}: ({}) Queue: {} has {} total messages", 
                        getClass().getSimpleName(), logId, name, childrenNodePaths.size());
                final long maxTime = System.currentTimeMillis() - this.retentionMs;
                int deleteCount =0;
                final Stat childStat = new Stat();
                for (final String childPath: childrenNodePaths) {
                    client.getData().storingStatIn(childStat).forPath(queuePath + "/" + childPath);
                    if (childStat.getCtime() <= maxTime) {
                        deleteCount++;
                        client.delete().forPath(queuePath + "/" + childPath);
                    }
                }
                if (deleteCount >0) {
                    logger.info("{}: ({}) Retention check Queue: {}: {} messages deleted previous than: {} ", 
                            getClass().getSimpleName(), logId, name, deleteCount, new Date(maxTime));
                }
            }
        } catch (Exception e) {
            logger.error("{}: ({}) Cannot delete retained messages", getClass().getSimpleName(), logId,e);
        }
    }

    @Override
    public void childEvent(CuratorFramework client, TreeCacheEvent event) throws Exception {
        if (event.getType() == TreeCacheEvent.Type.NODE_ADDED) {
            readMessages(false, client);
        } else if (event.getType() == TreeCacheEvent.Type.CONNECTION_LOST) {
            this.retentionExecutor.shutdownNow();
        }
    }
  
    protected void readMessages(final boolean onlyMoveOffset, final CuratorFramework client) {
        final Stat parentStat = new Stat();
        final String parentPath = Queues.QUEUE_PREFIX + name;
        try {
            final List<String> childrenNodePaths = client.getChildren()
                    .storingStatIn(parentStat).forPath(parentPath);
            if (!childrenNodePaths.isEmpty() && parentStat.getNumChildren() > 0) {
                // get them all
                List<MessageMetadata> objects = getUnreadMessages(client, parentPath, childrenNodePaths, onlyMoveOffset);
                if (logger.isDebugEnabled() && objects!=null) {
                    logger.debug("{}: ({}) New messages on queue: {} for listener: {}, now: (ever size: {})", 
                            getClass().getSimpleName(), logId, name, userListener.hashCode(), objects.size(), 
                            parentStat.getNumChildren());
                }
                // pass em
                for (final MessageMetadata meta: objects) {
                    userListener.accept(meta);
                }
            } else if (!onlyMoveOffset) {
                logger.error("{}: ({}) Node changed event on Name: {} without children (?) ", 
                        getClass().getSimpleName(), logId, name);
            }
        } catch (IllegalStateException ise) {
            // no problem machine is going down
            logger.warn("{}: ({}) Node going down while consuming topic messages ({})", 
                    getClass().getSimpleName(), logId, ise.getMessage());
        } catch (Exception e) {
            logger.error("{}: ({}) Unexpected error while reading queue: {}, children path: {}", 
                    getClass().getSimpleName(), logId, name, parentPath, e);
        }
    }
    
    private List<MessageMetadata> getUnreadMessages(
            final CuratorFramework client, 
            final String parentPath,
            final List<String> childrenNodePaths, 
            final boolean onlyMoveOffset) throws Exception {
        
        final Stat childStat = new Stat();
        final List<MessageMetadata> datas = new ArrayList<>();
        long lastNodeCtime = 0;
        for (final String childNode: childrenNodePaths) {
            try {
                final byte[] in = client.getData().storingStatIn(childStat).forPath(parentPath + "/" + childNode);
                if (childStat.getDataLength() > 0) {
                    boolean isUnread = childStat.getCtime() > lastCtimeRead;
                    if (isUnread && !onlyMoveOffset) {
                        bytesToMessage(childStat, datas, childNode, in);
                    }
                    if (childStat.getCtime() > lastNodeCtime) {
                        lastNodeCtime = childStat.getCtime(); 
                    }
                }
            } catch (NoNodeException nne) {
                //no problem, ticktime x maxSessionTimeout is greater than my Shard's restart !
                //logger.warn("{}: Nothing found so skipping: {}", getClass().getSimpleName(), parentPath, nne);
            } catch (Exception e) {
                logger.error("{}: ({}) Unexpected while reading paths: {}", getClass().getSimpleName(), logId, 
                        parentPath + "/" + childNode, e);
                
            }
        }
        // default to now when moving offset;
        this.lastCtimeRead = lastNodeCtime > 0 ? lastNodeCtime : System.currentTimeMillis();
        return datas;
    }

    private void bytesToMessage(final Stat childStat, final List<MessageMetadata> datas,
            final String childNode, final byte[] in) {
        try {
            Object o = SerializationUtils.deserialize(in);
            if (o!=null) {
                datas.add((MessageMetadata)o);
            }
        } catch (Exception e) {
            logger.warn("{}: ({}) Deserializing error while reading on queue: {} for "
                    + "new message: {}, size: {}, created: {}", getClass().getSimpleName(), logId, name, 
                    childNode, childStat.getDataLength(), childStat.getCtime());
        }
    }
    
    protected long getLastCtimeRead() {
        return this.lastCtimeRead;
    }

    protected void setLastCtimeRead(long lastCtimeRead) {
        this.lastCtimeRead = lastCtimeRead;
    }

    @Override
    public void run() {
        checkRetention();
    }
    

}