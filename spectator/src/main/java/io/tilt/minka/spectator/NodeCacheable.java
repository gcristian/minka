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

import java.util.Collection;
import java.util.function.Consumer;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.SerializationUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.api.transaction.CuratorTransactionResult;
import org.apache.curator.framework.api.transaction.OperationType;
import org.apache.curator.framework.recipes.cache.TreeCache;
import org.apache.curator.framework.recipes.cache.TreeCacheListener;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Handling of TreeCache and creation of ZNodes for creating and updating both Queues and Wells 
 * 
 * @author Cristian Gonzalez
 * @since Dec 24, 2015
 *
 */
public abstract class NodeCacheable extends Spectator {

    private static final Logger logger = LoggerFactory.getLogger(NodeCacheable.class);

    /**
     * Constraint for Payloads for uniqueness across a shared path, topic or queue.
     * @author Cristian Gonzalez
     * @since Jan 20, 2016
     *
     */
    public interface Identifiable {
        String getId();
    }
    
    protected NodeCacheable(Spectator spec) {
        super(spec);
    }
    
    protected NodeCacheable() {
        super();
    }

    protected NodeCacheable(final String zookeeperHostPort, String logId) {
        super(zookeeperHostPort, logId);
    }
    
    protected boolean createPath(final String prefix, final String name, final Object payload) {
        final String subNodeName = payload instanceof Identifiable ? "-" + ((Identifiable)payload).getId() : "";
        return createOrUpdatePath(prefix + name + "/" + subNodeName + System.currentTimeMillis(), name, payload);
    }
    
    protected boolean updatePath(final String prefix, final String name, final Object payload) {
        final String subNodeName = payload instanceof Identifiable ? "/" + ((Identifiable)payload).getId() : "";
        return createOrUpdatePath(prefix + name + subNodeName, name, payload);
    }
    
    private int acc;
    /**
     * Create a child Znode (payload) within another parent (queue) Znode  
     */
    private boolean createOrUpdatePath(final String completeName, final String clientName, final Object payload) {
        final CuratorFramework client = getClient();
        if (client == null) {
            logger.error("{}: ({}) Cannot use Distributed utilities before setting Zookeeper connection", 
                    getClass().getSimpleName(), getLogId());
            return false;
        }
        boolean result = false;
        try {
            //String regenPath = null;
            final byte[] bytes = SerializationUtils.serialize(new MessageMetadata(payload, clientName));
            try {
                acc+=bytes.length;
                if (logger.isDebugEnabled()) {
                    logger.debug("({}) BYTES SENT: {} (total = {})", getLogId(), bytes.length, acc);
                }
                final Collection<CuratorTransactionResult> res = createAndSetZNodeEnsuringPath(
                        CreateMode.PERSISTENT, completeName, 0, bytes);
                return checkTxGeneratedPath(res)!=null;
            } catch (KeeperException e) {
                logger.error("{}: ({}) Unexpected while posting message: {}", getClass().getSimpleName(), getLogId(),
                        completeName, e);
                return false;
            }
        } catch (Exception e) {
            if (isStarted() && isConnected()) {
                logger.error("{}: ({}) Unexpected while posting message: {}", getClass().getSimpleName(), getLogId(),
                        completeName, e);
            } else {
                logger.error("{}: ({}) Zookeeper Disconnection: while posting message: {}", getClass().getSimpleName(), 
                        getLogId(), completeName, e.getMessage());
            }
        }
        return result;
    }
    
    /* zookeeper cannot create paths like mkdir -p so we build a tx to create intermediate folders */
    private Collection<CuratorTransactionResult> createAndSetZNodeEnsuringPath(
            final CreateMode mode,
            final String path, 
            final int oldpos,
            final byte[] bytes) throws Exception {
        
        Collection<CuratorTransactionResult> thisResult, nextResult = null;
        if (!path.isEmpty() && path.length()>1) {
            final int pos = path.indexOf('/', oldpos+1);
            if (pos >= 0) {
                thisResult = createZNode(mode, path.substring(0, pos), bytes);
                nextResult = createAndSetZNodeEnsuringPath(mode, path, pos, bytes);
                return nextResult==null ? thisResult : nextResult;
            } else {
                return createZNode(mode, path, bytes);
            }
        } else {
            return null;
        }
    }

    private Collection<CuratorTransactionResult> createZNode(
            final CreateMode mode, 
            final String path,
            final byte[] bytes) throws Exception {
        try {
            logger.info("{}: ({}) Changing path: {}", getClass().getSimpleName(), getLogId(), path);
            return getClient().inTransaction().setData().forPath(path, bytes).and().commit();
        } catch (KeeperException ke) {
            if (ke.code()==KeeperException.Code.NONODE) {
                logger.info("{}: ({}) Creating parent path for first time: {} because: {}",
                        getClass().getSimpleName(), getLogId(), path, ke.getMessage());
                return getClient().inTransaction().create().withMode(mode).forPath(path, bytes).and().commit();
            } else {
                logger.error("{}: ({}) Unexpected creating node: {}", getClass().getSimpleName(), getLogId(), path);
                throw ke;
            }
        }
    }

    private String checkTxGeneratedPath(Collection<CuratorTransactionResult> txs) {
        String regenPath = null;
        for (CuratorTransactionResult tx : txs) {
            if (tx.getType() == OperationType.CREATE) {
                regenPath = tx.getResultPath();
            } else if (tx.getType() == OperationType.SET_DATA) {
                regenPath = tx.getForPath();
            }
        }
        return regenPath;
    }
    
    protected <T> boolean createCache(
            final String prefix,
            final String name, 
            final Consumer<MessageMetadata>consumer,
            final TreeCacheListener listener) {
        return createCache(prefix, name, consumer, listener, false, 0, 0);
    }

    /* this is a Znode that gets always refreshed (zookeeper watcher) on data change events
     * and new changes on the znode Curator reattaches new watches to get notified, i.e. retriggered  */
    protected <T> boolean createCache(
            final String prefix, 
            final String name,
            final Consumer<MessageMetadata>consumer,
            final TreeCacheListener listener,
            final boolean fromTail, 
            final long sinceTimestamp, 
            final long retentionLapse) {

        TreeCache node = null;
        try {
            final String nodepath = prefix + name;
            final CuratorFramework client = getClient();
            
            if (!isStarted()) {
                logger.error("{}: ({}) Cannot use Distributed utilities before setting Zookeeper connection", 
                        getClass().getSimpleName(), getLogId());
                return false;
            }
            logger.info("{}: ({}) Caching changes on path: {} with listener:{}", getClass().getSimpleName(), getLogId(), 
                    nodepath, consumer.getClass().getSimpleName());
            node = new TreeCache(client, nodepath);
            node.getListenable().addListener(listener);
            add(name, new UserInstanceObject(node, consumer));
            // only to discard messages
            final Stat stat = client.checkExists().forPath(nodepath);
            if (stat != null && stat.getCtime() > 0 && listener instanceof PublishSubscribeQueue) {
                ((PublishSubscribeQueue)listener).readMessages(fromTail, getClient());
            }
            node.start();
            return true;
        } catch (Exception e) {
            if (isStarted()) {
                logger.error("{}: ({}), Unexpected while creating node cache: {} with consumer: {}",  
                        getClass().getSimpleName(), getLogId(), name, consumer.getClass().getSimpleName(), e);
            } else {
                logger.error("{}: ({}), Zookeeper Disconection: Cancelling creating node cache: {} with consumer: {}", 
                        getClass().getSimpleName(), getLogId(), name, consumer.getClass().getSimpleName(), e.getMessage());
            }
            IOUtils.closeQuietly(node); // only if operation fails
        }
        return false;
    }
}
