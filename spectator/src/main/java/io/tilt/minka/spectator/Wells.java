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
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.function.Consumer;

import org.apache.commons.lang.SerializationUtils;
import org.apache.commons.lang.Validate;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.TreeCacheEvent;
import org.apache.curator.framework.recipes.cache.TreeCacheListener;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Facility to create Wells, which are ZNodes that can -only- be updated, 
 * and hooked to a Listener for its events
 * 
 * @author Cristian Gonzalez
 * @since Ene 16, 2016
 *
 */
public class Wells extends NodeCacheable  {

    private static final Logger logger = LoggerFactory.getLogger(Wells.class);
    public static final String WELL_PREFIX = "/well:";

    public Wells() {
        super();
    }

    public Wells(Spectator spec) {
        super(spec);
    }

    public Wells(final String zookeeperHostPort) {
        super(zookeeperHostPort, null);
    }
    
    public Wells(final String zookeeperHostPort, String logId) {
        super(zookeeperHostPort, logId);
    }
    

    /**
     * Run a consumer when the Well gets updated with new content
     * 
     * @param wellName     a unique name within the ensemble for the updates to arrive
     * @param consumer     a message consumer to be called on new updates
     * @param <T> 		   the type
     * @return             whether or not the subscription succeed
     */
    public <T> boolean runOnUpdate(
            final String wellName, 
            final Consumer<MessageMetadata> consumer) {

        boolean added = false;
        if (!(added = checkExistance(wellName, consumer))) {
            final WellListener event = new WellListener(wellName, consumer);
            added = createCache(WELL_PREFIX, wellName, consumer, event);
        }
        return added;
    }

    /**
     * If you plan to dinamically consume from a lot of Wells, you should release them when no longer care.
     * Otherwise non closed subscription resources will only be released at shutdown 
     * (subject to proper termination)
     * @param wellName	the name of the refreshing well
     */
    public void closeWell(final String wellName) {
        Set<UserInstanceObject> set = getUserMap().get(wellName);
        if (set != null) {
            Iterator<UserInstanceObject> it = set.iterator();
            while (it.hasNext()) {
                UserInstanceObject uio = it.next();
                if (uio.getTree() != null) {
                    uio.getTree().close();
                    it.remove();
                }
            }
        }
    }

    /**
     * Update a well with a message to be received by all Consumers
     * 
     * @param wellName     the well name where the object is send to update
     * @param message       the payload of the message to send
     * @return              whether the message was successfully sent or not
     */
    public boolean updateWell(final String wellName, final Object message) {
        Validate.notNull(wellName, "name of the queue to post to is required !");
        Validate.notNull(message, "message to post is required !");

        return updatePath(WELL_PREFIX, wellName, message);
    }

    protected class WellListener implements TreeCacheListener {
        
        public final String name;
        private final Consumer<MessageMetadata> userListener;
        
        public WellListener(String name, Consumer<MessageMetadata> userListener) {
            super();
            this.name = name;
            this.userListener = userListener;
        }

        @Override
        public void childEvent(CuratorFramework client, TreeCacheEvent event) throws Exception {
            if (event.getType() == TreeCacheEvent.Type.NODE_ADDED || event.getType() == TreeCacheEvent.Type.NODE_UPDATED) {
                readOneMessage(event.getData(), client);
            } else if (event.getType() == TreeCacheEvent.Type.CONNECTION_LOST) {
                // TODO nothing i think
            }
        }    
        
        protected void readOneMessage(final ChildData data, final CuratorFramework client) {
            final String parentPath = Wells.WELL_PREFIX + name;
            try {
                final List<MessageMetadata> datas = new ArrayList<>(); 
                bytesToMessage(data.getStat(), datas, data.getPath(), data.getData());
                if (logger.isDebugEnabled() && datas!=null) {
                    logger.debug("{}: ({}) New update on queue: {} now version: {}", getClass().getSimpleName(), 
                            getLogId(), name, datas.size(), data.getStat().getVersion());
                }
            
                for (final MessageMetadata meta: datas) {
                    userListener.accept(meta);
                }
            } catch (IllegalStateException ise) {
                // no problem machine is going down
                logger.warn("{}: ({}) Node going down while consuming topic messages ({})", 
                        getClass().getSimpleName(), getLogId(), ise.getMessage());
            } catch (Exception e) {
                logger.error("{}: ({}) Unexpected error while reading queue: {}", getClass().getSimpleName(), getLogId(),
                        parentPath, e);
            }
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
                        + "new message: {}, size: {}, created: {}", getClass().getSimpleName(), getLogId(), name, 
                        childNode, childStat.getDataLength(), childStat.getCtime());
            }
        }
    }


}
