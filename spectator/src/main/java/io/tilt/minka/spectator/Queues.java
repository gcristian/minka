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

import java.util.Iterator;
import java.util.Set;
import java.util.function.Consumer;

import org.apache.commons.lang.Validate;

/**
 * Over-curation of apache curator Queueing recipes for over-simplified usage
 * 
 * @author Cristian Gonzalez
 * @since Dec 24, 2015
 *
 */
public class Queues extends NodeCacheable {

    public static final String QUEUE_PREFIX = "/queue:";

    public Queues(Spectator spec) {
        super(spec);
    }

    public Queues(final String zookeeperHostPort) {
        super(zookeeperHostPort, null);
    }
    
    public Queues(final String zookeeperHostPort, String logId) {
        super(zookeeperHostPort, logId);
    }

    /**
     * Subscribe to messages at a Topic kind of queue.
     * 
     * @param topicName     a unique name within the ensemble for the messages to arrive
     * @param consumer      a message consumer to be called on new messages 
     * @param fromTail      option to subscribe to the tail of the queue discarding messages prior to now
     * @param <T>		the type parameter
     * @return              whether or not the subscription succeed
     */
    public <T> boolean runAsSubscriber(
            final String topicName, 
            final Consumer<MessageMetadata> consumer,
            boolean fromTail) {

        boolean added = false;
        if (!(added = checkExistance(topicName, consumer))) {
            final PublishSubscribeQueue listener = new PublishSubscribeQueue(topicName, consumer, 0, getClient(), getLogId());
            added = super.createCache(QUEUE_PREFIX, topicName, consumer, listener, fromTail, 0, 0);
        }
        return added;
    }

    /**
     * Subscribe only to new messages received at a Topic kind of queue.
     * 
     * @param topicName     a unique name within the ensemble for the messages to arrive
     * @param consumer      a message consumer to be called on new messages 
     * @return              whether or not the subscription succeed
     */
    public boolean runAsSubscriber(
            final String topicName, 
            final Consumer<MessageMetadata> consumer) {

        return runAsSubscriber(topicName, consumer, true);
    }

    /**
     * Subscribe onlu to new messages received at a topic kind of queue
     * 
     * @param topicName             queue name
     * @param consumer              listener to call on new messages
     * @param readSinceTimestamp    ignore messages created before this time stamp
     * @param retentionLapse 		the lapse to retent data
     * @return
     */
    public boolean runAsSubscriber(
            final String topicName, 
            final Consumer<MessageMetadata> consumer,
            final long readSinceTimestamp, 
            final long retentionLapse) {

        boolean added = false;
        if (!(added = checkExistance(topicName, consumer))) {
            final PublishSubscribeQueue listener = new PublishSubscribeQueue(topicName, consumer, retentionLapse, 
                    getClient(), getLogId());
            if (readSinceTimestamp > 0) {
                listener.setLastCtimeRead(readSinceTimestamp);
            }
            
            added = createCache(QUEUE_PREFIX, topicName, consumer, listener, false, readSinceTimestamp, retentionLapse);
        }
        return added;
    }

    /**
     * If you plan to dinamically consume from a lot of Queues, you should release them when no longer care.
     * Otherwise non stopped subscription resources will only be released at shutdown 
     * (subject to proper termination)
     * @param topicName	the name of the virtual topic
     */
    public void stopSubscription(final String topicName) {
        Set<UserInstanceObject> set = getUserMap().get(topicName);
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
     * Post a message to be published to all subscribers on the topic
     * 
     * @param topicName     the topic name where the message is send
     * @param message       the payload of the message to send
     * @return              whether the message was successfully sent or not
     */
    public boolean postBroadcastMessage(final String topicName, final Object message) {
        Validate.notNull(topicName, "name of the queue to post to is required !");
        Validate.notNull(message, "message to post is required !");

        return createPath(QUEUE_PREFIX, topicName, message);
    }

}
