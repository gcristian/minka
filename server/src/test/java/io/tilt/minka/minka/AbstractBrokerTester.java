/**
TASKS * Copyright (c) 2011-2015 Zauber S.A. -- All rights reserved
 */

package io.tilt.minka.minka;

import static io.tilt.minka.broker.EventBroker.Channel.INSTRUCTIONS_TO_FOLLOWER;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import org.junit.Before;
import org.junit.Test;

import io.tilt.minka.api.Config;
import io.tilt.minka.broker.EventBroker;
import io.tilt.minka.business.LeaderShardContainer;
import io.tilt.minka.business.impl.TransportlessLeaderShardContainer;
import io.tilt.minka.domain.NetworkShardID;
import io.tilt.minka.domain.NetworkShardIDImpl;
import junit.framework.Assert;

public abstract class AbstractBrokerTester {

    private static final int MIN_BROKERS = 50;
    private static final int MIN_PORT_VALUE = 8000;
    private static final int MAX_PORT_VALUE = 65000;
    private CountDownLatch latch;
    private Consumer<Serializable> consumer;
    private AtomicInteger msgAtOrigin;
    private AtomicInteger msgAtDestiny;
    
    @Before
    public void setup() {
        latch = new CountDownLatch(1);
        msgAtOrigin = new AtomicInteger();
        msgAtDestiny = new AtomicInteger();
        consumer = buildConsumer(msgAtDestiny);
    }

    
    @Test
    public void test_three_brokers_leader_follower_type_communication() throws Exception {

        // network communication requires some time
                
        
        final Config configL = buidConfig(22000);
        final NetworkShardIDImpl shardL = new NetworkShardIDImpl(configL);
        
        final Config configF1 = buidConfig(22001);
        final NetworkShardIDImpl shardF1 = new NetworkShardIDImpl(configF1);
        final Config configF2 = buidConfig(22002);
        final NetworkShardIDImpl shardF2 = new NetworkShardIDImpl(configF2);
        
        final LeaderShardContainer container = new TransportlessLeaderShardContainer(shardL);
        container.setNewLeader(shardL);
        
        final EventBroker brokerL = buildBroker(consumer, container, configL, shardL);
        final EventBroker brokerF1 = buildBroker(consumer, container, configF1, shardF1);
        final EventBroker brokerF2 = buildBroker(consumer, container, configF2, shardF2);
        
        Random rnd = new Random();
        for (int i=0;i<=10;i++) {
            test(brokerL, shardF1, configL, rnd.nextInt());
            test(brokerL, shardF1, configL, rnd.nextInt());
            test(brokerL, shardF1, configL, rnd.nextInt());
            test(brokerL, shardF2, configL, rnd.nextInt());
            test(brokerF1, shardL, configF1, rnd.nextInt());
            test(brokerF2, shardL, configF2, rnd.nextInt());
        }
    }
    
    public static class MetaBroker {
        private final Config config;
        private final NetworkShardIDImpl shard;
        private final EventBroker broker;
        
        public MetaBroker(
                final Config config, 
                final NetworkShardIDImpl shard, 
                final EventBroker broker) {
            super();
            this.config = config;
            this.shard = shard;
            this.broker = broker;
        }

        public Config getConfig() {
            return this.config;
        }

        public NetworkShardIDImpl getShard() {
            return this.shard;
        }

        public EventBroker getBroker() {
            return this.broker;
        }
    }
    
    
    @Test
    public void test_n_brokers_full_communication() throws Exception {
        LeaderShardContainer container = null;        
        final List<MetaBroker> brokers = new ArrayList<>();
        final Random rnd = new Random();
        int brokerSize = rnd.nextInt(MIN_BROKERS);
        while ((brokerSize = rnd.nextInt(10))<5);
        
        final Set<Integer> ports = new HashSet<>();
        int port = 0;
        ports.add(port);
        System.out.println("Testing " + brokerSize + " brokers speaking and listening each other");
        for (int i = 0;i<brokerSize;i++) {
            while (!ports.add(port = rnd.nextInt(MAX_PORT_VALUE)) && port<MIN_PORT_VALUE);
            final Config config = buidConfig(port);
            final NetworkShardIDImpl shard = new NetworkShardIDImpl(config);
            if (container == null ) {
                container = new TransportlessLeaderShardContainer(shard);
            }
            brokers.add(new MetaBroker(config, shard, 
                    buildBroker(consumer, container, config, shard)));
            if (rnd.nextBoolean() && container.getLeaderShardId()==null || 
                    container.getLeaderShardId()==null && i+1==brokerSize) {
                container.setNewLeader(shard);
            }
        }
        
        for (MetaBroker source: brokers) {
            for (MetaBroker target: brokers) {
                if (target!=source) {
                    test(source.getBroker(), target.getShard(), source.getConfig(), rnd.nextInt());
                }
            }
        }
    }

    private void test(
            final EventBroker sourceBroker,
            final NetworkShardID targetShard, 
            final Config config, 
            final int integer) throws InterruptedException {
        
        latch = new CountDownLatch(1);
        msgAtOrigin.set(integer);
        msgAtDestiny.set(0);
        
        Assert.assertTrue(sourceBroker.postEvent(
                sourceBroker.buildToTarget(config, INSTRUCTIONS_TO_FOLLOWER, targetShard), msgAtOrigin));
        latch.await(5, TimeUnit.SECONDS); // wait for message to bounce loopback
        Assert.assertEquals(msgAtOrigin.intValue(), msgAtDestiny.get());
    }

    protected abstract EventBroker buildBroker(
            final Consumer<Serializable> driver, 
            final LeaderShardContainer container, 
            final Config config, 
            final NetworkShardID shard);

    protected Consumer<Serializable> buildConsumer(
            final AtomicInteger msgAtDestiny) {
        
        return new Consumer<Serializable>() {
            @Override
            public void accept(Serializable t) { 
                msgAtDestiny.addAndGet(((AtomicInteger)t).get());
                AbstractBrokerTester.this.latch.countDown();
            }
        };
    }

    protected Config buidConfig(int port) {
        Properties propF = new Properties();
        propF.setProperty("brokerServerPort", String.valueOf(port));
        return new Config(propF);
    }
    
}
