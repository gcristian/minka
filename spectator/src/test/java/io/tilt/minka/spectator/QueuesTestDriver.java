/**
 *  Copyright (c) 2011-2015 Zauber S.A.  -- All rights reserved
 */

package io.tilt.minka.spectator;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.function.Consumer;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.tilt.minka.spectator.MessageMetadata;
import io.tilt.minka.spectator.Queues;
import io.tilt.minka.spectator.Wells;

/**
 * Tests para validar todas las funciones en modo single-node
 * 
 * @author Cristian Gonzalez
 * @since Oct 9, 2015
 *
 */
public class QueuesTestDriver {

    private static final String A_TARAMBA = "a/tarambas";
    private static final Logger logger = LoggerFactory.getLogger(QueuesTestDriver.class);
    
    public static void test(String message, boolean bool) {
        if (!bool) {
            throw new RuntimeException(message);
        }
        //Assert.assertTrue("
    }
    
    @Test
    public void runQueueBroadcastTest() {
        Queues queues = new Queues("localhost:2181");
        String queueName = "cirio/mamasita";
        queues.postBroadcastMessage(queueName, "hola bebe llevame a pasear");
        queues.close();
    }

    @Test
    public void testQueuePublishSubscribe() throws InterruptedException {
        Queues queues = new Queues("localhost:2181");
    	
        Random rnd = new Random();
        String queue = String.valueOf(rnd.nextInt());
        int maxConsumers = getRandomSizeWithMin(rnd, 2, 5);
        List<NamedConsumer> ncs = new ArrayList<>(); 
        for (int i =0; i<maxConsumers; i++) {
            final int id = i;
            NamedConsumer nc = new NamedConsumer("consumer-" + id);
            queues.runAsSubscriber(queue, nc, true);
            ncs.add(nc);
        }
        
        int maxMessages = getRandomSizeWithMin(rnd, 1, 30);
        List<String> msgs = new ArrayList<>();
        for (int i =0; i<maxMessages; i++) {
            String msg = "message-no-" + i;
            queues.postBroadcastMessage(queue, msg);
            msgs.add(msg);
        }
        
        Thread.sleep(100);
        
        for (NamedConsumer nc: ncs) {
            test("not all messages were consumed by: " + nc.name, nc.getConsumedMessages().size()==maxMessages);
            for (String msgSent: msgs) {
                boolean fine = false;
                for (Object msgReceived: nc.getConsumedMessages()) {
                    fine |= msgReceived.equals(msgSent);
                }
                test("there're messages not consumed: " + msgSent + " by: " + nc.name, fine);
            }
        }
        queues.close();
    }
    
    @Test
    public void testWells() throws InterruptedException {
        final Wells w = new Wells("localhost:2181");
        w.runOnUpdate(A_TARAMBA, new Consumer<MessageMetadata>() {
            @Override
            public void accept(MessageMetadata t) {
                System.out.println(t.getPayload().toString());
            }
        });
        w.updateWell(A_TARAMBA, "santana");
        Thread.sleep(10000);
        w.closeWell(A_TARAMBA);
    }

    private static int getRandomSizeWithMin(final Random rnd, final int min, final int max) {
        int maxConsumers = 0;
        while ((maxConsumers = rnd.nextInt(max)) < min);
        return maxConsumers;
    }
    
    
    
    public static void log(String message) {
    	logger.info(message);
        //System.out.println("Thread: " + Thread.currentThread().hashCode() + " - " + message);
    }
    
    static class ValueObject {
        public int unsafeCounter;
    }
    

    /* clase tonta para darle nombre a un consumidor - solo para testear */
    static class NamedConsumer implements Consumer<MessageMetadata> {
        private final String name;
        private List<Object> consumedMessages;
        
        public NamedConsumer(final String name) {
            this.name = name;
            this.consumedMessages = new ArrayList<>();
        }        
        @Override
        public void accept(final MessageMetadata meta) {
            log("Queue: " + meta.getInbox() + " received: \"" + meta.toString() + "\" by consumer: " + this.name);
            this.consumedMessages.add(meta.getPayload());
        }

        protected List<Object> getConsumedMessages() {
            return this.consumedMessages;
        }
    }
    
}
