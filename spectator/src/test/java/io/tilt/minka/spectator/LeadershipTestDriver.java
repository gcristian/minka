
package io.tilt.minka.spectator;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import junit.framework.Assert;

/**
 * Tests para validar todas las funciones en modo single-node
 * 
 * @author Cristian Gonzalez
 * @since Oct 9, 2015
 *
 */
public class LeadershipTestDriver {

    private static final Logger logger = LoggerFactory.getLogger(LeadershipTestDriver.class);


    public static void test(String message, boolean bool) {
        if (!bool) {
            throw new RuntimeException(message);
        }
        //Assert.assertTrue("
    }
    
    private static int getRandomSizeWithMin(final Random rnd, final int min, final int max) {
        int maxConsumers = 0;
        while ((maxConsumers = rnd.nextInt(max)) < min);
        return maxConsumers;
    }
    
    
    /* candidateo un lider para relizar una tarea, si se corta aparece otro lider */
    // esto usaba el formato de start blockeante, la forma del test no sirve
    @Test
    public void testRestartableServer() throws InterruptedException {
        Random rnd = new Random();
        int servers = 3;
        int check = 0, total = 0;
        List<Candidate> cluster = new ArrayList<>();
        String clusterName = "cluster" + rnd.nextInt();
        for (int i = 0 ; i < servers; i++) {
            total += check = getRandomSizeWithMin(rnd, 3, 7);
            Candidate candidate = new Candidate(clusterName, "servidor-" + (i+1) + "-of-" + servers, check);
            cluster.add(candidate);
            logger.info("Candidating " + candidate.toString());
        }
        Thread.sleep(2000);
        
        boolean anyLeaderAvailable = true;
        while (anyLeaderAvailable) {
            anyLeaderAvailable = false;
            for (Candidate cand: cluster) {
                if (cand.isiAmTheLeader()) {
                    anyLeaderAvailable = true;
                    cand.stop();
                    Thread.sleep(2000);
                    break;
                }                
            }
        }
        
        for (Candidate candidate: cluster) {
            Assert.assertTrue("This node hasnt served: " + candidate.getName(), candidate.iHaveServed());
        }
        System.out.println("asdasd");
    }
    
    
    static class ValueObject {
        public int unsafeCounter;
    }
}
