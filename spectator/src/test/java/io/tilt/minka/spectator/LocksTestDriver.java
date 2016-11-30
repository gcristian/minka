package io.tilt.minka.spectator;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.tilt.minka.spectator.Locks;

/**
 * Tests para validar todas las funciones en modo single-node
 * 
 * @author Cristian Gonzalez
 * @since Oct 9, 2015
 *
 */
public class LocksTestDriver {

    private static final Logger logger = LoggerFactory.getLogger(LocksTestDriver.class);

    
    /* lanzo varios programas al mismo tiempo pero siempre corren en serie esperando su turno con la llave */

    @Test
    public void testLockWaitProgramTest() throws InterruptedException {
    	Locks locks = new Locks("localhost:2181");
        final ValueObject test = new ValueObject();
        final int max = getRandomSizeWithMin(new Random(), 2, 10);
        List<Thread> parallel = new ArrayList<>();
        for (int i =0; i<max; i++) {
            final int id = i;
            parallel.add(new Thread(()->locks.runWhenUnlocked("scrum-pesado", 
                    new NamedRunnable("roncero-" + id, test))));
        }
        
        for (Thread th: parallel) th.start();
        
        try { // give time
            long innerTime = 100; 
            Thread.sleep((max * innerTime) + innerTime);
        } catch (InterruptedException e) {
            throw e;
        }
        test("No corrieron en distintos momentos ?", test.unsafeCounter == max);
        locks.close();
    }

    public static void test(String message, boolean bool) {
        if (!bool) {
            throw new RuntimeException(message);
        }
        //Assert.assertTrue("
    }
    
    /* lanzo varios programas 'al mismo tiempo' pero solo 1 correra */
    @Test
    public void testRaceLockingProgram() throws InterruptedException {
    	Locks locks = new Locks("localhost:2181");
        final Random rnd = new Random();
        final String lock = "ovulo-" + rnd.nextInt() ;
        final AtomicInteger test = new AtomicInteger();
        int max = getRandomSizeWithMin(rnd, 2, 10);
        List<Thread> parallel = new ArrayList<>();
        for (int i=0; i<max; i++) {
            final int id = i;
            parallel.add(new Thread(()->locks.runOnLockRace(lock, 
                    new NamedRunnable("spermatozoide-" + id +"-of-" + max, test))));
        }
        for (Thread th: parallel) th.start();
        
        Thread.sleep(1000);
        test("Llegaron varios al mismo tiempo (" + test.get() + ")", test.get()==1);
        locks.close();
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
    
    /* clase tonta para darle nombre a una accion que espera 1 segundo - solo para testear */
    static class NamedRunnable implements Runnable {
        private final String name;
        private AtomicInteger counter;
        private ValueObject vo;
        public NamedRunnable(final String name, final ValueObject vo) {
            this.name = name;
            this.vo = vo;
        }
        public NamedRunnable(final String name, final AtomicInteger counter) {
            this.name = name;
            this.counter = counter;
        }
        @Override
        public void run() {            
            try {
                if (this.counter!=null) {
                    log("Race Lock: " + " obtained by: " + this.name);
                    this.counter.incrementAndGet();
                    Thread.sleep(200);
                }
                if (this.vo!=null) {
                    log("Waiting Lock: " + " obtained by: " + this.name);
                    this.vo.unsafeCounter+=1;
                    Thread.sleep(100);
                }
                // less than 50 
                
            } catch (InterruptedException e) {
                e.printStackTrace();
            } 
        }
    }
    
}
