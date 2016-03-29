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

import java.util.concurrent.locks.LockSupport;
import java.util.function.Consumer;

/**
 * Some samples using the facility
 * 
 * @author Cristian Gonzalez
 * @since Oct 9, 2015
 *
 */
public class SpectatorSamples {

    public static void waitLockingProgram() throws InterruptedException {
        /**
         * Curator: inter process mutex
         * Esto corre una llave re-entrante compartida y distribuida
         * Varios nodos pueden pedir la llave y quedan esperando a su liberacion
         * Tipicamente para tareas que comparten recursos que son single-thread
         * 
         * Ejemplo: ?
         */
        Locks locks = new Locks("localhost:2181");
        locks.runWhenUnlocked("truco", getRunnable("envido"));
        locks.runWhenUnlocked("truco", getRunnable("noquiero"));
        locks.runWhenUnlocked("truco", getRunnable("truco"));
        locks.runWhenUnlocked("truco", getRunnable("quierlvale4"));
        locks.close();
    }
    
    public static void raceLockingProgram() {
        /**
         * Curator: inter process mutex
         * Esto corre una llave re-entrante compartida y distribuida
         * Varios nodos pueden pedir la llave, pero SOLO 1 la obtendra
         * Tipico para coordinar actividad de servicios exclusivos durante un tiempo
         * 
         * Ejemplo: crones corriendo la misma tarea, automate ?
         */
        Locks locks = new Locks("localhost:2181");
        
        Thread t1 = new Thread(()->locks.runOnLockRace("ovulo", getRunnable("jaimito")));
        Thread t2 = new Thread(()->locks.runOnLockRace("ovulo", getRunnable("pedrito")));
        t1.start();
        t2.start();
        locks.close();
    }

    public static void highlyAvailableCluster() throws InterruptedException {
        /**
         * Curator: leader latch
         * Esto corre un leader election entre los nodos que se candidatean para una 
         * tarea especial o un servicio redundante en esquema pasivo/activo
         * 
         * Ejemplo: Historical, Tweet deletions, 
         */
        Locks locks = new Locks("localhost:2181");
        locks.runWhenLeader("historical", getServer("server-1"));
        locks.runWhenLeader("historical", getServer("server-2"));
        locks.close();
    }
    
    public static void publishSubscribeQueue() throws InterruptedException {
        /**
         * ZooKeeper
         * Esto corre TreeCache de Curator, y znodes de ZK, para lograr una cola de
         * tipo Publish/Subscribe, donde todos los consumidores reciben los mismos mensajes
         *  
         * Ejemplo: avisarle a los nodos de Torino que refresquen sus reglas
         */
        Queues queues = new Queues("localhost:2181");
        queues.runAsSubscriber("jelinek", getConsumer(), true);
        queues.runAsSubscriber("jelinek", getConsumer(), true);
        queues.postBroadcastMessage("jelinek", "me reventaste la tarjeta");
        queues.postBroadcastMessage("jelinek", "no hay mas guita loca");
        queues.postBroadcastMessage("jelinek", "volve de miami");
        queues.postBroadcastMessage("romay", "larga los pibes");
        queues.close();
    }

    private static Runnable getRunnable(final String name) {
        final String locker = name;
        return new Runnable() {
            @Override
            public void run() {
                System.out.println("lock: " + name + " / con accion: " + locker);
                LockSupport.parkUntil(1000l);
            }
        };
    }

    private static ServerCandidate getServer(final String name) {
        return new ServerCandidate() {
            @Override
            public void start() {
                try {
                    Thread.sleep(1000);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
            @Override
            public void stop() {
                System.out.println("lock: stop");
            }
        };
    }

    private static Consumer<MessageMetadata> getConsumer() {
        return new Consumer<MessageMetadata>() {
            @Override
            public void accept(final MessageMetadata meta) {
                System.out.println(meta.getInbox() + " recibio: " + meta.getPayload());
            }
        };
    }
    
    public static void main(final String[] args) throws Exception {
        //highlyAvailableCluster();        
        raceLockingProgram();
        publishSubscribeQueue();
        // TODO runQueueTopicSample();
        waitLockingProgram();        
        Thread.sleep(1000);
    }
    
}
