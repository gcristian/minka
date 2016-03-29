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
package io.tilt.minka.business.leader;

import static io.tilt.minka.domain.DutyEvent.CREATE;
import static io.tilt.minka.domain.DutyEvent.FINALIZED;
import static io.tilt.minka.domain.DutyEvent.UPDATE;
import static io.tilt.minka.domain.ShardDuty.State.CONFIRMED;
import static io.tilt.minka.domain.ShardDuty.State.DANGLING;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicInteger;

import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.tilt.minka.api.Duty;
import io.tilt.minka.business.Coordinator;
import io.tilt.minka.business.Semaphore.Action;
import io.tilt.minka.business.leader.PartitionTable.ClusterHealth;
import io.tilt.minka.business.leader.distributor.Reallocation;
import io.tilt.minka.domain.DutyEvent;
import io.tilt.minka.domain.Heartbeat;
import io.tilt.minka.domain.Shard;
import io.tilt.minka.domain.ShardDuty;
import io.tilt.minka.domain.ShardState;
import io.tilt.minka.utils.SlidingSortedSet;

/**
 * Maintainer and only writer of {@linkplain PartitionTable} 
 * Accounts coming heartbeats, detects 
 * Only write-access to 
 * 
 * @author Cristian Gonzalez
 * @since Jan 4, 2016
 */
public class Auditor {

    private final Logger logger = LoggerFactory.getLogger(getClass());
    
    // TODO : este valor -para no generar inconcistencias- lo puedo y debo calcular dinamicamente 
    private static final int MAX_EVENT_DATE_FOR_DIRTY = 10000;

    private final PartitionTable partitionTable;
    private Reallocation currentReallocation;
    private SlidingSortedSet<Reallocation> history;
    private final Coordinator coordinator;
    
    public Auditor(final PartitionTable partitionTable, final Coordinator coordinator) {
        this.partitionTable = partitionTable;
        this.currentReallocation = new Reallocation();
        this.history = new SlidingSortedSet<>(20);
        this.coordinator = coordinator;
    }
    
    public List<Reallocation> getHistory() {
        return this.history.values();
    }

    public Reallocation getCurrentReallocation() {
        return this.currentReallocation;
    }

    public void addReallocation(Reallocation change) {
        this.currentReallocation = change;
        this.history.add(change);
    }


    /**
     * TODO: sopesar los HBs en una ventana de tiempo para evitar 
     * inconsistencias de configuracion y que c/u no sea impactante para la tabla
     * - agregar validaciones para poner en REBELL al shard
     * por ejemplo hoy los HBs pueden venir con distinto contenido en fila y no lo chekeo
     * osea 1ro con 10 taras, 2do con 9 tareas, 3ro con 10 tareas, etc: eso no lo chekeo  
     */
    public void account(final Heartbeat hb, final Shard shard) {
        if (hb.getStateChange() == ShardState.QUITTED) {
            checkShardChangingState(shard);
            return;
        }
        shard.addHeartbeat(hb);
        if (getCurrentReallocation().isEmpty()) {
            // believe only when online: to avoid Dirty efects after follower's hangs/stucks
            // so it clears itself before trusting their HBs
            //if (shard.getState()==ShardState.ONLINE || shard.getState() == ShardState.JOINING) {
                for (final ShardDuty duty: hb.getDuties()) {
                    if (duty.getState() == CONFIRMED) {
                        try {
                            partitionTable.confirmDutyAboutShard(duty, shard);
                        } catch (ConcurrentDutyException cde) {
                            if (partitionTable.getHealth()==ClusterHealth.STABLE) {
                                // TODO 
                            }
                            logger.error("{}: Rebell shard: {}", getClass().getSimpleName(), shard, cde); 
                        }
                    } else if (duty.getState() == DANGLING) {
                        logger.error("{}: Shard {} reported Dangling Duty (follower's unconfident: {}): {}", 
                                getClass().getSimpleName(), shard.getShardID(), duty.getDutyEvent(), duty);
                        // TODO : x ahora no va a pasar nada
                        // luego tengo q expirar el evento para no generar falsa alarma
                        // o ver de re-"ejecutar" la tarea si continua existiendo
                        // o enviar la baja sin instruir al delegado para que el follower no moleste
                        if (duty.getDutyEvent().is(DutyEvent.CREATE)) {
                        } else if (duty.getDutyEvent().is(DutyEvent.DELETE)) {                    
                        }
                    }
                }
            //}
        } else {
            analyzeReportedDuties(shard, hb.getDuties());
        }
        // TODO perhaps in presence of Reallocation not ?
        if (shard.getState().isAlive()) {
            declareHeartbeatAbsencesAsMissing(shard, hb.getDuties());
        }
    }
    
    private void analyzeReportedDuties(
            final Shard shard, final List<ShardDuty> heartbeatDuties) {

        final Reallocation currentRealloc = getCurrentReallocation();
        final Set<ShardDuty> currentChanges = currentRealloc.getGroupedIssues().get(shard);
        
        confirmReallocated(shard, heartbeatDuties, currentChanges);
        confirmAbsences(shard, heartbeatDuties, currentChanges);
        
        if (currentRealloc.hasCurrentStepFinished() && currentRealloc.hasFinished()) {
            logger.info("{}: Resetting (all duties confirmed)", getClass().getSimpleName());
            currentRealloc.resetIssues();
        } else if (currentRealloc.hasCurrentStepFinished()) {
            coordinator.forward(coordinator.get(Action.DISTRIBUTOR_PERIODIC));
        } else {
            
            /*logger.info("{}: Still unfinished ! ({})", 
                    getClass().getSimpleName(), troubling.toString());*/
        }

    }
    
    /* this checks partition table looking for missing duties (not declared dangling, that's diff) */
    private void declareHeartbeatAbsencesAsMissing(
            final Shard shard, 
            final List<ShardDuty> heartbeatDuties) {
        
        // TODO : fix in 1 HB this aint possible
        final Set<ShardDuty> sortedLog = new TreeSet<>();
        final Map<String, AtomicInteger> absencesPerHeartbeat = new HashMap<>();
        //final int maxTolerance = 2; // how many erroneous HBs ?
        for (final ShardDuty duty: partitionTable.getDutiesByShard(shard)) {
            boolean found = false;
            for (ShardDuty hbDuty: heartbeatDuties) {
                if (duty.equals(hbDuty)) {
                    found = true;
                    break;
                }
            }
            if (!found) {
                AtomicInteger ai = absencesPerHeartbeat.get(duty.getDuty().getId());
                if (ai == null) {
                    absencesPerHeartbeat.put(duty.getDuty().getId(), ai = new AtomicInteger(0));
                }
                //if (ai.incrementAndGet()>maxTolerance) {
                    sortedLog.add(duty);
                    partitionTable.getDutiesMissing().add(duty);
                //}
            }
        }
        if (!sortedLog.isEmpty()) {
            logger.warn("{}: ShardID: {}, Missing Duties in heartbeat {}, Added to PartitionTable", 
                getClass().getSimpleName(), shard.getShardID(), ShardDuty.toStringIds(sortedLog));
        }
    }

    /* find the up-coming */
    private void confirmReallocated(
            final Shard shard, 
            final List<ShardDuty> heartbeatDuties, 
            final Set<ShardDuty> currentChanges) {
        
        final Set<ShardDuty> sortedLogConfirmed = new TreeSet<>();
        final Set<ShardDuty> sortedLogDirty = new TreeSet<>();
        for (final ShardDuty heartbeatDuty: heartbeatDuties) {
            for (ShardDuty prescriptedDuty: currentChanges) {
                if (prescriptedDuty.equals(heartbeatDuty)) {
                    if (heartbeatDuty.getState()==CONFIRMED
                            && prescriptedDuty.getDutyEvent()==heartbeatDuty.getDutyEvent()) {    
                        sortedLogConfirmed.add(heartbeatDuty);
                        // remove the one holding older State
                        prescriptedDuty.registerEvent(CONFIRMED);
                        currentChanges.remove(prescriptedDuty);
                        partitionTable.confirmDutyAboutShard(heartbeatDuty, shard);
                        break;
                    } else {
                        final DateTime fact = prescriptedDuty.getEventDateForState(prescriptedDuty.getState());
                        final long now = System.currentTimeMillis();
                        if (now-fact.getMillis() > MAX_EVENT_DATE_FOR_DIRTY) {
                            sortedLogDirty.add(heartbeatDuty);
                        }
                    }
                }
            }
        }
        if (!sortedLogConfirmed.isEmpty()) {
            logger.info("{}: ShardID: {}, Confirming partition event for Duties: {}", 
                getClass().getSimpleName(), shard.getShardID(), ShardDuty.toStringIds(sortedLogConfirmed));
        }
        if (!sortedLogDirty.isEmpty()) {
            logger.warn("{}: ShardID: {}, Reporting DIRTY partition event for Duties: {}", 
                getClass().getSimpleName(), shard.getShardID(), ShardDuty.toStringIds(sortedLogDirty));
        }
    }

    /* check un-coming as unassign */
    private void confirmAbsences(
            final Shard shard, 
            final List<ShardDuty> heartbeatDuties, 
            final Set<ShardDuty> currentChanges) {
        
        final Iterator<ShardDuty> it = currentChanges.iterator();
        final Set<ShardDuty> sortedLog = new TreeSet<>();
        while (it.hasNext()) {
            final ShardDuty reallocatedDuty = it.next();
            if ((reallocatedDuty.getDutyEvent().is(DutyEvent.UNASSIGN) || 
                    reallocatedDuty.getDutyEvent().is(DutyEvent.DELETE))
                && !heartbeatDuties.stream().anyMatch(i-> i.equals(reallocatedDuty) 
                && i.getDutyEvent()!=reallocatedDuty.getDutyEvent())) {
                sortedLog.add(reallocatedDuty);
                reallocatedDuty.registerEvent(CONFIRMED);
                partitionTable.confirmDutyAboutShard(reallocatedDuty, shard);
                it.remove();
            }
        }
        if (!sortedLog.isEmpty()) {
            logger.info("{}: ShardID: {}, Confirming (by absence) partioning event for Duties: {}", 
                getClass().getSimpleName(), shard.getShardID(), ShardDuty.toStringIds(sortedLog));
        }
    }

    public void checkShardChangingState( final Shard shard) {
        switch (shard.getState()) {
            case GONE:
            case QUITTED:
                //if (getCurrentReallocation().isEmpty()) {
                    recoverAndRetire(shard);
                //} else {
                    // TODO esta situacion estaba contemplada pero aparecio un bug en 
                    // el manejo del transporte por el cambio de broker que permite
                    // resetear el realloc cuando se cayo un shard, antes no pasaba !!!!
                //}
                break;
            case ONLINE:
                // TODO get ready
                break;
            case QUARANTINE:
                // TODO lot of consistency checks here on duties 
                // to avoid chain of shit from heartbeats reporting doubtful stuff 
                break;
            default:
                break;
        }
    }

    /* dangling duties are set as already confirmed, change wont wait for this to be confirmed */
    private void recoverAndRetire(final Shard shard) {
        final Set<ShardDuty> dangling = new TreeSet<>();
        final Set<ShardDuty> fallenDuties = partitionTable.getDutiesByShard(shard);
        dangling.addAll(fallenDuties);
        
        logger.info("{}: Saved from fallen Shard: {}, {} Duties: {}", 
                getClass().getSimpleName(), shard, dangling.size(), ShardDuty.toStringIds(dangling));
        
        logger.info("{}: Removing Shard: {} from partition table", getClass().getSimpleName(), shard);
        partitionTable.removeShard(shard);
        partitionTable.getDutiesDangling().addAll(dangling);
    }

    private boolean presentInPartition(final ShardDuty duty) {
        final Shard shardLocation = partitionTable.getDutyLocation(duty);
        return shardLocation !=null && shardLocation.getState().isAlive();
    }
    
    public void cleanTemporaryDuties() {
        partitionTable.removeCrudDuties();
        partitionTable.getDutiesDangling().clear();
    }

    public void removeAndRegisterCruds(final List<Duty<?>> dutiesFromSource) {
        final Set<ShardDuty> sortedLog = new TreeSet<>();
        final Iterator<Duty<?>> it = dutiesFromSource.iterator();
        while (it.hasNext()) {
            final ShardDuty potential = ShardDuty.create(it.next());
            if (presentInPartition(potential)) {
                sortedLog.add(potential);
                it.remove();
            } else {
                partitionTable.addCrudDuty(potential);
            }
        }
        if (!sortedLog.isEmpty()) {
            logger.info("{}: Skipping Crud Duty already in Partition Table: {}", 
                getClass().getSimpleName(), ShardDuty.toStringIds(sortedLog));
        }
    }

    /**
     * Check valid actions to client sent duties, 
     * according their action and the current partition table
     * 
     * @param dutiesFromAction
     */
    public void registerCrudThruCheck(ShardDuty ...dutiesFromAction) {
        for (final ShardDuty duty: dutiesFromAction) {
            final boolean found = presentInPartition(duty);
            final DutyEvent event = duty.getDutyEvent();
            if (event.isCrud() && (event==CREATE)) {
                if (!found) {
                    logger.info("{}: Registering Crud Duty: {}", getClass().getSimpleName(), duty);
                    partitionTable.addCrudDuty(duty);
                } else {
                    logger.warn("{}: Skipping Crud Event {} already in Partition Table: {}", 
                            getClass().getSimpleName(), event, duty);
                }
            } else if (event.isCrud() && (event==DutyEvent.DELETE || event==FINALIZED || event== UPDATE )) {
                if (found) {
                    logger.info("{}: Registering Crud Duty: {}", getClass().getSimpleName(), duty);
                    partitionTable.addCrudDuty(duty);
                } else {
                    logger.warn("{}: Skipping Crud Event {} Not Found in Partition Table: {}", 
                        getClass().getSimpleName(), event, duty);
                }                   
            } else if (!event.isCrud()) {
                throw new RuntimeException("Bad call");
            }
        }
    }

}
