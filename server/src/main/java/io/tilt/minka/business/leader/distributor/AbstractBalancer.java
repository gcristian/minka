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
package io.tilt.minka.business.leader.distributor;

import static io.tilt.minka.domain.ShardDuty.State.PREPARED;
import static io.tilt.minka.domain.ShardState.ONLINE;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.tilt.minka.api.Config;
import io.tilt.minka.business.leader.PartitionTable;
import io.tilt.minka.domain.DutyEvent;
import io.tilt.minka.domain.Shard;
import io.tilt.minka.domain.ShardDuty;
import io.tilt.minka.domain.ShardDuty.State;
import io.tilt.minka.utils.CircularCollection;

/**
 * Common fixed flow for balancing
 * 
 * @author Cristian Gonzalez
 * @since Ene 4, 2015
 */
public abstract class AbstractBalancer implements Balancer {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    private final Config config;

    protected AbstractBalancer(final Config config) {
        this.config = config;
    }

    /**
     * Implement according these params:
     * 
     * @param table         the current situation 
     * @param realloc          previous allocation to check for anything of interest
     * @param onlineShards  the shards to distribute
     * @param dangling      to treat as creations
     * @param creations     new additions reported from source or added from partition service to distribute
     * @param deletions     already registered: passed only for calculation
     * @param accounted     summarization of already running and stable duties 
     */
    protected abstract void balance(final PartitionTable table, final Reallocation realloc,
            final List<Shard> onlineShards, final Set<ShardDuty> dangling, final Set<ShardDuty> creations,
            final Set<ShardDuty> deletions, final int accounted);

    @Override
    public final Reallocation evaluate(final PartitionTable table, final Reallocation previousChange) {
        final Reallocation realloc = new Reallocation();
        final List<Shard> onlineShards = table.getShardsByState(ONLINE);
        // recently fallen shards
        final Set<ShardDuty> dangling = table.getDutiesDangling();
        registerMissing(table, realloc, table.getDutiesMissing());
        // add previous fallen and never confirmed migrations
        dangling.addAll(restoreUnfinishedBusiness(previousChange));
        final Set<ShardDuty> creations = table.getDutiesCrudByFilters(DutyEvent.CREATE, PREPARED);
        final Set<ShardDuty> deletions = table.getDutiesCrudByFilters(DutyEvent.DELETE, PREPARED);
        final int accounted = table.getAccountConfirmed();
        // 1st step: delete all
        registerDeletions(table, realloc, deletions);
        balance(table, realloc, onlineShards, dangling, creations, deletions, accounted);
        return realloc;
    }

    private void registerMissing(final PartitionTable table, final Reallocation realloc, final Set<ShardDuty> missing) {
        for (final ShardDuty missed : missing) {
            final Shard lazy = table.getDutyLocation(missed);
            logger.info("{}: Registering from {}Shard: {}, a dangling Duty: {}",
                    getClass().getSimpleName(), lazy==null?"fallen ":"", lazy, missed);            
            if (lazy!=null) {
                // missing duties are a confirmation per-se from the very shards,
                // so the ptable gets fixed right away without a realloc.
                missed.registerEvent(DutyEvent.DELETE, State.CONFIRMED);
                table.confirmDutyAboutShard(missed, lazy);
            }
            missed.registerEvent(DutyEvent.CREATE, State.PREPARED);
            table.addCrudDuty(missed);
        }
        if (!missing.isEmpty()) {
            logger.info("{}: Registered {} dangling duties from fallen Shard/s, {}",
                    getClass().getSimpleName(), missing.size(), missing);
        }
        // clear it or nobody will
        missing.clear();
    }

    /* check waiting duties never confirmed (for fallen shards as previous target candidates) */
    protected List<ShardDuty> restoreUnfinishedBusiness(final Reallocation previousChange) {
        List<ShardDuty> unfinishedWaiting = new ArrayList<>();
        if (previousChange != null && !previousChange.isEmpty() && !previousChange.hasCurrentStepFinished()
                && !previousChange.hasFinished()) {
            previousChange.getGroupedIssues().keys()
                    /*.stream() no siempre la NO confirmacion sucede sobre un fallen shard
                    .filter(i->i.getServiceState()==QUITTED)
                    .collect(Collectors.toList())*/
                    .forEach(i -> previousChange.getGroupedIssues().get(i).stream()
                            .filter(j -> j.getState() == State.SENT).forEach(j -> unfinishedWaiting.add(j)));
            if (unfinishedWaiting.isEmpty()) {
                logger.info("{}: Previous change although unfinished hasnt waiting duties",
                        getClass().getSimpleName());
            } else {
                logger.info("{}: Previous change's unfinished business saved as Dangling: {}",
                        getClass().getSimpleName(), unfinishedWaiting.toString());
            }
        }
        return unfinishedWaiting;
    }

    /**
     * put new duties into receptive shards willing to add
     */
    protected void registerCreations(final Set<ShardDuty> duties, final Reallocation realloc,
            CircularCollection<Shard> receptiveCircle) {

        for (ShardDuty duty : duties) {
            Shard target = receptiveCircle.next();
            realloc.addChange(target, duty);
            duty.registerEvent(DutyEvent.ASSIGN, State.PREPARED);
            logger.info("{}: Assigning to shard: {}, duty: {}", getClass().getSimpleName(),
                    target.getShardID(), duty.toBrief());
        }
    }

    /* by user deleted */
    private void registerDeletions(final PartitionTable table, final Reallocation realloc,
            final Set<ShardDuty> deletions) {

        for (final ShardDuty deletion : deletions) {
            Shard shard = table.getDutyLocation(deletion);
            deletion.registerEvent(DutyEvent.UNASSIGN, State.PREPARED);
            realloc.addChange(shard, deletion);
            logger.info("{}: Deleting from: {}, Duty: {}", getClass().getSimpleName(), shard.getShardID(),
                    deletion.toBrief());
        }
    }

    protected Config getConfig() {
        return this.config;
    }

}
