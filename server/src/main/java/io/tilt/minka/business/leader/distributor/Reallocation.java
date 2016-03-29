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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicLong;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.SetMultimap;

import io.tilt.minka.business.leader.PartitionTable;
import io.tilt.minka.domain.DutyEvent;
import io.tilt.minka.domain.Shard;
import io.tilt.minka.domain.ShardDuty;
import io.tilt.minka.domain.ShardDuty.State;

/**
 * A distribution change in progress, created by the {@linkplain Balancer}.
 * analyzing the {@linkplain PartitionTable}.
 * Composed of duties migrations from a shard to another, deletions, creations, etc. 
 * 
 * Such operations takes coordination to avoid parallelism and inconsistencies 
 * while they're yet to confirm, and needs to stay still while shards react, they also may fall.
 * 
 * No new distributions are made while this isn't finished
 * 
 * @author Cristian Gonzalez
 * @since Dec 11, 2015
 */
public class Reallocation implements Comparable<Reallocation> {

    private final Logger logger = LoggerFactory.getLogger(getClass());
    
    private SetMultimap<Shard, ShardDuty> problems;
    private List<SetMultimap<Shard, ShardDuty>> issues;
    private SetMultimap<Shard, ShardDuty> currentGroup;
    private Iterator<SetMultimap<Shard, ShardDuty>> it;
    private final long id;
    private final DateTime creation;
    private int retryCounter;
    
    private static final AtomicLong sequence = new AtomicLong();
    
    private final static int MAX_STEPS = 2;
    private final static int STEP_UNASSIGN = 0;
    private final static int STEP_ASSIGN = 1;
    
    public Reallocation() {
        this.issues = new ArrayList<>(MAX_STEPS);
        this.currentGroup = null;
        this.id = sequence.incrementAndGet();
        this.creation = new DateTime(DateTimeZone.UTC);
        this.problems = HashMultimap.create();
    }
    
    public void incrementRetry() {
        retryCounter++;
    }
    
    public int getRetryCount() {
        return retryCounter;
    }
 
    public void resetIssues() {
        this.issues = new ArrayList<>(MAX_STEPS);
    }
    
    private SetMultimap<Shard, ShardDuty> init(int idx) {
        SetMultimap<Shard, ShardDuty> dutyGroup = issues.size() > idx ? issues.get(idx) : null; 
        if (dutyGroup == null) {
            for (int i = issues.size(); issues.size() < idx; i++) {
                issues.add(i, HashMultimap.create());
            }
            issues.add(idx, dutyGroup = HashMultimap.create());
        }
        return dutyGroup;
    }
    
    public void addChange(final Shard shard, final ShardDuty duty) {
        if (duty.getDutyEvent().is(DutyEvent.CREATE) || duty.getDutyEvent().is(DutyEvent.ASSIGN)) {
            init(STEP_ASSIGN).put(shard, duty);
        } else if (duty.getDutyEvent().is(DutyEvent.DELETE) || duty.getDutyEvent().is(DutyEvent.UNASSIGN)) {
            init(STEP_UNASSIGN).put(shard, duty);
        }
    }
    
    public boolean isEmpty() {
        return issues.isEmpty();
    }
    
    public boolean hasFinished() {
        return it == null || !it.hasNext();
    }
    
    public void nextStep() {
        if (it==null) {
            it = issues.iterator();
        }
        if (it.hasNext()) {
            currentGroup = it.next();
        }
    }
    
    public boolean hasCurrentStepFinished() {
        final Set<ShardDuty> sortedLog = new TreeSet<>();
        for (final Shard shard: currentGroup.keySet()) {
            for (final ShardDuty duty: currentGroup.get(shard)) {
                if (duty.getState()!=State.CONFIRMED) {
                    // TODO get Partition TAble and check if Shard has long fell offline
                    sortedLog.add(duty);
                    logger.info("{}: waiting Shard: {} for at least Duties: {}", getClass().getSimpleName(), shard, 
                            ShardDuty.toStringIds(sortedLog));
                    return false;
                }
            }
        }
        return true;
    }
    
    public DateTime getCreation() {
        return this.creation;
    }

    public SetMultimap<Shard, ShardDuty> getGroupedIssues() {
        return currentGroup;
    }

    public long getId() {
        return this.id;
    }

    public SetMultimap<Shard, ShardDuty> getProblems() {
        return this.problems;
    }

    public void setProblems(SetMultimap<Shard, ShardDuty> problems) {
        this.problems = problems;
    }

    @Override
    public int compareTo(Reallocation o) {
        return o.getCreation().compareTo(getCreation());
    }

}
