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
package io.tilt.minka.domain;

import java.io.Serializable;
import java.util.Comparator;

import io.tilt.minka.business.leader.Leader;

/**
 * A workload comparable value to be descending-order sorted at {@link Leader}
 * so it can balance {@link ShardEntity} assignments    
 *  
 * @author Cristian Gonzalez
 * @since Nov 7, 2015
 */
public class Workload implements Comparable<Workload>, Comparator<ShardEntity>, Serializable {

    private static final long serialVersionUID = 2191475545082914908L;
    private Long maxLoad;
	private Long load;
	private WorkloadState workloadState;
	
	public Workload(final Long load) {
		this.load = load;
	}
	
	public Workload(final Long load, final Long maxLoad) {
        this.load = load;
        this.maxLoad = maxLoad;
    }
	
	public static Comparator<ShardEntity> getComparator() {
	    return new Workload(0l);
	}
	
	public Long getLoad() {
		return this.load;
	}

	public void setLoad(long load) {
		this.load = new Long(load);
	}
	
	public Long getMaxLoad() {
        return this.maxLoad;
    }

    public void setMaxLoad(Long max) {
        this.maxLoad = max;
    }

    public void setLoad(Long load) {
        this.load = load;
    }

    enum WorkloadState {
		/* the node cannot receive more sharded entities
		 * it is full of work
		 */
		FULL,
		/* the node can receive more work
		 */
		RECEPTIVE,
		/* the node is idle so it can receive work
		 */
		LOITER,
	}

	@Override
	public int compareTo(Workload o) {
		final int compare = o.getLoad().compareTo(getLoad());
		// when put in sets, identity isnt within the contract as allowed thou unrecommended!
		return compare == 0 ? -1 : compare;
	}

	public WorkloadState getWorkloadState() {
		return this.workloadState;
	}

	public void setWorkloadState(WorkloadState workloadState) {
		this.workloadState = workloadState;
	}
	
	@Override
	public String toString() {
		return "FollowerWorkload: loadSize: " + getLoad() + " state: " + getWorkloadState();
	}

    @Override
    public int compare(final ShardEntity o1, final ShardEntity o2) {
        return o1.getDuty().getWeight().compareTo(o2.getDuty().getWeight());
    }
}
