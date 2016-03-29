/**
 *  Copyright (c) 2011-2015 Zauber S.A.  -- All rights reserved
 */

package io.tilt.minka;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Set;
import java.util.TreeSet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.tilt.minka.api.Duty;
import io.tilt.minka.api.PartitionMaster;
import io.tilt.minka.api.PartitionService;
import jersey.repackaged.com.google.common.collect.Sets;

public class Delegado implements PartitionMaster<DemoDuty>, Serializable {

    private static final long serialVersionUID = 1L;
    private final Logger logger = LoggerFactory.getLogger(getClass());
    
    private Set<Duty<DemoDuty>> allOriginalDuties = new HashSet<>();
    private Set<Duty<DemoDuty>> runningDuties = Sets.newHashSet();
    
	private static int TOTAL_TASKS = 20;
	private PartitionService partitionService;
	private String id="{NN}";
	 
	public Delegado() {
        super();   
        allOriginalDuties = new HashSet<>();
        for (int i=0;i<TOTAL_TASKS;i++) {
            allOriginalDuties.add(new DemoDuty(i));
        }
    }

	
    protected PartitionService getPartitionService() {
        return this.partitionService;
    }


    public void setPartitionService(PartitionService partitionService) {
        this.partitionService = partitionService;
        this.id = partitionService.getShardIdentity();
    }


    @Override
	public void take(Set<Duty<DemoDuty>> entities) {
        logger.info("{} taking: {}+ ({})", id, entities.size(), toStringIds(entities));
		if (runningDuties == null) {
			runningDuties = new HashSet<>();
		}
		runningDuties.addAll(entities);
	}

	@Override
	public void update(Set<Duty<DemoDuty>> entities) {
	    logger.info("{} updating: {} ({})", id, entities.size(), toStringIds(entities));
	}

	private String toStringIds(Set<Duty<DemoDuty>> entities) {	    
	    final Set<Long> set = new TreeSet<>();
	    entities.forEach(e->set.add(Long.parseLong(e.getId())));
	    final StringBuilder sb = new StringBuilder();
	    set.forEach(e->sb.append(e).append(", "));
	    return sb.toString();
	}
	
	@Override
	public void release(Set<Duty<DemoDuty>> entities) {
	    logger.info("{} releasing: -{} ({})", id, entities.size(), toStringIds(entities));
		runningDuties.removeAll(entities);
	}

	int lastSize;
	long lastPrint;
	@Override
	public Set<Duty<DemoDuty>> reportTaken() {
	    long now = System.currentTimeMillis();
	    if (lastSize!=runningDuties.size() || now-lastPrint>20*1000) {
	        lastPrint = now;
	        lastSize = runningDuties.size();
	        logger.info("{} running: {}{} ({})", id, this.runningDuties.size(), 
	                partitionService.isCurrentLeader()? "*":"", toStringIds(runningDuties));
	    }
		return this.runningDuties;
	}

	@Override
	public Set<Duty<DemoDuty>> reportTotal() {
		return allOriginalDuties;
	}

	@Override
	public void activate() {
		logger.info("activating");
	}

	@Override
	public void deactivate() {
	    logger.info("de-activating");
	}

    @Override
    public void receive(Set<Duty<DemoDuty>> duty, Serializable clientPayload) {
        logger.info("receiving");
    }

    @Override
    public boolean isReady() {
        logger.info("delegado is ready");
        return true;
    }
}