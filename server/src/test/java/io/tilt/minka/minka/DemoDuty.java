/**
 *  Copyright (c) 2011-2015 Zauber S.A.  -- All rights reserved
 */

package io.tilt.minka.minka;

import java.io.Serializable;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.lang.builder.HashCodeBuilder;

import io.tilt.minka.api.Duty;
import io.tilt.minka.domain.Workload;

public class DemoDuty implements Duty<DemoDuty>, Serializable {
    
    private static final int MAX_LOAD_DEMO = 1500; 
    private static final long serialVersionUID = 8906612596789795356L;
    private final Long id;
    private static final AtomicLong sequence = new AtomicLong();
    private final long load;
    
    public DemoDuty() {
        this.id = sequence.incrementAndGet();
        this.load = (long)(new Random().nextInt(MAX_LOAD_DEMO));
    }
    
    public DemoDuty(long id) {
        this.id = id;
        this.load = (long)(new Random().nextInt(MAX_LOAD_DEMO));
    }
    
    @Override
    public int compareTo(String o) {
        return getId().compareTo(o);
    }

    @Override
    public Class<DemoDuty> getClassType() {
        return DemoDuty.class;
    }

    @Override
    public DemoDuty get() {
        return DemoDuty.this;
    }
    
    @Override
    public boolean equals(Object obj) {
        if (obj!=null && obj instanceof DemoDuty) {
            DemoDuty demo = (DemoDuty)obj;
            return getId().equals(demo.getId());
        } else {
            return false;
        }
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder()
            .append(getId())
            .toHashCode();
    }
    
    @Override
    public String toString() {
        return id.toString();
    }
    @Override
    public String getId() {
        return String.format("%02d", id);
        //return id.toString(); 
    }
    @Override
    public Workload getWeight() {
        return new Workload(load, (long)MAX_LOAD_DEMO);
    }
    
}