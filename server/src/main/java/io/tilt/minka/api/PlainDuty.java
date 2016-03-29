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
package io.tilt.minka.api;

import java.io.Serializable;

import org.apache.commons.lang.Validate;
import org.apache.commons.lang.builder.HashCodeBuilder;

import io.tilt.minka.domain.Workload;

/**
 * A plain simple duty that can be stored in maps, sets, compared to others, etc.
 * Instance identity is upon ID param.
 *  
 * @author Cristian Gonzalez
 * @since Jan 5, 2016
 * 
 * @param <T>
 */
public class PlainDuty<T> implements Duty<T>, Serializable {
    
    private static final long serialVersionUID = 4976043821619752116L;
    
    private final String id;
    private final long load;
    private final long maxLoad;
    private final T value;
    private Class<T> type;
    
    /**
     * Create a simple Duty representing an ID without payload or content
     * Duties with same ID will be handled as equals
     * @param id    must be a unique ID within your domain
     * @param load  the work load associated to this duty at execution time
     * @return
     */
    public static PlainDuty<String> build(final String id, final long load) {
        return new PlainDuty<String>(String.class, id, id, load, load);
    }
    
    /**
     * Create a simple Duty representing an ID without payload or content
     * Duties with same ID will be handled as equals
     * @param id    must be a unique ID within your domain
     * @return
     */
    public static PlainDuty<String> build(final String id) {
        return new PlainDuty<String>(String.class, id, id, 1l, 1l);
    }
    
    /** 
     * Create a simple Duty representing an ID without payload or content
     * Duties with same ID will be handled as equals
     * @param id            must be a unique ID within your domain
     * @param load          the work load associated to this duty at execution time
     * @param maxLoad       the maximum work load (limit) that your domains recognizes for a duty
     * @return
     */
    public static PlainDuty<String> build(final String id, final long load, final long maxLoad) {
        return new PlainDuty<String>(String.class, id, id, load, maxLoad);
    }
    
    /**
     * Create a simple Duty with a payload.
     * Such payload is not read by Minka Only de/serialized. 
     * @param type          class object for the payload
     * @param value         the payload traveling from Leader to Follower
     * @param id            must be a unique ID within your domain
     * @param load          the work load associated to this duty at execution time
     * @return
     */
    public static <T> PlainDuty<T> build(
            final Class<T> type, 
            final T value, 
            final String id, 
            final long load) {
        return new PlainDuty<T>(type, value, id, load, load);
    }
    
    /**
     * Create a simple Duty with a payload.
     * Such payload is not read by Minka Only de/serialized. 
     * @param type          class object for the payload
     * @param value         the payload traveling from Leader to Follower
     * @param id            must be a unique ID within your domain
     * @param load          the work load associated to this duty at execution time
     * @param maxLoad       the maximum work load (limit) that your domains recognizes for a duty
     * @return
     */
    public static <T> PlainDuty<T> build(
            final Class<T> type, 
            final T value, 
            final String id, 
            final long load, 
            final long maxLoad) {
        return new PlainDuty<T>(type, value, id, load, maxLoad);
    }
    
    private PlainDuty(final Class<T> type, final T value, final String id, final long load, final long maxLoad) {
        Validate.notNull(type, "You must specify param's class or use overload builder");
        Validate.notNull(value, "You must specify param or use overload builder");
        Validate.notNull(id, "A non null ID is required");
        Validate.isTrue(load>0, "A number greater than 0 expected for workload representing the duty");
        Validate.notNull(maxLoad, "A number greater than 0 expected for the maximum workload (limit) in your domain");
        this.id = id;
        this.load = load;
        this.value = value;
        this.type = type;
        this.maxLoad = maxLoad;
    }

    @SuppressWarnings("rawtypes")
    @Override
    public boolean equals(Object obj) {
        if (obj!=null && obj instanceof PlainDuty) {
            PlainDuty plain = (PlainDuty)obj;
            return getId().equals(plain.getId());
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
    public int compareTo(String o) {
        return id.toString().compareTo(o);
    }

    @Override
    public Class<T> getClassType() {
        return type;
    }


    @Override
    public T get() {
        return value;
    }


    @Override
    public Workload getWeight() {
        return new Workload(load, maxLoad);
    }


    @Override
    public String getId() {
        return id;
    }

}
