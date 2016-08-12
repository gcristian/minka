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
package io.tilt.minka.business;

import java.io.Closeable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Facility to avoid concurrency on services
 * 
 * @author Cristian Gonzalez
 * @since Dec 3, 2015
 *
 */
public interface Service extends Closeable {

    final Logger logger = LoggerFactory.getLogger(Service.class);

    default void init() {
        logger.warn("{}: Default init without control !", getClass().getSimpleName());
        start();
    }
    
    default void destroy() {
        logger.warn("{}: Default destroy without control !", getClass().getSimpleName());
        stop();
    }
    	
    /* callable from here only */
    abstract void start();
    
    /* callable from here only */    
    abstract void stop();
    
    default void close() {
        stop();
    }
    
    boolean inService();
    
    State getState();
    
    enum State {
        INITIALIZING,
        INIT_ERROR,
        STARTING,
        STARTED,
        STOPPING,
        STOPPED,
        DESTROYED,
        DESTROY_ERROR
    }

}
