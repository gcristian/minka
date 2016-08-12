/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package io.tilt.minka.core.impl;

import java.util.concurrent.locks.ReentrantLock;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.tilt.minka.core.Service;

/**
 * Facility to avoid concurrency on services
 * 
 * @author Cristian Gonzalez
 * @since Dec 3, 2015
 *
 */
public abstract class ServiceImpl implements Service {

	public final Logger logger = LoggerFactory.getLogger(Service.class);

	private final DateTime creation;
	private DateTime start;
	private DateTime stop;
	private State state;

	private final ReentrantLock singleLock = new ReentrantLock();
	private boolean inService;

	public ServiceImpl() {
		state = State.INITIALIZING;
		this.creation = new DateTime(DateTimeZone.UTC);
	}

	public final void init() {
		if ((singleLock.tryLock() || start != null)) {//&& !inService()) {
			try {
				logger.info("{}: Starting service", getClass().getSimpleName());
				this.start = new DateTime(DateTimeZone.UTC);
				state = State.STARTING;
				start();
				state = State.STARTED;
				this.inService = true;
				logger.info("{}: Service start took: {} msec", getClass().getSimpleName(), getDiff(this.start));
			} catch (Exception e) {
				state = State.INIT_ERROR;
				logger.error("{}: Unexpected at service start(): ", getClass().getSimpleName(), e);
				start = null;
			} finally {
				singleLock.unlock();
			}
		} else {
			logger.error("{}: service already started !!", getClass().getSimpleName());
		}
	}

	/* mostly callable from spring */
	public void destroy() {
		state = State.STOPPING;
		if ((singleLock.tryLock() || stop != null)) {// && inService()) {
			try {
				logger.info("{}: Shutting down service", getClass().getSimpleName());
				this.stop = new DateTime(DateTimeZone.UTC);
				stop();
				state = State.STOPPED;
				this.inService = false;
				logger.info("{}: Service shutdown took: {} msec", getClass().getSimpleName(), getDiff(this.stop));
			} catch (Exception e) {
				state = State.DESTROY_ERROR;
				logger.error("{}: Unexpected at service stop(): ", getClass().getSimpleName(), e);
				stop = null;
			} finally {
				singleLock.unlock();
				if (state == State.STOPPED) {
					state = State.DESTROYED;
				}
			}
		} else {
			logger.error("{}: service already stopped !! {}", getClass().getSimpleName(),
				inService() ? "" : "(actually never serviced !!)");
		}
	}
	
	public String getName() {
		return getClass().getSimpleName();
	}

	@Override
	public State getState() {
		return state;
	}

	/* callable from here only */
	public void start() {
		logger.warn("{}: Service without start implementation !", getClass().getSimpleName());
	}

	/* callable from here only */
	public void stop() {
		logger.warn("{}: Service without stop implementation !", getClass().getSimpleName());
	}

	public boolean inService() {
		return inService;
	}

	public long getMillisSinceCreation() {
		return getDiff(creation);
	}

	public long getMillisUntilService() {
		return getDiff(start);
	}

	private long getDiff(DateTime dt) {
		return new DateTime(DateTimeZone.UTC).minus(dt.getMillis()).getMillis();
	}

	public DateTime getCreation() {
		return this.creation;
	}

	public DateTime getStart() {
		return this.start;
	}

	public DateTime getStop() {
		return this.stop;
	}
}
