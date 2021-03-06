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
package io.tilt.minka.core.task.impl;

import static io.tilt.minka.core.task.Semaphore.Action.ANY;
import static io.tilt.minka.core.task.Semaphore.Hierarchy.CHILD;
import static io.tilt.minka.core.task.Semaphore.Hierarchy.PARENT;
import static io.tilt.minka.core.task.Semaphore.Hierarchy.SIBLING;
import static io.tilt.minka.core.task.Semaphore.Permission.DENIED;
import static io.tilt.minka.core.task.Semaphore.Permission.GRANTED;
import static io.tilt.minka.core.task.Semaphore.Permission.RETRY;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.commons.lang.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.util.concurrent.CycleDetectingLockFactory;
import com.google.common.util.concurrent.CycleDetectingLockFactory.Policies;

import io.tilt.minka.api.Config;
import io.tilt.minka.core.task.Semaphore;
import io.tilt.minka.core.task.Service;
import io.tilt.minka.spectator.Locks;

/**
 * @author Cristian Gonzalez
 * @since Dec 28, 2015
 */
public class SemaphoreImpl implements Service, Semaphore {

	private final Logger logger = LoggerFactory.getLogger(getClass());

	private static final long DISTRIBUTED_LOCK_WAIT_MS = 1000l;

	/* for local scope actions */
	private Map<Action, ReentrantLock> locksByAction;
	private final Map<Action, Rule> rules;

	/* for global scope actions */
	private Locks locks;
	private final Config config;
	private final SpectatorSupplier supplier;
	private final String logName;

	public SemaphoreImpl(final Config config, final SpectatorSupplier supplier, final String logName) {
		this.config = config;
		this.logName = logName;
		this.supplier = supplier;
		
		
		final List<Rule> ruless = getLockingRules();
		this.rules = new HashMap<>(ruless.size());
		ruless.forEach(rule -> this.rules.put(rule.getAction(), rule));
		locksByAction = new HashMap<>();
		final CycleDetectingLockFactory lockFactory = CycleDetectingLockFactory.newInstance(Policies.DISABLED);
		for (final Action key : Action.values()) {
			final ReentrantLock lock = lockFactory.newReentrantLock(key.toString());
			logger.debug("{}: ({}) Creating lock: {} for Action: {}", SemaphoreImpl.class.getSimpleName(), logName,
				lock.getClass().getSimpleName(), key);
			locksByAction.put(key, lock);
			//locksByAction.put(key, new ReentrantLock());
		}
	}

	@Override
	public void stop() {
		this.locks.close();
	}

	@Override
	public void start() {
		this.locks = new Locks(supplier.get());
	}

	@Override
	public Permission acquireBlocking(final Action ia) {
		return acquire_(ia, true);
	}

	@Override
	public synchronized Permission acquire(final Action e) {
		return acquire_(e, false);
	}

	private synchronized Permission acquire_(final Action action, boolean blockThread) {
		Validate.notNull(action);
		// para los medium blocking terminaba roto
		Permission ret = blockThread ? null : checkState(action);
		if (ret == null) {
			final Rule rule = rules.get(action);
			final List<Action> fellows = rule.getRelated(SIBLING);
			if (fellows == null || fellows.isEmpty()) {
				ret = lock(action, blockThread, action) ? GRANTED : RETRY;
			} else {
				ret = lockRelated(action, blockThread, fellows) && lock(action, blockThread, null) ? GRANTED : RETRY;
			}
		}
		return ret;
	}

	private Permission checkState(final Action action) {
		if (action.getScope() == Scope.LOCAL) {
			final ReentrantLock lock = g(action);
			if (lock.isHeldByCurrentThread()) {
				logger.error("{}: ({}) {} lock already acquired by YOU ! CHECK YOUR CODE !", SemaphoreImpl.class.getSimpleName(),
					logName, action, new LockerCollideFakeException());
				return DENIED;
				//throw new RuntimeException("Come on check your code consistency pal !");
			} else if (lock.isLocked()) {
				logger.error("{}: ({}) {} lock already acquired by {} thread ! and holds: {} more", SemaphoreImpl.class.getSimpleName(), 
					logName, action, lock.isHeldByCurrentThread() ? "current" : "may be sibling?", lock.getQueueLength(), 
							new LockerCollideFakeException());
				return DENIED;
			}
		} else if (action.getScope() == Scope.GLOBAL) {
			// TODO
		}

		// in the case of not being slave to those master to me or any (got that?)
		// i simply let them acquire, otherwise behead it !
		for (final Entry<Action, Rule> entry : rules.entrySet()) {
			final List<Action> related = entry.getValue().getRelated(PARENT);
			if (!related.isEmpty()) {
				if (isLocked(entry.getKey()) && (related.contains(ANY) || related.contains(action))) {
					if (!rules.get(action).getRelated(CHILD).contains(entry.getKey())) {
						logger.warn("{}: ({}) {} Cannot be acquired because not Child of previously acquired Parent lock: {}",
							SemaphoreImpl.class.getSimpleName(), logName, action, entry.getKey());
						return DENIED;
					}
				}
			}
		}
		return null;
	}

	/**
	 * this will consider threadlock and actually lock the current thread 
	 * @param threadLock
	 * @param related      operation dependencies upon a main action relies on to be not running
	 * @return             Fail or Success by acquiring the group and releasing acquired locks if any failed       
	 */
	private boolean lockRelated(final Action cause, final boolean threadLock, final List<Action> related) {
		final Action[] rollback = new Action[related.size()];
		int i = 0;
		for (final Action action : related) {
			if (lock(action, threadLock, cause)) {
				rollback[i++] = action;
			} else {
				for (Action e : rollback) {
					release_(e, cause);
				}
				return false;
			}
		}
		return true;
	}

	private boolean isLocked(Action action) {
		if (action.getScope() == Scope.LOCAL) {
			return g(action).isLocked();
		} else if (action.getScope() == Scope.GLOBAL) {
			// TODO 
			return false;
		}
		return false;
	}

	private boolean lock(final Action action, final boolean blockThread, final Action cause) {

		if (logger.isDebugEnabled()) {
			if (cause == null) {
				logger.debug("{}: ({}) {} is {}", SemaphoreImpl.class.getSimpleName(), logName, action,
					blockThread ? "Locking" : "TryLocking");
			} else {
				logger.debug("{}: ({}) {} is {}: {}", SemaphoreImpl.class.getSimpleName(), logName, cause,
					blockThread ? "Locking" : "TryLocking", action);
			}
		}
		switch (action.getScope()) {
		case GLOBAL:
			return locks.acquireDistributedLock(nameForDistributedLock(action, null), true, DISTRIBUTED_LOCK_WAIT_MS,
					TimeUnit.MILLISECONDS);
		case LOCAL:
			ReentrantLock lock = g(action);
			if (blockThread) {
				lock.lock();
				return true;
			} else {
				return lock.tryLock();
			}
		default:
			logger.error("{}: ({}) what the heck ? --> {}", SemaphoreImpl.class.getSimpleName(), logName, action.getScope());
			return false;
		}
	}

	@Override
	public void release(final Action action) {
		release_(action, null);
	}

	private void release_(final Action action, final Action cause) {
		Validate.notNull(action);
		// first unlock the dependencies so others may start running
		if (cause == null) { // only when not rolling back to avoid deadlock
			final List<Action> group = rules.get(action).getRelated(SIBLING);
			if (group != null) {
				group.forEach(a -> release_(a, action));
			}
		}
		switch (action.getScope()) {
		case LOCAL:
			// then unlock the main lock
			ReentrantLock lock = g(action);
			if (lock == null) {
				logger.error("{}: ({}) Locks not implemented for action: {} ", SemaphoreImpl.class.getSimpleName(), logName, action);
			} else {
				if (logger.isDebugEnabled() && cause != null) {
					logger.debug("{}: ({}) {} is Releasing: {}", SemaphoreImpl.class.getSimpleName(), logName, cause, action);
				}
				lock.unlock();
			}
			break;
		case GLOBAL:
			locks.releaseDistributedLock(nameForDistributedLock(action, null));
			break;
		}
	}

	private String nameForDistributedLock(final Action action, final String id) {
		return new StringBuilder()
				.append(SpectatorSupplier.MINKA_SUBDOMAIN).append("/")
				.append(config.getBootstrap().getNamespace()).append("/")
				.append(action.toString()).append((id == null ? "" : "-" + id))
				.toString();
	}

	private ReentrantLock g(final Action a) {
		return locksByAction.get(a);
	}
	
	protected Config getConfig() {
		return this.config;
	}
}
