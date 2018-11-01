package io.tilt.minka.shard;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Locker {
	
	private final Logger logger = LoggerFactory.getLogger(getClass());

	public Map<Locker.LockId, ReentrantReadWriteLock> locks = Collections.synchronizedMap(new HashMap<>());
	
	public enum LockId {
		SHARD_BEATS,
		COMMITTED_STATE,
		DIRTY_STATE,
		FOLLOWER_PARTITION,
		;
	}
	public Locker() {
		for(Locker.LockId lid: LockId.values()) {
			locks.put(lid, new ReentrantReadWriteLock(true));
		}
	}
	public boolean read(final Locker.LockId lock, final Runnable run) {
		return lockRunUnlock(locks.get(lock).readLock(), run);
	}
	public boolean write(final Locker.LockId lock, final Runnable run) {
		return lockRunUnlock(locks.get(lock).writeLock(), run);
	}
	private boolean lockRunUnlock(final Lock lock, final Runnable run) {
		try {
			if (lock.tryLock(1, TimeUnit.SECONDS)) {
				run.run();
				return true;
			} else {
				logger.error("{}: Cannot lock {} for {}", 
						getClass().getSimpleName(), lock, run.toString());
			}
		} catch (InterruptedException e) {
		} finally {
			lock.unlock();				
		}
		return false;
	}
}