package io.tilt.minka.api.crud;

import java.util.Collections;
import java.util.ConcurrentModificationException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.tilt.minka.model.Duty;

/**
 * Coordination place where futures as responses to User's CRUD operations are
 * parked, waiting for resolution from the Leader.
 * Two types of Futures: Replies for CRUD ops. and CommitState for valid replied CRUDs   
 */
@SuppressWarnings({ "rawtypes", "unchecked" })	
public class LatchHandler {

	private static final Logger logger = LoggerFactory.getLogger(LatchHandler.class);

	public static final int NO_EXPIRATION = -1;
	public static final int MAX_STALE_SIZE = Short.MAX_VALUE;
	public static final int MAX_STALE_DURATION_MS = 1_000 * 60;
	
	// 1 - Lot for CRUD parking futures (reply from Leader about consistency of a creation/deletion )
	private Map<String, Transfer> replies = Collections.synchronizedMap(new HashMap<>(1024));
	// 2 - Lot for distribution parking futures (capture and replication events)
	private Map<Duty, Transfer> states = Collections.synchronizedMap(new HashMap<>(1024));
	
	static class Transfer<T> {
		private T payload;
		private CountDownLatch latch;
		private final long timestamp = System.currentTimeMillis();;
		
		boolean isResolved() {
			return payload!=null;
		}
		void resolve(final T t) {
			payload = t;
			if (latch!=null) {
				latch.countDown();
			}
		}
		void set(final T t) {
			payload = t;
		}
		CountDownLatch createLatch() {
			return latch = new CountDownLatch((payload!=null) ? 0 : 1);
		}
		T get() {
			return payload;
		}
	}
	
	/** suspend thread until latch release with results */
	<K,V> V waitAndGet(final K k, long maxWaitMs) {
		final Map<K, Transfer> transfers = (Map) ((k instanceof Duty) ? states : replies);
		V ret = null;
		try {
			if (logger.isDebugEnabled()) {
				logger.debug("{}: Parking future {} for: {}", getClass().getSimpleName(), type(k), k);
			}
			
			final Transfer neo = new Transfer();
			// resolutions may come before on leader-client joint
			final Transfer<V> prev = transfers.putIfAbsent(k, neo);
			if (prev!=null && !prev.isResolved()) {
				throw new IllegalStateException("Operation " + k + " has been already submitted");	
			}
			if (prev==null) {
				if (maxWaitMs>0) {
					neo.createLatch().await(maxWaitMs, TimeUnit.MILLISECONDS);
				} else {
					neo.createLatch().await();
				}
			}
			Transfer<V> rmv = transfers.remove(k);
			ret = prev!=null ? prev.get() : rmv.get();
			if (logger.isDebugEnabled()) {
				logger.debug("{}: Resuming latch for: {} ({}) {}", getClass().getSimpleName(), k, 
					ret==null ? "not found" : "found", ret);
			}
		} catch (Exception e) {
			logger.error("{}: Unexpected parking: {}", getClass().getSimpleName(), k, e);
		} finally {			
			if (transfers.size()>MAX_STALE_SIZE) {
				checkAndExpire(transfers);
			}
		}
		return ret;
	}

	private <K> Object type(final K k) {
		return k instanceof String ? "CommitBatchResponse" : "CommitState";
	}

	private <K> void checkAndExpire(final Map<K, Transfer> transfers) {
		try {
			final long now = System.currentTimeMillis();
			Iterator<Map.Entry<K, Transfer>> it = transfers.entrySet().iterator();
			while (it.hasNext()) {
				final Entry<K, Transfer> e = it.next();
				if (now - e.getValue().timestamp > MAX_STALE_DURATION_MS
						&& !e.getValue().isResolved()) {
					logger.error("{}: Expiring unclaimed {}:{}", getClass().getSimpleName(), 
							type(e.getKey()), e.getKey());
					// release the waiting task if exists
					e.getValue().resolve(null);
					it.remove();
				}
			}
		} catch (final ConcurrentModificationException cme) {
			// dont mind: next will do
		}
	}

	<K> boolean exists(final K k) {
		return ((Map) ((k instanceof Duty) ? states : replies)).containsKey(k);
	}
	
	public int getCommitsSize() {
		return states.size();
	}
	public int getResponsesSize() {
		return replies.size();
	}
	
	// 22:16 la bebe se tumbo
	
	/** resume latch with results */
	public <K, V> void transfer(final K k, final V v) {
		final Map<K, Transfer> transfers = (Map) ((k instanceof Duty) ? states : replies);
		Transfer tp = transfers.get(k);
		if (tp==null) {
			// resolutions may come before on leader-client joint
			tp = new Transfer();
			tp.set(v);
			transfers.put(k, tp);
		} else {
			tp.resolve(v);
		}
	}
	
}
