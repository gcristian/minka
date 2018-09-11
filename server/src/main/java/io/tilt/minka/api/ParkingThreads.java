package io.tilt.minka.api;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.tilt.minka.api.CommitBatch.CommitBatchResponse;
import io.tilt.minka.core.leader.data.CommitState;

/**
 * Coordination place where futures as responses to User's CRUD operations are
 * parked, waiting for resolution from the Leader.
 * Two types of Futures: Replies for CRUD ops. and CommitState for valid replied CRUDs   
 */
@SuppressWarnings({ "rawtypes", "unchecked" })	
public class ParkingThreads {

	private static final Logger logger = LoggerFactory.getLogger(ParkingThreads.class);

	public static final long NO_EXPIRATION = -1;
	
	// 1 - Lot for CRUD parking futures (reply from Leader about consistency of a creation/deletion )
	public Map<String, CountDownLatch> responseLatches = Collections.synchronizedMap(new HashMap<>());
	public Map<String, CommitBatchResponse> responses = Collections.synchronizedMap(new HashMap<>());

	// 2 - Lot for distribution parking futures (capture and replication events)  
	public Map<Duty, CountDownLatch> stateLatches = Collections.synchronizedMap(new HashMap<>());
	public Map<Duty, CommitState> states = Collections.synchronizedMap(new HashMap<>());
	

	/** suspend thread until latch release with results */
	<K,V> V wait(final K k, long maxWaitMs) {
		if (latches(k).containsKey(k)) {
			throw new IllegalStateException("Operation " + k + " has been already submitted");
		}
		V ret = null;
		try {
			final CountDownLatch latch = new CountDownLatch(1);
			logger.info("{}: Parking future for: {}", getClass().getSimpleName(), k);
			latches(k).put(k, latch);
			if (maxWaitMs>0) {
				latch.await(maxWaitMs, TimeUnit.MILLISECONDS);
			} else {
				latch.await();
			}			
			ret = (V) resolutions(k).remove(k);
			logger.info("{}: Resuming latch for: {} ({}) {}", getClass().getSimpleName(), k, 
					ret==null ? "not found" : "found", ret);
		} catch (Exception e) {
			logger.error("{}: Unexpected parking: {}", getClass().getSimpleName(), k, e);
		} finally {
			latches(k).remove(k);
		}
		return ret;
	}

	/** resume latch with results */
	public <K, V> void resolve(final K k, final V v) {
		final CountDownLatch latch = latches(k).remove(k);
		if (latch!=null) {
			((Map)resolutions(k)).put(k, v);
			latch.countDown();
		} else {
			logger.error("{}: Resolution {} without Latch: {}", getClass().getSimpleName(), v, k);
		}
	}

	private <K, V> Map<K, V> resolutions(final K k) {
		if (k instanceof Duty) {
			return (Map)states;
		} else if (k instanceof String) {
			return (Map)responses;
		} else {
			throw new IllegalArgumentException("Bad argument type: " + k);
		}
	}
	
	private <K, V> Map<K, CountDownLatch> latches(final K k) {
		if (k instanceof Duty) {
			return (Map)stateLatches;
		} else if (k instanceof String) {
			return (Map)responseLatches;
		} else {
			throw new IllegalArgumentException("Bad argument type: " + k);
		}
	}

}
