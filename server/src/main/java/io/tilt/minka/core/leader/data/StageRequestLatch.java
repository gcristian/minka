package io.tilt.minka.core.leader.data;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import io.tilt.minka.api.Duty;
import io.tilt.minka.api.Entity;

public class StageRequestLatch {

	public static final long NO_EXPIRATION = -1;
	
	public Map<Entity, CountDownLatch> latchesByDuty = Collections.synchronizedMap(new HashMap<>());
	public Map<Duty, CommitState> statesByDuty = Collections.synchronizedMap(new HashMap<>());
	
	public CommitState blockToResolution(final Entity duty, long maxWaitMs) {
		try {
			final CountDownLatch cdl = new CountDownLatch(1);
			latchesByDuty.put(duty, cdl);
			if (maxWaitMs>0) {
				if (cdl.await(maxWaitMs, TimeUnit.MILLISECONDS)) {
					return statesByDuty.remove(duty);
				} else {
					// else nobody will
					latchesByDuty.remove(duty);
				}
			} else {
				cdl.await();
				return statesByDuty.remove(duty);
			}
		} catch (InterruptedException e) {
			latchesByDuty.remove(duty);
			// log
		}
		return null;
	}
	
	public void resolve(final Collection<StageRequest> requests) {
		for (StageRequest sr: requests) {
			final Duty key = sr.getEntity().getDuty();
			final CountDownLatch cdl = latchesByDuty.remove(key);
			if (cdl!=null) {
				statesByDuty.put(key, sr.getState());
				cdl.countDown();
			}
		}
	}
	
}
