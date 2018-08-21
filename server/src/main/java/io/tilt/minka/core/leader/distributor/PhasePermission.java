package io.tilt.minka.core.leader.distributor;

import java.time.Instant;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.tilt.minka.api.Config;
import io.tilt.minka.core.leader.data.Scheme;
import io.tilt.minka.core.leader.data.Scheme.ClusterHealth;
import io.tilt.minka.core.task.LeaderAware;
import io.tilt.minka.shard.ShardState;

/**
 * Distributor phase permission to run.
 * Running in stealth mode (default): 
 * 		- only user or environment changes trigger distribution phase.
 * 		- last user change must surpass time-distance threshold on the previous.
 * On shards other than online: 
 * 		- distributor phase is suspended, until too many suspensions and run again.
 * 		- distributor phase is NOT suspended if a ChangePlan is taking place
 * There's a min shards online that disables phase to execute.
 */
public class PhasePermission {

	private final Logger logger = LoggerFactory.getLogger(getClass());
	
	private final LeaderAware leaderAware;
	private final Config config;
	private final Scheme scheme; 
	private final String tag;
	
	private int counterForAvoids;
	private Instant lastStealthBlocked;

	PhasePermission(
			final Config config, 
			final Scheme scheme, 
			final LeaderAware leaderAware,
			final String tag) {
		this.config = config;
		this.scheme = scheme;
		this.leaderAware = leaderAware;
		this.tag = tag;
	}

	/** @return FALSE among many reasons to avoid phase execution */
	boolean authorize() {
		// also if this's a de-frozening thread
		if (!leaderAware.imLeader()) {
			logger.warn("{}: ({}) Suspending distribution: not leader anymore ! ", getClass().getSimpleName(), tag);
			return false;
		}
		// skip if unstable unless a plan in progress or expirations will occurr and dismiss
		final ChangePlan currPlan = scheme.getCurrentPlan();
		final boolean noPlan = currPlan==null || currPlan.getResult().isClosed();
		final boolean changes = config.getDistributor().isRunOnStealthMode() &&
				(scheme.getCommitedState().isStealthChange() 
				|| scheme.getDirty().isStealthChange());
		
		if (noPlan && scheme.getShardsHealth() == ClusterHealth.UNSTABLE) {
			if (counterForAvoids++<30) {
				if (counterForAvoids==0) {
					logger.warn("{}: ({}) Suspending distribution until reaching cluster stability", getClass().getSimpleName(), tag);
				}
				return false;
			} else {
				logger.error("{}: ({}) Cluster unstable but too many phase avoids", getClass().getSimpleName(), tag);
				counterForAvoids = 0;
			}
		}
		
		if (!changes && noPlan) {
			return false;
		} else if (changes && noPlan && !changesFurtherEnough()) {
			return false;
		}
		
		final int online = scheme.getCommitedState().shardsSize(ShardState.ONLINE.filter());
		final int min = config.getProctor().getMinShardsOnlineBeforeSharding();
		if (online < min) {
			logger.warn("{}: Suspending distribution: not enough online shards (min:{}, now:{})", getClass().getSimpleName(), min, online);
			return false;
		}
		
		return true;
	}

	/** @return TRUE to release distribution phase considering uncommited stealthing */
	private boolean changesFurtherEnough() {
		if (scheme.getDirty().isStealthChange()) {
			final long threshold = config.beatToMs(config.getDistributor().getStealthHoldThreshold());
			if (!scheme.getDirty().stealthOverThreshold(threshold)) {						
				if (lastStealthBlocked==null) {
					lastStealthBlocked = Instant.now();
				} else if (System.currentTimeMillis() - lastStealthBlocked.toEpochMilli() >
					// safety release policy
					config.beatToMs(config.getDistributor().getPhaseFrequency() * 5)) {
					lastStealthBlocked = null;
					logger.warn("{}: Phase release: threshold ", getClass().getSimpleName());
				} else {
					logger.info("{}: Phase hold: stage's stealth-change over time distance threshold", getClass().getSimpleName());
					return false;
				}
			}
		}
		return true;
	}
	

}
