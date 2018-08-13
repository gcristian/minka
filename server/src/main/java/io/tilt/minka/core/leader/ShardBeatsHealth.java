package io.tilt.minka.core.leader;

import static io.tilt.minka.utils.LogUtils.HEALTH_DOWN;
import static io.tilt.minka.utils.LogUtils.HEALTH_UP;

import java.util.List;

import org.apache.commons.math3.stat.descriptive.SummaryStatistics;
import org.apache.commons.math3.util.Precision;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.tilt.minka.api.Config;
import io.tilt.minka.domain.Heartbeat;
import io.tilt.minka.shard.NetworkShardIdentifier;

/**
 * Diagnose a health state transition for a Shard given its heartbeats, current state and last changes
 */
class ShardBeatsHealth {
	
	private final Logger logger = LoggerFactory.getLogger(getClass());
	private final Config config;

	ShardBeatsHealth(Config config) {
		super();
		this.config = config;
	}

	boolean isHealthly(final long now, final long normalDelay, final List<Heartbeat> onTime) {
		SummaryStatistics stat = new SummaryStatistics();
		// there's some hope
		long lastCreation = onTime.get(0).getCreation().getMillis();
		long biggestDistance = 0;
		for (Heartbeat hb : onTime) {
			long creation = hb.getCreation().getMillis();
			long arriveDelay = now - creation;
			long distance = creation - lastCreation;
			stat.addValue(distance);
			lastCreation = creation;
			biggestDistance = distance > biggestDistance ? distance : biggestDistance;
			if (logger.isDebugEnabled()) {
				logger.debug("{}: HB SeqID: {}, Arrive Delay: {}ms, Distance: {}ms, Creation: {}", 
						getClass().getSimpleName(), hb.getSequenceId(), arriveDelay, distance, hb.getCreation());
			}
		}

		long stdDeviationDelay = (long) Precision.round(stat.getStandardDeviation(), 2);
		long permittedStdDeviationDistance = (normalDelay
				* (long) (config.getProctor().getHeartbeatMaxDistanceStandardDeviation() * 10d) / 100);
		/*
		 * long permittedBiggestDelay = (normalDelay *
		 * (long)(config.getProctorHeartbeatMaxBiggestDistanceFactor()*10d) /
		 * 100);
		 */
		final NetworkShardIdentifier shardId = onTime.get(0).getShardId();
		boolean healthly = stdDeviationDelay < permittedStdDeviationDistance;// && biggestDelay < permittedBiggestDelay;
		if (logger.isDebugEnabled()) {
			logger.debug("{}: Shard: {}, {} Standard deviation distance: {}/{}", getClass().getSimpleName(), shardId,
					healthly ? HEALTH_UP : HEALTH_DOWN, stdDeviationDelay, permittedStdDeviationDistance);
		}
		return healthly;
	}
}