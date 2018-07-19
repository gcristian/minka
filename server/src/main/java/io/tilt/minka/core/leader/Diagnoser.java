package io.tilt.minka.core.leader;

import static io.tilt.minka.domain.Shard.ShardState.GONE;
import static io.tilt.minka.domain.Shard.ShardState.JOINING;
import static io.tilt.minka.domain.Shard.ShardState.ONLINE;
import static io.tilt.minka.domain.Shard.ShardState.QUARANTINE;
import static io.tilt.minka.utils.LogUtils.HEALTH_DOWN;
import static io.tilt.minka.utils.LogUtils.HEALTH_UP;

import java.time.Instant;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.commons.math3.stat.descriptive.SummaryStatistics;
import org.apache.commons.math3.util.Precision;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.tilt.minka.api.Config;
import io.tilt.minka.domain.Heartbeat;
import io.tilt.minka.domain.NetworkShardIdentifier;
import io.tilt.minka.domain.Shard;
import io.tilt.minka.domain.Shard.ShardState;
import io.tilt.minka.utils.CollectionUtils.SlidingSortedSet;

/**
 * Diagnose a health state transition for a Shard given its heartbeats, current state and last changes
 */
public class Diagnoser {
	
	private final Logger logger = LoggerFactory.getLogger(getClass());
	private final Config config;

	private final long normalDelay;
	private final long configuredLapse;
	private final int minHealthlyToGoOnline;
	private final int minToBeGone;
	private final int maxSickToGoQuarantine;
	
	public Diagnoser(Config config) {
		super();
		this.config = config;
		this.normalDelay = config.beatToMs(config.getFollower().getHeartbeatFrequency());
		this.configuredLapse = config.beatToMs(config.getProctor().getHeartbeatLapse());
		this.minHealthlyToGoOnline = config.getProctor().getMinHealthlyHeartbeatsForShardOnline();
		this.minToBeGone = config.getProctor().getMinAbsentHeartbeatsBeforeShardGone();
		this.maxSickToGoQuarantine = config.getProctor().getMaxSickHeartbeatsBeforeShardQuarantine();
	}

	/** @return a state transition if diagnosed at all or the same transition */
	protected Shard.Transition nextTransition(
			final ShardState currentState,
			final SlidingSortedSet<Shard.Transition> transitions,
			final SlidingSortedSet<Heartbeat> beats) {
		
		final long now = System.currentTimeMillis();
		final long lapseStart = now - configuredLapse;
		ShardState newState = currentState;
		LinkedList<Heartbeat> pastLapse = null;
		Shard.Cause cause = transitions.values().iterator().next().getCause();
		
		if (beats.size() < minToBeGone) {
			final long max = config.beatToMs(config.getProctor().getMaxShardJoiningState());
			if (transitions.last().getTimestamp().plusMillis(max).isBefore(Instant.now())) {
				cause = Shard.Cause.JOINING_STARVED;
				newState = GONE;
			} else {
				cause = Shard.Cause.FEW_HEARTBEATS;
				newState = JOINING;
			}
		} else {
			pastLapse = beats.values().stream().filter(i -> i.getCreation().isAfter(lapseStart))
					.collect(Collectors.toCollection(LinkedList::new));
			int pastLapseSize = pastLapse.size();
			if (pastLapseSize > 0 && checkHealth(now, normalDelay, pastLapse)) {
				if (pastLapseSize >= minHealthlyToGoOnline) {
					cause = Shard.Cause.HEALTHLY_THRESHOLD;
					newState = ONLINE;
				} else {
					cause = Shard.Cause.HEALTHLY_THRESHOLD;
					newState = QUARANTINE;
					// how many times should we support flapping before killing it
				}
			} else {
				if (pastLapseSize > maxSickToGoQuarantine) {
					if (pastLapseSize <= minToBeGone || pastLapseSize == 0) {
						cause = Shard.Cause.MIN_ABSENT;
						newState = GONE;
					} else {
						cause = Shard.Cause.MAX_SICK_FOR_ONLINE;
						newState = QUARANTINE;
					}
				} else if (pastLapseSize <= minToBeGone && currentState == QUARANTINE) {
					cause = Shard.Cause.MIN_ABSENT;
					newState = GONE;
				} else if (pastLapseSize > 0 && currentState == ONLINE) {
					cause = Shard.Cause.SWITCH_BACK;
					newState = QUARANTINE;
				} else if (pastLapseSize == 0 
						&& (currentState == QUARANTINE || currentState == ONLINE)) {
					cause = Shard.Cause.BECAME_ANCIENT;
					newState = GONE;
				}
			}
		}
		return new Shard.Transition(cause, newState);
	}

	private boolean checkHealth(final long now, final long normalDelay, final List<Heartbeat> onTime) {
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