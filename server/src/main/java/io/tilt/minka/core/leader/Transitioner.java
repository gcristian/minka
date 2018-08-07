package io.tilt.minka.core.leader;

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
import io.tilt.minka.shard.NetworkShardIdentifier;
import io.tilt.minka.shard.ShardState;
import io.tilt.minka.shard.Transition;
import io.tilt.minka.shard.TransitionCause;
import io.tilt.minka.utils.CollectionUtils.SlidingSortedSet;

/**
 * Diagnose a health state transition for a Shard given its heartbeats, current state and last changes
 */
class Transitioner {
	
	private final Logger logger = LoggerFactory.getLogger(getClass());
	private final Config config;

	private final long normalDelay;
	private final long configuredLapse;
	private final int minHealthlyToGoOnline;
	private final int minToBeGone;
	private final int maxSickToGoQuarantine;
	
	Transitioner(Config config) {
		super();
		this.config = config;
		this.normalDelay = config.beatToMs(config.getFollower().getHeartbeatFrequency());
		this.configuredLapse = config.beatToMs(config.getProctor().getHeartbeatLapse());
		this.minHealthlyToGoOnline = config.getProctor().getMinHealthlyHeartbeatsForShardOnline();
		this.minToBeGone = config.getProctor().getMinAbsentHeartbeatsBeforeShardGone();
		this.maxSickToGoQuarantine = config.getProctor().getMaxSickHeartbeatsBeforeShardQuarantine();
	}

	/** @return a state transition if diagnosed at all or the same transition */
	Transition nextTransition(
			final ShardState currentState,
			final SlidingSortedSet<Transition> transitions,
			final SlidingSortedSet<Heartbeat> beats) {
		
		final long now = System.currentTimeMillis();
		final long lapseStart = now - configuredLapse;
		ShardState newState = currentState;
		LinkedList<Heartbeat> pastLapse = null;
		TransitionCause cause = transitions.values().iterator().next().getCause();
		
		if (beats.size() < minToBeGone) {
			final long max = config.beatToMs(config.getProctor().getMaxShardJoiningState());
			if (transitions.last().getTimestamp().plusMillis(max).isBefore(Instant.now())) {
				cause = TransitionCause.JOINING_STARVED;
				newState = ShardState.GONE;
			} else {
				cause = TransitionCause.FEW_HEARTBEATS;
				newState = ShardState.JOINING;
			}
		} else {
			pastLapse = beats.values().stream().filter(i -> i.getCreation().isAfter(lapseStart))
					.collect(Collectors.toCollection(LinkedList::new));
			int pastLapseSize = pastLapse.size();
			if (pastLapseSize > 0 && checkHealth(now, normalDelay, pastLapse)) {
				if (pastLapseSize >= minHealthlyToGoOnline) {
					cause = TransitionCause.HEALTHLY_THRESHOLD;
					newState = ShardState.ONLINE;
				} else {
					cause = TransitionCause.HEALTHLY_THRESHOLD;
					newState = ShardState.QUARANTINE;
					// how many times should we support flapping before killing it
				}
			} else {
				if (pastLapseSize > maxSickToGoQuarantine) {
					if (pastLapseSize <= minToBeGone || pastLapseSize == 0) {
						cause = TransitionCause.MIN_ABSENT;
						newState = ShardState.GONE;
					} else {
						cause = TransitionCause.MAX_SICK_FOR_ONLINE;
						newState = ShardState.QUARANTINE;
					}
				} else if (pastLapseSize <= minToBeGone && currentState == ShardState.QUARANTINE) {
					cause = TransitionCause.MIN_ABSENT;
					newState = ShardState.GONE;
				} else if (pastLapseSize > 0 && currentState == ShardState.ONLINE) {
					cause = TransitionCause.SWITCH_BACK;
					newState = ShardState.QUARANTINE;
				} else if (pastLapseSize == 0 
						&& (currentState == ShardState.QUARANTINE || currentState == ShardState.ONLINE)) {
					cause = TransitionCause.BECAME_ANCIENT;
					newState = ShardState.GONE;
				}
			}
		}
		return new Transition(cause, newState);
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