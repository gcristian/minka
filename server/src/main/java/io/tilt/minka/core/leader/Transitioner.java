package io.tilt.minka.core.leader;

import static io.tilt.minka.shard.ShardState.GONE;
import static io.tilt.minka.shard.ShardState.JOINING;
import static io.tilt.minka.shard.ShardState.ONLINE;
import static io.tilt.minka.shard.ShardState.DELAYED;
import static io.tilt.minka.shard.TransitionCause.BECAME_ANCIENT;
import static io.tilt.minka.shard.TransitionCause.FEW_HEARTBEATS;
import static io.tilt.minka.shard.TransitionCause.HEALTHLY_THRESHOLD;
import static io.tilt.minka.shard.TransitionCause.JOINING_STARVED;
import static io.tilt.minka.shard.TransitionCause.MAX_SICK_FOR_ONLINE;
import static io.tilt.minka.shard.TransitionCause.MIN_ABSENT;
import static io.tilt.minka.shard.TransitionCause.SWITCH_BACK;

import java.time.Instant;
import java.util.LinkedList;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.tilt.minka.api.Config;
import io.tilt.minka.domain.Heartbeat;
import io.tilt.minka.shard.ShardState;
import io.tilt.minka.shard.Transition;
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
	
	private final ShardBeatsHealth cardio;
	
	Transitioner(Config config, final ShardBeatsHealth cardio) {
		super();
		this.config = config;
		this.normalDelay = config.beatToMs(config.getFollower().getHeartbeatFrequency());
		this.configuredLapse = config.beatToMs(config.getProctor().getHeartbeatLapse());
		this.minHealthlyToGoOnline = config.getProctor().getMinHealthlyHeartbeatsForShardOnline();
		this.minToBeGone = config.getProctor().getMinAbsentHeartbeatsBeforeShardGone();
		this.maxSickToGoQuarantine = config.getProctor().getMaxSickHeartbeatsBeforeShardDelayed();
		this.cardio = cardio;
	}

	/** @return a state transition if diagnosed at all or the same transition */
	Transition nextTransition(
			final ShardState currentState,
			final SlidingSortedSet<Transition> transitions,
			final SlidingSortedSet<Heartbeat> beats) {
		
		final long now = System.currentTimeMillis();
		final long lapseStart = now - configuredLapse;
		
		Transition ret = new Transition(
				transitions.values().iterator().next().getCause(), 
				currentState);
		
		if (beats.size() < minToBeGone) {
			final long max = config.beatToMs(config.getProctor().getMaxShardJoiningState());
			if (transitions.last().getTimestamp().plusMillis(max).isBefore(Instant.now())) {
				ret = new Transition(JOINING_STARVED, GONE);
			} else {
				ret = new Transition(FEW_HEARTBEATS, JOINING);
			}
		} else {
			final LinkedList<Heartbeat> pastLapse = beats.values().stream()
					.filter(i -> i.getCreation().isAfter(lapseStart))
					.collect(Collectors.toCollection(LinkedList::new));
			int size = pastLapse.size();
			if (size > 0 && cardio.isHealthly(now, normalDelay, pastLapse)) {
				ret = new Transition(HEALTHLY_THRESHOLD, 
						size >= minHealthlyToGoOnline ? ONLINE : DELAYED);
					// how many times should we support flapping before killing it
			} else {
				if (size > maxSickToGoQuarantine) {
					if (size <= minToBeGone || size == 0) {
						ret = new Transition(MIN_ABSENT, GONE);
					} else {
						ret = new Transition(MAX_SICK_FOR_ONLINE, DELAYED);
					}
				} else if (size <= minToBeGone && currentState == DELAYED) {
					ret = new Transition(MIN_ABSENT, GONE);
				} else if (size > 0 && currentState == ONLINE) {
					ret = new Transition(SWITCH_BACK, DELAYED);
				} else if (size == 0 
						&& (currentState == DELAYED || currentState == ONLINE)) {
					ret = new Transition(BECAME_ANCIENT, GONE);
				} else if (currentState == JOINING && size == 0 ) {
					ret = new Transition(JOINING_STARVED, GONE);
				}
			}
		}
		return ret;
	}

}