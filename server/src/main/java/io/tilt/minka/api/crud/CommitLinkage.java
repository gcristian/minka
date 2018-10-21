package io.tilt.minka.api.crud;

import static java.util.concurrent.CompletableFuture.completedFuture;

import java.util.Collection;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.function.Function;

import io.tilt.minka.core.leader.data.CommitState;
import io.tilt.minka.core.task.LeaderAware;
import io.tilt.minka.domain.EntityEvent;
import io.tilt.minka.domain.ShardEntity;
import io.tilt.minka.domain.ShardedPartition;
import io.tilt.minka.shard.NetworkShardIdentifier;

/**
 * Handle building of futures for CommitState response
 */
class CommitLinkage {
	
	private final LatchHandler latchHandler;
	private final LeaderAware leaderAware;
	private final ShardedPartition partition;
	private final int maxWaitMs;
	private ExecutorService statePool;
	
	CommitLinkage(
			final LatchHandler latchHandler, 
			final LeaderAware leaderAware,
			final ShardedPartition partition,
			final ExecutorService statePool,
			final int maxWaitMs) {
		super();
		this.latchHandler = latchHandler;
		this.leaderAware = leaderAware;
		this.partition = partition;
		this.maxWaitMs = maxWaitMs;
		this.statePool = statePool;
	}

	/** links the reply with a Future knowing the system's commit-state of a Duty  
	 * @param event */
	void assignFutures(
			final Collection<Reply> replies, 
			final EntityEvent event, 
			final Function<Reply, Void> retrier) {
		
		final NetworkShardIdentifier previous = leaderAware.getLeaderShardId();
		for (final Reply reply: replies) {
			// only successful replies
			if ((reply.getValue()==ReplyValue.SUCCESS)) {
				try {
					if (latchHandler.exists(reply.getEntity())) {
						reply.setValue(ReplyValue.SUCCESS_OPERATION_ALREADY_SUBMITTED);
					} else {
						if (reply.getRetryCounter()==0) {
							reply.setState(statePool.submit(()-> waitOrRetry(event, previous, reply, retrier)));
						} else {
							// retry: if we're already in a blocking future we can block
							reply.setState(completedFuture(waitOrRetry(event, previous, reply, retrier)));
						}
					}
				} catch (Exception e) {
					CrudExecutor.logger.error("{}: Unexpected linking promise for {}", getClass().getSimpleName(), reply, e);
				}
			} else {
				CrudExecutor.logger.warn("{}: Reply failure ({}): {}", getClass().getSimpleName(), 
						reply, reply.getEntity().getId());
			}
		}
	}

	/** 
	 * block and sleep until leader response or retry while quota
	 * recursively retry.
	 * @param f 
	 *  
	 * @throws InterruptedException 
	 */
	private CommitState waitOrRetry(
			final EntityEvent event, 
			final NetworkShardIdentifier prevLeader, 
			final Reply reply,
			final Function<Reply, Void> retry) {
		
		CommitState cs = latchHandler.waitAndGet(reply.getEntity(), maxWaitMs);
		if (cs == null) {
			cs = determineState(prevLeader, reply, event);
			if (cs == CommitState.REJECTED) {
				if (reply.getRetryCounter() < CrudExecutor.MAX_RETRIES) {
					reply.addRetry();
					// take adaptation period
					try {
						Thread.sleep(CrudExecutor.RETRY_SLEEP);
						CrudExecutor.logger.warn("{}: Retrying after leader reelection, duty: {}", 
								getClass().getSimpleName(), reply.getEntity());
						retry.apply(reply);
					} catch (InterruptedException e) {
					}
				} else {
					CrudExecutor.logger.error("{}: Reply from Leader retries exhausted for {}", getClass().getSimpleName(), reply);								
				}
			}
		} else {
			reply.withTiming(Reply.Timing.CLIENT_RECEIVED_STATE_TS, System.currentTimeMillis());
			CrudExecutor.logger.info("{}: CommitState:{} for: {} ({} ms)", getClass().getSimpleName(), 
					cs, reply.getEntity().getId(), reply.getTimeElapsedSoFar());
		}
		return cs;
	}

	private CommitState determineState(final NetworkShardIdentifier prevLeader, final Reply reply, final EntityEvent ee) {
		// a good reason for leader indifference: reelection happened
		if (leaderAware.getLastLeaderChange().toEpochMilli() 
					> reply.getTiming(Reply.Timing.CLIENT_RECEIVED_REPLY_TS)
				&& !leaderAware.getLeaderShardId().equals(prevLeader)) {
			return convalidate(reply, ee) ? CommitState.FINISHED : CommitState.REJECTED;
		} else {
			CrudExecutor.logger.error("{}: Client op cancelled for starvation!: for: {} ({} ms)", 
				getClass().getSimpleName(), reply.getEntity().getId(), reply.getTimeElapsedSoFar());
			return CommitState.CANCELLED;
		}
	}

	/**
	 * now how is the current CommitedState about the asked operation ?
	 * convalidate the current state if it reflects the user's intention
	 */
	private boolean convalidate(final Reply r, final EntityEvent ee) {
		// TODO FALSO myself may not be involved
		final Optional<ShardEntity> se = this.partition.getDuties().stream()
				.filter(d->d.getDuty().equals(r.getEntity()))
				.findFirst();
		if (ee==EntityEvent.CREATE && se.isPresent() || ee==EntityEvent.REMOVE && !se.isPresent()) {
			// leader changed after commiting request but before replying state
			return true;
		} else {
			//if (se.isPresent() && se.get().getCommitTree().getLast())
			CrudExecutor.logger.error("{}: Client op rejected for leader reelection!: for: {} ({} ms)", 
					getClass().getSimpleName(), r.getEntity().getId(), r.getTimeElapsedSoFar());
			return false;
		}
	}
}