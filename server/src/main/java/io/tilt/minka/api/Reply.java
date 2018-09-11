package io.tilt.minka.api;

import static io.tilt.minka.api.ReplyValue.SUCCESS;
import static io.tilt.minka.api.ReplyValue.SUCCESS_OPERATION_ALREADY_SUBMITTED;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Future;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonProperty;

import io.tilt.minka.core.leader.data.CommitState;
import io.tilt.minka.domain.EntityEvent;
import io.tilt.minka.domain.EntityState;

/**
 * Response of an operation sent from the Client to the Leader shard.
 * The {@link ReplyValue} refers to the legality of the operation, not the commit result.
 * For positive responses the result can be get at {@linkplain CommitState} future. 
 * 
 * @author Cristian Gonzalez
 * @since 1/may/2018
 */
@JsonInclude(Include.NON_NULL)
public class Reply {
	
	private ReplyValue value;
	private Entity entity;
	private String message;
	private Future<CommitState> commitState;
	
	static enum Timing {
		// the CRUD request born
		CLIENT_CREATED_TS,
		// sent from client endpoint after creation (may be retries, delays)
		CLIENT_SENT_TS,
		// moment at leader reply creation
		LEADER_REPLY_TS,
		// time of reply reception 
		CLIENT_RECEIVED_REPLY_TS,
		// when answer state arrives (CommitState)
		CLIENT_RECEIVED_STATE_TS,
	}
	
	private Map<Timing, Long> times = new HashMap<>(Timing.values().length);
	
	//serialization
	public Reply() {}
	Reply(
			final ReplyValue value, 
			final Entity entity, 
			final String msg) {
		super();
		this.value = value;
		this.entity = entity;
		this.message = msg;
		withTiming(Timing.LEADER_REPLY_TS, System.currentTimeMillis());
	}
	
	Reply withTiming(final Timing t, final long value) {
		this.times.put(t, value);
		return this;
	}
	@JsonProperty("times")
	private Map<Timing, Long> getTimes_() {
		return times;
	}
	@JsonProperty("repliedMs")
	private long getRepliedIn_() {
		final Timing since = Timing.CLIENT_CREATED_TS;
		final Timing until = Timing.CLIENT_RECEIVED_REPLY_TS;
		return rest(since, until);
	}
	@JsonProperty("distributedMs")
	private long getDistributedIn_() {
		final Timing since = Timing.CLIENT_CREATED_TS;
		final Timing until = Timing.CLIENT_RECEIVED_STATE_TS;
		return rest(since, until);
	}
	private long rest(final Timing since, final Timing until) {
		if (times.containsKey(since) && times.containsKey(until)) {
			return times.get(until) - times.get(since);
		} else {
			return -1;
		}
	}
	@JsonIgnore
	long getTimeElapsedSoFar() {
		long until = times.get(times.containsKey(Timing.CLIENT_RECEIVED_STATE_TS) ? 
				Timing.CLIENT_RECEIVED_STATE_TS : Timing.CLIENT_RECEIVED_REPLY_TS);
		return until-times.get(Timing.CLIENT_CREATED_TS);
	}
	
	@JsonIgnore
	public boolean isSuccess() {
		return value==SUCCESS || value==SUCCESS_OPERATION_ALREADY_SUBMITTED;
	}
	public ReplyValue getValue() {
		return value;
	}
	public Entity getEntity() {
		return entity;
	}
	/** @return explanation, NULL when success */
	public String getMessage() {
		return message;
	}
	public ConsistencyException toException() {
		final ConsistencyException e = new ConsistencyException(message);
		e.setSourceResponse(this);
		return e;
	}
	/** @return Future if ReplyValue is SUCCESS or SUCCESS_SENT, null otherwise */
	public Future<CommitState> getState() {
		if (value==ReplyValue.SUCCESS || value==ReplyValue.SENT_SUCCESS) {
			return commitState;
		} else {
			throw new IllegalStateException("Current Reply was " + value.toString() + " and lacks of a CommitState");
		}
	}
	void setFuture(Future<CommitState> future) {
		this.commitState = future;
	}
	
	@Override
	public int hashCode() {
		return entity.getId().hashCode();
	}
	@Override
	public boolean equals(Object obj) {
		if (obj==null || !(obj instanceof Reply)) {
			return false;
		} else if (obj==this) {
			return true;
		} else {
			final Reply r = (Reply)obj;
			return r.getEntity().getId().equals(entity.getId());
		}
 	}
	
	@Override
	public String toString() {
		return new StringBuilder()
				.append("Success:").append(isSuccess()).append(',')
				.append("Reply-Code:").append(value.toString()).append(',')
				.toString()
				;
	}
	
	// serialization
	public void setValue(final ReplyValue value) {
		this.value = value;
	}
	public void setEntity(final Entity entity) {
		this.entity = entity;
	}
	public void setMessage(final String message) {
		this.message = message;
	}
	
	public String toMessage() {
		return new StringBuilder(toString())
				.append(" >> ").append(message)
				.append(", entity:").append(entity)
				.append(", value:").append(value)
				.toString();
	}
	
	
	
	// ================================= Utility builder methods =================================  
	
	
	public static Reply error(final Exception e) {
		return new Reply(ReplyValue.FAILURE, null, e.getMessage());
	}
	
	public static Reply notFound(Entity entity) {
		final String msg = String.format("Skipping operation not found in CommittedState: %s", 
				entity.getId());
		return new Reply(ReplyValue.ERROR_ENTITY_NOT_FOUND, entity, msg);
	}
	public static Reply success(final Entity e, boolean added) {
		final String msg = String.format("Operation %s done: %s", added ? "": "already", e.getId());
		return new Reply(
				added ? ReplyValue.SUCCESS : ReplyValue.SUCCESS_OPERATION_ALREADY_SUBMITTED, 
                e, msg);
	}

	public static Reply alreadySubmitted(final Duty duty) {
		return new Reply(ReplyValue.SUCCESS_OPERATION_ALREADY_SUBMITTED, duty,  
				 String.format("Submited before !: %s", duty));
	}
	
	public static Reply alreadyExists(final Entity entity) {
		final String msg = String.format("Skipping operation, entity already in CommittedState: %s", 
				entity.getId());
		return new Reply(ReplyValue.ERROR_ENTITY_ALREADY_EXISTS, entity, msg);
	}
	public static Reply inconsistent(final Duty duty) {
		final String msg = String.format("Skipping Crud Event %s: Pallet ID :%s set not found or yet created",
				EntityEvent.CREATE, null, duty.getPalletId());
		return new Reply(ReplyValue.ERROR_ENTITY_INCONSISTENT, duty, msg);
	}
	public static Reply sentAsync(final Entity entity) {
		return new Reply(ReplyValue.SENT_SUCCESS, entity,  null);
	}
	public static Reply failedToSend(final Entity e) {
		return new Reply(ReplyValue.SENT_FAILED, e, null);
	}
	public static Reply leaderReplyTimedOut() {
		return new Reply(ReplyValue.REPLY_TIMED_OUT, null, null);
	}
	public static Reply sent(boolean sent, final Entity e) {
		return new Reply(sent ? ReplyValue.SENT_SUCCESS : ReplyValue.SENT_FAILED, e, null);
	}
	
	
}