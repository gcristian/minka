package io.tilt.minka.api;

import static io.tilt.minka.api.ReplyValue.SUCCESS;
import static io.tilt.minka.api.ReplyValue.SUCCESS_OPERATION_ALREADY_SUBMITTED;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;

import io.tilt.minka.domain.EntityEvent;
import io.tilt.minka.domain.EntityState;
import io.tilt.minka.domain.ShardEntity;

/**
 * Response of an operation sent from the Client to the Leader
 * 
 * @author Cristian Gonzalez
 * @since 1/may/2018
 */
@JsonInclude(Include.NON_NULL)
public class Reply {
	
	private ReplyValue value;
	private Entity entity;
	private EntityState state;
	private EntityEvent event;
	private String message;
	
	//serialization
	public Reply() {}
	Reply(
			final ReplyValue value, 
			final Entity entity, 
			final EntityState state,
			final EntityEvent event, 
			final String msg) {
		super();
		this.value = value;
		this.entity = entity;
		this.state = state;
		this.event = event;
		this.message = msg;
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
	/** @return the event of the entity, NULL when non related  */
	public EntityEvent getEvent() {
		return event;
	}
	/** @return the stsate of the entity, NULL when non related  */
	public EntityState getState() {
		return state;
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
	public void setState(final EntityState state) {
		this.state = state;
	}
	public void setEvent(final EntityEvent event) {
		this.event = event;
	}
	public void setMessage(final String message) {
		this.message = message;
	}
	
	public String toMessage() {
		return new StringBuilder(toString())
				.append(" >> ").append(message)
				.append(", entity:").append(entity)
				.append(", state:").append(state)
				.toString();
	}
	
	public static Reply error(final Exception e) {
		return new Reply(ReplyValue.FAILURE, null, null, null, e.getMessage());
	}
	
	public static Reply notFound(Entity entity) {
		final String msg = String.format("Skipping operation not found in CommitedState: %s", 
				entity.getId());
		return new Reply(ReplyValue.ERROR_ENTITY_NOT_FOUND, entity, null, null, msg);
	}
	public static Reply success(Entity e, boolean added) {
		final String msg = String.format("Operation %s done: %s", added ? "": "already", e.getId());
		return new Reply(added ? 
				ReplyValue.SUCCESS : ReplyValue.SUCCESS_OPERATION_ALREADY_SUBMITTED, 
                e, null, null, msg);
	}

	public static Reply alreadySubmitted(final Duty duty) {
		return new Reply(ReplyValue.SUCCESS_OPERATION_ALREADY_SUBMITTED, duty, null, 
				null, String.format("Submited before !: %s", duty));
	}
	
	public static Reply alreadyExists(final Entity entity) {
		final String msg = String.format("Skipping operation, entity already in CommitedState: %s", 
				entity.getId());
		return new Reply(ReplyValue.ERROR_ENTITY_ALREADY_EXISTS, 
				entity, null, null, msg);
	}
	public static Reply inconsistent(final Duty duty) {
		final String msg = String.format("Skipping Crud Event %s: Pallet ID :%s set not found or yet created",
				EntityEvent.CREATE, null, duty.getPalletId());
		return new Reply(ReplyValue.ERROR_ENTITY_INCONSISTENT, duty, null, null, msg);
	}
	public static Reply sentAsync(Entity entity) {
		return new Reply(ReplyValue.SENT_SUCCESS, entity, null, null, null);
	}
	public static Reply failedToSend(Entity entity) {
		return new Reply(ReplyValue.SENT_FAILED, entity, null, null, null);
	}
	public static Reply sent(boolean sent, final Entity e) {
		return new Reply(
				sent ? ReplyValue.SENT_SUCCESS : ReplyValue.SENT_FAILED, 
				e, 
				null, 
				null, 
				null);
	}
	
	
}