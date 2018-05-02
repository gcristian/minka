package io.tilt.minka.api;

import static io.tilt.minka.api.ReplyResult.SUCCESS;
import static io.tilt.minka.api.ReplyResult.SUCCESS_OPERATION_ALREADY_SUBMITTED;

import java.io.Serializable;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;

import io.tilt.minka.domain.EntityEvent;
import io.tilt.minka.domain.EntityState;

/**
 * Response of an operation sent from the Client to the Leader
 * 
 * @author Cristian Gonzalez
 * @since 1/may/2018
 */
@JsonInclude(Include.NON_NULL)
public class Reply {
	
	private ReplyResult cause;
	private Entity<? extends Serializable> entity;
	private EntityState state;
	private EntityEvent event;
	private String message;
	
	//serialization
	public Reply() {}
	public Reply(
			final ReplyResult cause, 
			final Entity<? extends Serializable> entity, 
			final EntityState state,
			final EntityEvent event, 
			final String msg) {
		super();
		this.cause = cause;
		this.entity = entity;
		this.state = state;
		this.event = event;
		this.message = msg;
	}
	@JsonIgnore
	public boolean isSuccess() {
		return cause==SUCCESS || cause==SUCCESS_OPERATION_ALREADY_SUBMITTED;
	}
	public ReplyResult getCause() {
		return cause;
	}
	public Entity<? extends Serializable> getEntity() {
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
				.append("Client-Result:").append(isSuccess()).append(',')
				.append("Cause-Code:").append(cause.toString()).append(',')
				.toString()
				;
	}
	
	// serialization
	public void setCause(final ReplyResult cause) {
		this.cause = cause;
	}
	public void setEntity(final Entity<? extends Serializable> entity) {
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
}