package io.tilt.minka.api;

import static io.tilt.minka.api.ReplyValue.SUCCESS;
import static io.tilt.minka.api.ReplyValue.SUCCESS_OPERATION_ALREADY_SUBMITTED;

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
	
	private ReplyValue value;
	private Entity<? extends Serializable> entity;
	private EntityState state;
	private EntityEvent event;
	private String message;
	
	//serialization
	public Reply() {}
	public Reply(
			final ReplyValue value, 
			final Entity<? extends Serializable> entity, 
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
				.append("Cause-Code:").append(value.toString()).append(',')
				.toString()
				;
	}
	
	// serialization
	public void setValue(final ReplyValue value) {
		this.value = value;
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