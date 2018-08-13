package io.tilt.minka.api;

import com.fasterxml.jackson.annotation.JsonProperty;

public enum ReplyValue {
	
	/** response from the leader */
	SUCCESS(202),
	
	/** response from the leader */
	SUCCESS_OPERATION_ALREADY_SUBMITTED(208),
	
	/** sent to the leader without waiting for response */
	SENT_SUCCESS(204),
	
	// our bad
	FAILURE(500),
	
	// network error: cannot send the operation thru the wire
	SENT_FAILED(503),
	
	// cannot repeat this operation 
	ERROR_ENTITY_ALREADY_EXISTS(409),
	
	// the related entity does not exist 
	ERROR_ENTITY_NOT_FOUND(404),
	
	// lacks of vital information, has invalid data or modifies already sent data 
	ERROR_ENTITY_INCONSISTENT(422),
	
	;
	
	final int httpCode;
	ReplyValue(final int code) {
		this.httpCode = code;
	}
	/** @return an http status code, restful analog */
	@JsonProperty("http")
	public int getHttpCode() {
		return httpCode;
	}
	
	public String toString() {
		return httpCode + "-" + this.name();
	}
}