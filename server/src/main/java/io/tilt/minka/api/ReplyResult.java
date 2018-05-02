package io.tilt.minka.api;

public enum ReplyResult {
	
	/** response from the leader */
	SUCCESS(202),
	
	/** response from the leader */
	SUCCESS_OPERATION_ALREADY_SUBMITTED(208),
	
	/** sent to the leader without waiting for response */
	SUCCESS_SENT(204),
	
	// our bad
	FAILURE(500),
	
	// network error: cannot send the operation thru the wire
	FAILURE_NOT_SENT(503),
	
	// cannot repeat this operation 
	ERROR_ENTITY_ALREADY_EXISTS(409),
	
	// the related entity does not exist 
	ERROR_ENTITY_NOT_FOUND(404),
	
	// lacks of vital information, has invalid data or modifies already sent data 
	ERROR_ENTITY_INCONSISTENT(422),
	
	;
	
	final int httpCode;
	ReplyResult(final int code) {
		this.httpCode = code;
	}
	/** @return an http status code, restful analog */
	public int getHttpCode() {
		return httpCode;
	}
	
	public String toString() {
		return httpCode + "-" + this.name();
	}
}