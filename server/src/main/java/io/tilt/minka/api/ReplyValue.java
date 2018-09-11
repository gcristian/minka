package io.tilt.minka.api;

import javax.naming.ServiceUnavailableException;
import javax.ws.rs.ClientErrorException;
import javax.ws.rs.NotFoundException;
import javax.ws.rs.ServerErrorException;

import com.fasterxml.jackson.annotation.JsonProperty;

public enum ReplyValue {
	
	/** response from the leader */
	SUCCESS(202),
	
	/** response from the leader */
	SUCCESS_OPERATION_ALREADY_SUBMITTED(208),
	
	/** sent to the leader without waiting for response */
	SENT_SUCCESS(204),
	
	// our bad
	FAILURE(500) {
		@Override
		public Exception toRestException() {
			return new ServerErrorException(httpCode);
		}
	},
	
	SENT_FAILED(503) {
		@Override
		public Exception toRestException() {
			return new ServerErrorException("Cannot contact Leader (network error)", httpCode); 
		}
	},

	REPLY_TIMED_OUT(504) {
		@Override
		public Exception toRestException() {
			return new ServiceUnavailableException("Leader didnt respond back request");
		}
	},

	ERROR_ENTITY_ALREADY_EXISTS(409) {
		@Override
		public Exception toRestException() {
			return new ClientErrorException("Entity already exists", httpCode);
		}
	},
	
	ERROR_ENTITY_NOT_FOUND(404) {
		@Override
		public Exception toRestException() {
			return new NotFoundException("Entity doesnt exists");
		}
	},
	
	ERROR_ENTITY_INCONSISTENT(422) {
		@Override
		public Exception toRestException() {
			return new ClientErrorException("Entity with invalid data", httpCode);
		}
	},
	
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
	
	public Exception toRestException() {
		throw new IllegalStateException("Successful replies have no exception !");
	}
}