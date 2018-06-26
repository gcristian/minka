package io.tilt.minka.api;

import java.io.InputStream;
import java.io.Serializable;

import com.fasterxml.jackson.annotation.JsonIgnore;

/**
 * Facility to transport streams for large binaries from Leader to Followers.
 * 
 * @author Cristian Gonzalez
 * @since Apr 1, 2016
 *
 */
public interface EntityPayload extends Serializable {
	
	@JsonIgnore
	default boolean isStream() {
		return false;
	}

	/**
	 * Only called first time to fetch 
	 * @return	the stream to fetch data from
	 */
	@JsonIgnore
	default InputStream getInputStream() {
		return null;
	}

}
