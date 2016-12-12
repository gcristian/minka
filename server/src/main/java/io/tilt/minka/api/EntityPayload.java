package io.tilt.minka.api;

import java.io.InputStream;
import java.io.Serializable;

/**
 * Facility to transport streams for large binaries from Leader to Followers.
 * 
 * @author Cristian Gonzalez
 * @since Apr 1, 2016
 *
 */
public interface EntityPayload extends Serializable {

	default boolean hasStreamPayload() {
		return false;
	}

	/**
	 * Only called first time to fetch 
	 * @return	the stream to fetch data from
	 */
	default InputStream getInputStream() {
		return null;
	}

}
