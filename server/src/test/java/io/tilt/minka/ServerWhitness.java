package io.tilt.minka;

import java.util.Set;

import io.tilt.minka.api.Duty;
import io.tilt.minka.api.Server;

/**
 * Facility wrapper with the server instance 
 * and testimony data of it's main events
 */
public class ServerWhitness {
	private final Server<String, String> server;
	private final Set<Duty<String>> everCaptured;
	private final Set<Duty<String>> everReleased;
	private final Set<Duty<String>> current;
	public ServerWhitness(
			final Server<String, String> server, 
			final Set<Duty<String>> everCaptured,
			final Set<Duty<String>> everReleased,
			final Set<Duty<String>> current) {
		super();
		this.server = server;
		this.everCaptured = everCaptured;
		this.everReleased = everReleased;
		this.current = current;
	}
	public Server<String, String> getServer() {
		return server;
	}
	/** @return what has ever been captured ignoring those already released */
	public Set<Duty<String>> getEverCaptured() {
		return everCaptured;
	}
	/** @return what has ever been released ignoring those re-captured again */
	public Set<Duty<String>> getEverReleased() {
		return everReleased;
	}
	/** @return what is assigned right now (captured minus released) */
	public Set<Duty<String>> getCurrent() {
		return current;
	}
}