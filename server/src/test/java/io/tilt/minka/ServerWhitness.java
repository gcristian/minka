package io.tilt.minka;

import java.util.Set;

import io.tilt.minka.api.Duty;
import io.tilt.minka.api.Server;

/**
 * Facility wrapper with the server instance 
 * and testimony data of it's main events
 */
public class ServerWhitness {
	private final Server server;
	private final Set<Duty> everCaptured;
	private final Set<Duty> everReleased;
	private final Set<Duty> current;
	public ServerWhitness(
			final Server server, 
			final Set<Duty> everCaptured,
			final Set<Duty> everReleased,
			final Set<Duty> current) {
		super();
		this.server = server;
		this.everCaptured = everCaptured;
		this.everReleased = everReleased;
		this.current = current;
	}
	public Server getServer() {
		return server;
	}
	/** @return what has ever been captured ignoring those already released */
	public Set<Duty> getEverCaptured() {
		return everCaptured;
	}
	/** @return what has ever been released ignoring those re-captured again */
	public Set<Duty> getEverReleased() {
		return everReleased;
	}
	/** @return what is assigned right now (captured minus released) */
	public Set<Duty> getCurrent() {
		return current;
	}
}