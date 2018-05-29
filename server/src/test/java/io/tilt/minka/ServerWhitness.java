package io.tilt.minka;

import java.util.Set;

import io.tilt.minka.api.Duty;
import io.tilt.minka.api.Server;

public class ServerWhitness {
	private final Server<String, String> server;
	private final Set<Duty<String>> captured;
	private final Set<Duty<String>> released;
	public ServerWhitness(
			final Server<String, String> server, 
			final Set<Duty<String>> captured,
			final Set<Duty<String>> released) {
		super();
		this.server = server;
		this.captured = captured;
		this.released = released;
	}
	public Server<String, String> getServer() {
		return server;
	}
	public Set<Duty<String>> getCaptured() {
		return captured;
	}
	public Set<Duty<String>> getReleased() {
		return released;
	}
}