
package io.tilt.minka.api;

public class ConsistencyException extends IllegalStateException {
	private static final long serialVersionUID = -8670349051342724490L;

	
	private Reply sourceResponse;
	
	public ConsistencyException() {
		super();
	}
	public ConsistencyException(final String arg) {
		super(arg);
	}
	
	public void setSourceResponse(final Reply response) {
		this.sourceResponse = response;
	}
	public Reply getSourceResponse() {
		return sourceResponse;
	}
	
}
