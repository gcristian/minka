package io.tilt.minka.core.leader.distributor;

public enum ChangePlanState {
	/* still a running plan */
	RUNNING,
	/* resending deliveries for a nth time */
	RETRYING,
	/* all deliveries passed from enqueued to pending and confirmed */
	CLOSED_APPLIED,
	/* plan contains invalid shippings unable to deliver */
	CLOSED_ERROR,
	/* some deliveries became obsolete/impossible, rebuilding is required */
	CLOSED_OBSOLETE,
	/* some deliveries were never confirmed beyond retries/waiting limits */
	CLOSED_EXPIRED;

	public boolean isSuccess() {
		return this == CLOSED_APPLIED;
	}

	public boolean isClosed() {
		return this != RUNNING && this != RETRYING;
	}
}