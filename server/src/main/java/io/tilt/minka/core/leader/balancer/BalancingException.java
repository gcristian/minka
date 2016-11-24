/**
 *  Copyright (c) 2011-2015 Zauber S.A.  -- All rights reserved
 */

package io.tilt.minka.core.leader.balancer;

public class BalancingException extends IllegalStateException {
	private static final long serialVersionUID = -8670349051342724490L;
	public BalancingException(final String msg) {
		super(msg);
	}
	public BalancingException(final String msg, Object...args) {
		this(String.format(msg, args));
	}
}
