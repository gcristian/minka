package io.tilt.minka.shard;

import java.io.Serializable;
import java.util.Comparator;

import io.tilt.minka.core.leader.balancer.NetworkLocation;

public class DateComparer implements Comparator<NetworkLocation>, Serializable {
	
	private static final long serialVersionUID = -2098725005810996576L;
	
	@Override
	public int compare(final NetworkLocation s, final NetworkLocation s2) {
		return compareByCreation(s, s2);
	}
	
	static int compareByCreation(final NetworkLocation s, final NetworkLocation s2) {
		return s.getCreation().compareTo(s2.getCreation());
	}
}