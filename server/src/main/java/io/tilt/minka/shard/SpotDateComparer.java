package io.tilt.minka.shard;

import java.io.Serializable;
import java.util.Comparator;

import io.tilt.minka.core.leader.balancer.Spot;

public class SpotDateComparer implements Comparator<Spot>, Serializable {
	
	private static final long serialVersionUID = -2098725005810996576L;
	
	@Override
	public int compare(final Spot s, final Spot s2) {
		return compareByCreation(s, s2);
	}
	
	static int compareByCreation(final Spot s, final Spot s2) {
		return s.getCreation().compareTo(s2.getCreation());
	}
}