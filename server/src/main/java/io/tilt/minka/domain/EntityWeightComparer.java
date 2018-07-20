package io.tilt.minka.domain;

import java.io.Serializable;
import java.util.Comparator;

import io.tilt.minka.api.Duty;

public class EntityWeightComparer implements Comparator<Duty>, Serializable {
	
	private static final long serialVersionUID = 2191475545082914908L;
	
	@Override
	public int compare(final Duty o1, final Duty o2) {
		if (o1 == null || o2 == null) {
			return ShardEntity.compareNulls(o1, o2);
		} else {
			int ret = Double.compare(o1.getWeight(), o2.getWeight());
			// break comparator contract about same weight same entity yeah rightttttt
			if (ret == 0) {
				return ShardEntity.compareTieBreak(o1, o2);
			}
			return ret;
		}
	}
}