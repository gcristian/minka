package io.tilt.minka.domain;

import java.io.Serializable;
import java.util.Comparator;

import io.tilt.minka.model.Duty;

public class EntityHashComparer implements Comparator<Duty>, Serializable {
	
	private static final long serialVersionUID = 3709876521530551544L;
	
	@Override
	public int compare(final Duty o1, final Duty o2) {
		if (o1 == null || o2 == null) {
			return ShardEntity.compareNulls(o1, o2);
		} else {
			int i = (o1.getPalletId()+o1.getId()).compareTo(o2.getPalletId()+o2.getId());
			if (i == 0) {
				i = ShardEntity.compareTieBreak(o1, o2);
			}
			return i;
		}
	}
}