package io.tilt.minka.domain;

import java.io.Serializable;
import java.util.Comparator;

import io.tilt.minka.api.Duty;
import io.tilt.minka.api.DutyBuilder;
import io.tilt.minka.api.DutyBuilder.Task;

public class EntityDateComparer implements Comparator<Duty>, Serializable {
	
	private static final long serialVersionUID = 3709876521530551544L;
	
	@Override
	public int compare(final Duty o1, final Duty o2) {
		int i = 0;
		if (o1 == null || o2 == null) {
			return ShardEntity.compareNulls(o1, o2);
		} else if (o1 instanceof DutyBuilder.Task && o2 instanceof DutyBuilder.Task) {
			final Task o1ts = (DutyBuilder.Task)o1;
			final Task o2ts = (DutyBuilder.Task)o1;
			i = o1ts.getTimestamp().compareTo(o2ts.getTimestamp());
			if (i == 0) {
				i = ShardEntity.compareTieBreak(o1, o2);
			}				
		}
		return i;
	}
}