package io.tilt.minka.core.leader.data;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import io.tilt.minka.domain.EntityRecord;
import io.tilt.minka.utils.CollectionUtils;
import io.tilt.minka.utils.CollectionUtils.SlidingSortedSet;

public class Vault {

	private final Map<String, SlidingSortedSet<EntityRecord>> duties = new HashMap<>();
	
	public boolean isEmpty() {
		return duties.isEmpty();
	}
	public void add(final String shardid, final EntityRecord deleted) {
		SlidingSortedSet<EntityRecord> byShard = duties.get(shardid);
		if (byShard == null) {
			duties.put(shardid, byShard = CollectionUtils.sliding(100));
		}
		byShard.add(deleted);
	}
	
	public Collection<EntityRecord> getAllDuties() {
		final Collection<EntityRecord> ret = new ArrayList<>();
		duties.values().forEach(s->ret.addAll(s.values()));
		return ret;
	}
	
	public Collection<EntityRecord> getDuties(final String shardid) {
		SlidingSortedSet<EntityRecord> byShard = duties.get(shardid);
		if (byShard != null) {
			return byShard.values();
		} else {
			return Collections.emptyList();
		}
	}
	
}
