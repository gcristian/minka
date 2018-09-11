package io.tilt.minka.core.leader.data;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import io.tilt.minka.domain.EntityRecord;
import io.tilt.minka.shard.Shard;
import io.tilt.minka.shard.ShardIdentifier;
import io.tilt.minka.shard.Transition;
import io.tilt.minka.utils.CollectionUtils;
import io.tilt.minka.utils.CollectionUtils.SlidingSortedSet;

public class Vault {

	private final Map<String, SlidingSortedSet<EntityRecord>> duties = new HashMap<>();
	private final Map<ShardIdentifier, SlidingSortedSet<Transition>> goneShards = new HashMap<>();
	
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

	public Map<ShardIdentifier, SlidingSortedSet<Transition>> getGoneShards() {
		return goneShards;
	}

	/** backup gone shards for history */
	public void addGoneShard(final Shard shard) {
		int max = 5;
		for (final Iterator<Transition> it = shard.getTransitions().values().iterator(); it.hasNext() && max>0;max--) {
			SlidingSortedSet<Transition> set = goneShards.get(shard.getShardID());
			if (set==null) {
				goneShards.put(shard.getShardID(), set = CollectionUtils.sliding(max));
			}
			set.add(it.next());
		}
	}
	
}
