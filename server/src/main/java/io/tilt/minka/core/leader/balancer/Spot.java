package io.tilt.minka.core.leader.balancer;

import java.time.Instant;
import java.util.Comparator;
import java.util.Map;

import io.tilt.minka.shard.ShardCapacity;
import io.tilt.minka.model.Pallet;
import io.tilt.minka.shard.NetworkShardIdentifier;
import io.tilt.minka.shard.Shard;

/** 
 * Synonym and read-only view of a {@linkplain Shard}.
 * Safety decorator for balancers to use, in presence of custom balancers. 
 */
public class Spot implements Comparator<Spot>, Comparable<Spot> {
	
	private final Shard shard;
	
	public Spot(final Shard shard) {
		this.shard = shard;
	}
	public Map<Pallet, ShardCapacity> getCapacities() {
		return this.shard.getCapacities();
	}
	public NetworkShardIdentifier getId() {
		return this.shard.getShardID();
	}
	public Instant getCreation() {
		return this.shard.getFirstTimeSeen();
	}
	@java.lang.Override
	public String toString() {
		return this.shard.toString();
	}
	/**
	 * To be used by user's custom balancers for location/server reference. 
	 * @return the tag set with {@linkplain Minka} on setLocationTag(..) 
	 */
	public String getTag() {
	    return this.shard.getShardID().getTag();
	}
	private Shard getShard() {
		return shard;
	}
	@java.lang.Override
	public int compare(final Spot o1, final Spot o2) {
		return shard.compare(o1.getShard(), o2.shard);
	}
	@java.lang.Override
	public int compareTo(final Spot o) {
		return shard.compareTo(o.getShard());
	}
	@java.lang.Override
	public int hashCode() {
		return shard.hashCode();
	}
	@java.lang.Override
	public boolean equals(Object obj) {
		if (obj==null || !(obj instanceof Spot)) {
			return false;
		} else if (obj==this) {
			return true;
		} else {
			return shard.equals(((Spot)obj).getShard());
		}
	}
}