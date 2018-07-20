package io.tilt.minka.core.leader.balancer;

import java.time.Instant;
import java.util.Comparator;
import java.util.Map;

import io.tilt.minka.api.Pallet;
import io.tilt.minka.shard.Capacity;
import io.tilt.minka.shard.NetworkShardIdentifier;
import io.tilt.minka.shard.Shard;

/** safety read-only Shard's decorator for balancers to use */
public class NetworkLocation implements Comparator<NetworkLocation>, Comparable<NetworkLocation> {
	private final Shard shard;
	public NetworkLocation(final Shard shard) {
		this.shard = shard;
	}
	public Map<Pallet, Capacity> getCapacities() {
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
	public int compare(final NetworkLocation o1, final NetworkLocation o2) {
		return shard.compare(o1.getShard(), o2.shard);
	}
	@java.lang.Override
	public int compareTo(final NetworkLocation o) {
		return shard.compareTo(o.getShard());
	}
	@java.lang.Override
	public int hashCode() {
		return shard.hashCode();
	}
	@java.lang.Override
	public boolean equals(Object obj) {
		if (obj==null || !(obj instanceof NetworkLocation)) {
			return false;
		} else if (obj==this) {
			return true;
		} else {
			return shard.equals(((NetworkLocation)obj).getShard());
		}
	}
}