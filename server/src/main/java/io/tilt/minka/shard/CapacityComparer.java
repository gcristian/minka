package io.tilt.minka.shard;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Comparator;
import java.util.stream.Collectors;

import io.tilt.minka.api.Pallet;
import io.tilt.minka.core.leader.balancer.NetworkLocation;

/* is important to maintain a predictable order to avoid migration churning */
public class CapacityComparer implements Comparator<NetworkLocation>, Serializable {
	
	private static final long serialVersionUID = 2191475545082914908L;
	private final Pallet pallet;
	
	public CapacityComparer(Pallet pallet) {
		super();
		this.pallet = pallet;
	}
	@Override
	public int compare(final NetworkLocation s, final NetworkLocation s2) {
		final Capacity cap1 = s.getCapacities().get(pallet);
		final Capacity cap2 = s2.getCapacities().get(pallet);
		if (cap1 == null) {
			return -1;
		} else if (cap2 == null) {
			return 1;
		} else {
			int ret = Double.compare(cap1.getTotal(), cap2.getTotal());
			// always the same predictable order 
			ret = ret != 0 ? ret : DateComparer.compareByCreation(s, s2);
			if (ret==0) {
				// TODO refactory
				ret = Arrays.asList(s.getId().getId(), s2.getId().getId())
					.stream().sorted()
					.collect(Collectors.toList())
					.get(0).equals(s.getId().getId()) ? -1 : 1;
			}
			return ret;
		}
	}
}