/**
 *  Copyright (c) 2011-2015 Zauber S.A.  -- All rights reserved
 */

package io.tilt.minka;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TreeSet;

import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import io.tilt.minka.api.Config;
import io.tilt.minka.api.Pallet;
import io.tilt.minka.api.PalletBuilder;
import io.tilt.minka.broker.EventBroker.BrokerChannel;
import io.tilt.minka.core.leader.distributor.Balancer.Location;
import io.tilt.minka.domain.TCPShardIdentifier;
import io.tilt.minka.domain.Shard;
import io.tilt.minka.domain.Shard.CapacityComparer;
import io.tilt.minka.domain.ShardCapacity.Capacity;

public class ShardTest {

	private Pallet<String> p = PalletBuilder.<String>builder("1").build();;

	@Test
	public void test_shard_comparers() throws Exception {
		final Set<Location> capacityOrder = new TreeSet<>(new CapacityComparer(p));
		final Set<Location> dateOrder = new TreeSet<>(new Shard.DateComparer());
		for (int i = 0; i < new Random().nextInt(100); i++) {
			capacityOrder.add(new Location(buildShard(p, new Random().nextDouble())));
		}
		 Iterator<Location> it = capacityOrder.iterator();
		double lastcap = 0;
		while (it.hasNext()) {
			Location next = it.next();
			final double thiscap = next.getCapacities().get(p).getTotal();
			Assert.assertTrue("capacity comparer returned a different relation", thiscap > lastcap);
			lastcap = thiscap;
			dateOrder.add(next); // add out of order
		}
		Assert.assertTrue("no shard built", lastcap > 0);
		
		it = dateOrder.iterator();
		Location last = null;
		while (it.hasNext()) {
			Location next = it.next();
			if (last!=null) {
				Assert.assertTrue("date comparer returned a different order", 
						next.getCreation().getMillis() > last.getCreation().getMillis());
			}
			last = next;
		}
	}

	private Shard buildShard(final Pallet<String> p, final double cap) throws Exception {
		final Shard s1 = new Shard(Mockito.mock(BrokerChannel.class), new TCPShardIdentifier(new Config()));
		final Map<Pallet<?>, Capacity> c = new HashMap<>();
		c.put(p, new Capacity(p, cap));
		s1.setCapacities(c);
		return s1;
	}

}
