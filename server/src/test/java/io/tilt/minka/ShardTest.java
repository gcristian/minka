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
import io.tilt.minka.domain.NetworkShardIDImpl;
import io.tilt.minka.domain.Shard;
import io.tilt.minka.domain.Shard.CapacityComparer;
import io.tilt.minka.domain.ShardCapacity.Capacity;

public class ShardTest {

	private Pallet<String> p = PalletBuilder.<String>builder("1").build();;

	@Test
	public void test_shard_comparers() throws Exception {
		final Set<Shard> capacityOrder = new TreeSet<>(new CapacityComparer(p));
		final Set<Shard> dateOrder = new TreeSet<>(new Shard.DateComparer());
		for (int i = 0; i < new Random().nextInt(100); i++) {
			capacityOrder.add(buildShard(p, new Random().nextDouble()));
		}
		 Iterator<Shard> it = capacityOrder.iterator();
		double lastcap = 0;
		while (it.hasNext()) {
			Shard next = it.next();
			final double thiscap = next.getCapacities().get(p).getTotal();
			Assert.assertTrue("capacity comparer returned a different relation", thiscap > lastcap);
			lastcap = thiscap;
			dateOrder.add(next); // add out of order
		}
		Assert.assertTrue("no shard built", lastcap > 0);
		
		it = dateOrder.iterator();
		Shard last = null;
		while (it.hasNext()) {
			Shard next = it.next();
			if (last!=null) {
				Assert.assertTrue("date comparer returned a different order", 
						next.getFirstTimeSeen().getMillis() > last.getFirstTimeSeen().getMillis());
			}
			last = next;
		}
	}

	private Shard buildShard(final Pallet<String> p, final double cap) throws Exception {
		final Shard s1 = new Shard(Mockito.mock(BrokerChannel.class), new NetworkShardIDImpl(new Config()));
		final Map<Pallet<?>, Capacity> c = new HashMap<>();
		c.put(p, new Capacity(p, cap));
		s1.setCapacities(c);
		return s1;
	}

}
