/**
 *  Copyright (c) 2011-2015 Zauber S.A.  -- All rights reserved
 */

package io.tilt.minka;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TreeSet;

import org.joda.time.DateTime;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import io.tilt.minka.api.Pallet;
import io.tilt.minka.api.PalletBuilder;
import io.tilt.minka.broker.EventBroker.BrokerChannel;
import io.tilt.minka.core.leader.balancer.Balancer.NetworkLocation;
import io.tilt.minka.domain.TCPShardIdentifier;
import io.tilt.minka.domain.Shard;
import io.tilt.minka.domain.Shard.CapacityComparer;
import io.tilt.minka.domain.ShardCapacity.Capacity;

public class ShardTest {

	private Pallet<String> p = Pallet.<String>builder("1").build();;

	@Test
	public void test_shard_comparers() throws Exception {
		final Set<NetworkLocation> capacityOrder = new TreeSet<>(new CapacityComparer(p));
		final Set<NetworkLocation> dateOrder = new TreeSet<>(new Shard.DateComparer());
		for (int i = 0; i < new Random().nextInt(100); i++) {
			capacityOrder.add(new NetworkLocation(buildShard(p, new Random().nextDouble())));
		}
		 Iterator<NetworkLocation> it = capacityOrder.iterator();
		double lastcap = 0;
		while (it.hasNext()) {
			NetworkLocation next = it.next();
			final double thiscap = next.getCapacities().get(p).getTotal();
			Assert.assertTrue("capacity comparer returned a different relation", thiscap > lastcap);
			lastcap = thiscap;
			dateOrder.add(next); // add out of order
		}
		Assert.assertTrue("no shard built", lastcap > 0);
		
		it = dateOrder.iterator();
		NetworkLocation last = null;
		while (it.hasNext()) {
			NetworkLocation next = it.next();
			if (last!=null) {
				Assert.assertTrue("date comparer returned a different order", 
						next.getCreation().getMillis() > last.getCreation().getMillis());
			}
			last = next;
		}
	}
	
	
	public static Shard buildShard(final Pallet<String> p, final double cap) throws Exception {
		return buildShard(p, cap, null);
	}
	
	public static final Random rnd = new Random();
	
	public static Shard buildShard(final Pallet<String> p, final double cap, final Integer id) throws Exception {
		final TCPShardIdentifier mockedShardID = mock(TCPShardIdentifier.class);
		final String idi = String.valueOf(id==null ? rnd.nextInt(5000): id);
		when(mockedShardID.getId()).thenReturn(idi);
		when(mockedShardID.getCreation()).thenReturn(new DateTime());
		when(mockedShardID.toString()).thenReturn(idi);
		final Shard s1 = new Shard(Mockito.mock(BrokerChannel.class), mockedShardID);
		final Map<Pallet<?>, Capacity> c = new HashMap<>();
		c.put(p, new Capacity(p, cap));
		s1.setCapacities(c);
		return s1;
	}

}
