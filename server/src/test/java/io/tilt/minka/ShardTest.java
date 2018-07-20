/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
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
import io.tilt.minka.broker.EventBroker.BrokerChannel;
import io.tilt.minka.core.leader.balancer.Spot;
import io.tilt.minka.shard.ShardCapacity;
import io.tilt.minka.shard.SpotCapacityComparer;
import io.tilt.minka.shard.SpotDateComparer;
import io.tilt.minka.shard.Shard;
import io.tilt.minka.shard.TCPShardIdentifier;

public class ShardTest {

	private Pallet p = Pallet.<String>builder("1").build();;

	@Test
	public void test_shard_comparers() throws Exception {
		final Set<Spot> capacityOrder = new TreeSet<>(new SpotCapacityComparer(p));
		final Set<Spot> dateOrder = new TreeSet<>(new SpotDateComparer());
		for (int i = 0; i < new Random().nextInt(100); i++) {
			capacityOrder.add(new Spot(buildShard(p, new Random().nextDouble())));
		}
		 Iterator<Spot> it = capacityOrder.iterator();
		double lastcap = 0;
		while (it.hasNext()) {
			Spot next = it.next();
			final double thiscap = next.getCapacities().get(p).getTotal();
			Assert.assertTrue("capacity comparer returned a different relation", thiscap > lastcap);
			lastcap = thiscap;
			dateOrder.add(next); // add out of order
		}
		Assert.assertTrue("no shard built", lastcap > 0);
		
		it = dateOrder.iterator();
		Spot last = null;
		while (it.hasNext()) {
			Spot next = it.next();
			if (last!=null) {
				Assert.assertTrue("date comparer returned a different order", 
						next.getCreation().isAfter(last.getCreation()));
			}
			last = next;
		}
	}
	
	
	public static Shard buildShard(final Pallet p, final double cap) throws Exception {
		return buildShard(p, cap, null);
	}
	
	public static final Random rnd = new Random();
	
	public static Shard buildShard(final Pallet p, final double cap, final Integer id) throws Exception {
		final TCPShardIdentifier mockedShardID = mock(TCPShardIdentifier.class);
		final String idi = String.valueOf(id==null ? rnd.nextInt(5000): id);
		when(mockedShardID.getId()).thenReturn(idi);
		when(mockedShardID.getCreation()).thenReturn(new DateTime());
		when(mockedShardID.toString()).thenReturn(idi);
		final Shard s1 = new Shard(Mockito.mock(BrokerChannel.class), mockedShardID);
		final Map<Pallet, ShardCapacity> c = new HashMap<>();
		c.put(p, new ShardCapacity(p, cap));
		s1.setCapacities(c);
		return s1;
	}

}
