/**
 * Copyright (c) 2011-2015 Zauber S.A. -- All rights reserved
 */

package io.tilt.minka;

import java.util.HashSet;
import java.util.Set;

import org.junit.Assert;
import org.junit.Test;

import io.tilt.minka.api.DutyBuilder;
import io.tilt.minka.domain.ShardEntity;

public class ShardDutyTest {

		@Test
		public void tgst() {
			int i = (int) Math.ceil((double) 20 / 3);
			System.out.println(i);
			Assert.assertTrue(true);
		}

		//@Test
		public void testBasic() {
			int i = (int) Math.ceil((double) 20 / 3);
			System.out.println(i);
			Assert.assertTrue(true);

			Set<ShardEntity> set = new HashSet<>();
			DutyBuilder<String> d2 = DutyBuilder.build(String.class, "0", "p2");
			set.add(ShardEntity.create(DutyBuilder.build(String.class, "0", "p2")));
			set.add(ShardEntity.create(d2));
			set.add(ShardEntity.create(DutyBuilder.build(String.class, "0", "p2")));
			assert(set.size() == 3);

			Assert.assertTrue(set.contains(ShardEntity.create(d2)));
		}

}
