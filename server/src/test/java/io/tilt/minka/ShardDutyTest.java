package io.tilt.minka;

import java.util.HashSet;
import java.util.Set;

import org.junit.Assert;
import org.junit.Test;

import io.tilt.minka.api.DutyBuilder;
import io.tilt.minka.api.Duty;
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
		Duty<String> d2 = DutyBuilder.<String>builder("0", "p2").build();
		set.add(ShardEntity.Builder.builder(DutyBuilder.builder("0", "p2").build()).build());
		set.add(ShardEntity.Builder.builder(d2).build());
		set.add(ShardEntity.Builder.builder(DutyBuilder.builder("0", "p2").build()).build());
		assert (set.size() == 3);

		Assert.assertTrue(set.contains(ShardEntity.Builder.builder(d2).build()));
	}

}
