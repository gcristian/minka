package io.tilt.minka.core.leader.distributor;

import java.util.Set;

import io.tilt.minka.api.Pallet;
import io.tilt.minka.core.leader.ShardingScheme;
import io.tilt.minka.domain.Shard;
import io.tilt.minka.domain.ShardEntity;


public class MigratorTest {

	public static Migrator migrator(
			final Set<Shard> shards,
			final Set<ShardEntity> duties, 
			final Pallet<?> p1) {
		final ShardingScheme pt = new ShardingScheme();
		shards.forEach(s->pt.getScheme().addShard(s));
		
		return new Migrator(pt, p1, duties);
	}


}
