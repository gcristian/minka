package io.tilt.minka.core.leader.distributor;

import java.util.Set;

import io.tilt.minka.api.Pallet;
import io.tilt.minka.core.leader.PartitionScheme;
import io.tilt.minka.domain.Shard;
import io.tilt.minka.domain.ShardEntity;


public class MigratorTest {

	public static Migrator migrator(
			final Set<Shard> shards,
			final Set<ShardEntity> duties, 
			final Pallet<?> p1) {
		final PartitionScheme pt = new PartitionScheme();
		shards.forEach(s->pt.getScheme().addShard(s));
		
		return new Migrator(pt, p1, duties);
	}


}
