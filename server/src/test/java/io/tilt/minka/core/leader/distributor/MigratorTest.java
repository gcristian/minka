package io.tilt.minka.core.leader.distributor;

import java.util.Set;

import io.tilt.minka.api.Pallet;
import io.tilt.minka.core.leader.PartitionTable;
import io.tilt.minka.domain.Shard;
import io.tilt.minka.domain.ShardEntity;


public class MigratorTest {

	public static Migrator migrator(
			final Set<Shard> shards,
			final Set<ShardEntity> duties, 
			final Pallet<?> p1) {
		final PartitionTable pt = new PartitionTable();
		shards.forEach(s->pt.getStage().addShard(s));
		
		return new Migrator(pt, p1, duties);
	}


}
