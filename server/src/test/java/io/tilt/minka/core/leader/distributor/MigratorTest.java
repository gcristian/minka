package io.tilt.minka.core.leader.distributor;

import java.util.Set;

import io.tilt.minka.api.Pallet;
import io.tilt.minka.core.leader.data.ShardingState;
import io.tilt.minka.domain.ShardEntity;
import io.tilt.minka.shard.Shard;


public class MigratorTest {

	public static Migrator migrator(
			final Set<Shard> shards,
			final Set<ShardEntity> duties, 
			final Pallet p1) {
		final ShardingState pt = new ShardingState();
		shards.forEach(s->pt.getCommitedState().addShard(s));
		
		return new Migrator(pt, p1, duties);
	}


}
