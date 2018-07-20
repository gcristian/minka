package io.tilt.minka.core.leader.distributor;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.junit.Test;

import io.tilt.minka.ShardTest;
import io.tilt.minka.api.Duty;
import io.tilt.minka.api.Pallet;
import io.tilt.minka.core.leader.balancer.FairWeightBalancer;
import io.tilt.minka.core.leader.balancer.Spot;
import io.tilt.minka.core.leader.balancer.PreSort;
import io.tilt.minka.core.leader.data.ShardingState;
import io.tilt.minka.domain.EntityEvent;
import io.tilt.minka.domain.ShardEntity;
import io.tilt.minka.shard.Shard;

public class FairWeightBalancerTest {


	public static Set<ShardEntity> someEntitiesWithOddOrder() {
		final Set<ShardEntity> set = new HashSet<>();
		set.add(ShardEntity.Builder.builder(buildDutyWithWeight(1400l, "heavy1")).build());
		set.add(ShardEntity.Builder.builder(buildDutyWithWeight(1400l, "heavy2")).build());
		set.add(ShardEntity.Builder.builder(buildDutyWithWeight(400l, "middle")).build());
		set.add(ShardEntity.Builder.builder(buildDutyWithWeight(1l, "tiny1")).build());
		set.add(ShardEntity.Builder.builder(buildDutyWithWeight(1l, "tiny2")).build());
		set.add(ShardEntity.Builder.builder(buildDutyWithWeight(2l, "tiny3")).build());
		set.add(ShardEntity.Builder.builder(buildDutyWithWeight(5l, "tiny4")).build());
		return set;
	}
	
	public static Duty buildDutyWithWeight(long weight, String idi) {
		return Duty.
			builder(idi, "1")
				.with(weight)
				.build();
	}

	public static Set<Duty> dutiesFromEntities(final Set<ShardEntity> set) {
		final Set<Duty> duties = new HashSet<>(
				set.stream()
					.map(e->e.getDuty())
					.collect(Collectors.toSet()));
		return duties;
	}

	public static Set<ShardEntity> someEntitiesWithGrowingOrder() {
		final Set<ShardEntity> set = new HashSet<>();
		set.add(ShardEntity.Builder.builder(buildDutyWithWeight(10l, "1")).build());
		set.add(ShardEntity.Builder.builder(buildDutyWithWeight(100l, "2")).build());
		set.add(ShardEntity.Builder.builder(buildDutyWithWeight(200l, "3")).build());
		set.add(ShardEntity.Builder.builder(buildDutyWithWeight(500l, "4")).build());
		set.add(ShardEntity.Builder.builder(buildDutyWithWeight(1000l, "5")).build());
		set.add(ShardEntity.Builder.builder(buildDutyWithWeight(1500l, "6")).build());
		set.add(ShardEntity.Builder.builder(buildDutyWithWeight(1500l, "7")).build());
		return set;
	}
	
	public static Set<ShardEntity> dutiesWithWeights(final long...weights) {
		final Set<ShardEntity> set = new HashSet<>();
		int id = 1;
		for (final long weight: weights) {
			set.add(ShardEntity.Builder.builder(
					buildDutyWithWeight(weight, String.valueOf(id++))).build());
		}
		return set;
	}
	

	public static Set<ShardEntity> someEntitiesWithSameOrder(final int size) {
		final Set<ShardEntity> set = new HashSet<>();
		for (int i = 0;i<size; i++) {
			set.add(ShardEntity.Builder.builder(buildDutyWithWeight(10l, String.valueOf(i))).build());
		}
		return set;
	}
	
	
	@Test
	public void testRepeatableDistribution() throws Exception {

		// some basic domain
		final Set<ShardEntity> ents = someEntitiesWithGrowingOrder();
		final Set<Duty> duties = dutiesFromEntities(ents);		
		final Pallet p1 = Pallet.builder("1")
				.with(new FairWeightBalancer.Metadata())
				.build();
		
		// only creations at stage
		final Map<EntityEvent, Set<Duty>> stage = new HashMap<>();
		stage.put(EntityEvent.CREATE, duties);
		stage.put(EntityEvent.REMOVE, Collections.emptySet());
		
		final ShardingState table = emptyTableWithShards(p1, 5000, 5000);
		
		final Set<Shard> shards = new HashSet<>();
		table.getCommitedState().findShards(null, shards::add);
		final Migrator migra1 = MigratorTest.migrator(shards, ents, p1);
		new FairWeightBalancer().balance(p1, stageFromTable(table), stage, migra1);		
		final Override v1o1 = migra1.getOverrides().get(0);
		final Override v1o2 = migra1.getOverrides().get(1);

		final Set<Shard> shardss = new HashSet<>();
		table.getCommitedState().findShards(null, shardss::add);
		final Migrator migra2 = MigratorTest.migrator(shardss, ents, p1);
		new FairWeightBalancer().balance(p1, stageFromTable(table), stage, migra2);
		final Override v2o1 = migra2.getOverrides().get(0);
		final Override v2o2 = migra2.getOverrides().get(1);

		// both migrators should have produced the same outputs
		
		assertEquals(v1o1, v2o1);
		assertEquals(v1o2, v2o2);
		
	}


	@Test
	public void testBalancerStableCapacitiesAndWeights() throws Exception {

		// some basic domain
		final Set<ShardEntity> ents = someEntitiesWithSameOrder(12);
		final Set<Duty> duties = dutiesFromEntities(ents);		
		final Pallet p1 = Pallet.builder("1")
				.with(new FairWeightBalancer.Metadata())
				.build();
		
		// only creations at backstage
		final Map<EntityEvent, Set<Duty>> stage = new HashMap<>();
		stage.put(EntityEvent.CREATE, duties);
		stage.put(EntityEvent.REMOVE, Collections.emptySet());
		
		// only empty shards at scheme
		final ShardingState table = emptyTableWithShards(p1, 5000, 5000, 5000);
		final Set<Shard> shards = new HashSet<>();
		table.getCommitedState().findShards(null, shards::add);
		final Migrator m = MigratorTest.migrator(shards, ents, p1);
		new FairWeightBalancer().balance(p1, stageFromTable(table), stage, m);		
		if (m.getOverrides().size()!=3) {
			int u = 0;
		}
		/*
		for (final Override o: m.getOverrides()) {
			System.out.println(o.getEntities());
		}
		*/
		for (final Override o: m.getOverrides()) {
			assertTrue("balancer didnt returned a fair amount of duties when all same weight", 
					o.getEntities().size()==4);
		}
		
		assertFalse(m.getOverrides().get(0).equals(m.getOverrides().get(1)));
		assertFalse(m.getOverrides().get(0).equals(m.getOverrides().get(2)));
		assertFalse(m.getOverrides().get(1).equals(m.getOverrides().get(2)));

	}

	
	@Test
	public void testLowScattering() throws Exception {
		// some basic domain
		
		final Set<ShardEntity> ents = someEntitiesWithOddOrder();
		final Set<Duty> duties = dutiesFromEntities(ents);		
		final Pallet p1 = Pallet.builder("1")
				.with(new FairWeightBalancer.Metadata(FairWeightBalancer.Dispersion.EVEN, PreSort.WEIGHT))
				.build();
		
		// only creations at backstage
		final Map<EntityEvent, Set<Duty>> stage = new HashMap<>();
		stage.put(EntityEvent.CREATE, duties);
		stage.put(EntityEvent.REMOVE, Collections.emptySet());
		
		final ShardingState table = emptyTableWithShards(p1, 4000, 500, 10);
		final Set<Shard> shards = new HashSet<>();
		table.getCommitedState().findShards(null, shards::add);
		final Migrator migra = MigratorTest.migrator(shards, ents, p1);
		new FairWeightBalancer().balance(p1, stageFromTable(table), stage, migra);		
		assertTrue(migra.getOverrides().size()==3);
		
		// Using Dispersion.EVEN
		// attention: contrary to intuition, logical scattering will not be achieved
		// by the FairWeightBalancer unless total duty weight is almost close to the cluster capacity
		// balancer's fairness algorithm tries to spill according to shard's capacities
		// bigger shards will get most of the weightL: when total duty weight is far from cluster capacity
		// more coherent scattering will be achieved: when total duty weight is almost close teo cluster capacity
		// see: FairWeightBalancer:192
		
		
		for (Override o: migra.getOverrides()) {
			if (o.getShard().getShardID().getId().equals("3")) {
				assertTrue("right size of tiny duties didnt enter the smallest shard", 
					o.getEntities().stream()
						.filter(e->e.getDuty().getId().startsWith("tiny"))
						.count()==3);
			} else if (o.getShard().getShardID().getId().equals("2")) {
				assertTrue("middle shard capacity didnt got the remainder of tiny duties", 
					o.getEntities().stream()
						.filter(e->e.getDuty().getId().startsWith("tiny"))
						.count()==1);
			} else if (o.getShard().getShardID().getId().equals("1")) {
				assertTrue("biggest shard didnt got the right size of heavy and middle duties",
					o.getEntities().stream()
						.filter(e->e.getDuty().getId().startsWith("heavy") ||
								e.getDuty().getId().startsWith("middle"))
						.count()==3);
			}
		}

	}
	
	
	
	@Test
	public void testBalancedScattering() throws Exception {
		// some basic domain
		final Set<ShardEntity> ents = someEntitiesWithOddOrder();
		final Set<Duty> duties = dutiesFromEntities(ents);		
		final Pallet p1 = Pallet.builder("1")
				.with(new FairWeightBalancer.Metadata(FairWeightBalancer.Dispersion.EVEN, PreSort.WEIGHT))
				.build();
		
		// only creations at backstage
		final Map<EntityEvent, Set<Duty>> stage = new HashMap<>();
		stage.put(EntityEvent.CREATE, duties);
		stage.put(EntityEvent.REMOVE, Collections.emptySet());
		
		final ShardingState table = emptyTableWithShards(p1, 2900, 500, 12);
		
		final Set<Shard> shards = new HashSet<>();
		table.getCommitedState().findShards(null, shards::add);
		final Migrator migra = MigratorTest.migrator(shards, ents, p1);
		new FairWeightBalancer().balance(p1, stageFromTable(table), stage, migra);		
		assertTrue(migra.getOverrides().size()==3);
		
		// this case shows cluster capacity slightly over total duty weight
		// which scatters the duty more close to intuition
		// this's due to balancer's fairness algorithm, which is trying
		// to fullfill the cluster so all shards receive proportional weight
		// causing all the shards to reach max capacity at the same time
		// instead of the smaller shards to reach limit early on, than bigger ones.
		
		for (Override o: migra.getOverrides()) {
			if (o.getShard().getShardID().getId().equals("3")) {
				assertTrue("smallest shard didt got all the tiny duties", 
					o.getEntities().stream()
						.filter(e->e.getDuty().getId().startsWith("tiny"))
						.count()==4);
			} else if (o.getShard().getShardID().getId().equals("2")) {
				assertTrue("middle shard didnt got the only one middle duty", 
					o.getEntities().stream()
						.filter(e->e.getDuty().getId().startsWith("middle"))
						.count()==1);
			} else if (o.getShard().getShardID().getId().equals("1")) {
				assertTrue("biggest shard didnt got the heaviest duties", 
					o.getEntities().stream()
						.filter(e->e.getDuty().getId().startsWith("heavy"))
						.count()==2);
			}
		}

	}
	
	
	/** @return shards id 1...N */
	public ShardingState emptyTableWithShards(
			final Pallet capacityPallet, 
			final double...shardCapacities) throws Exception {
		
		final ShardingState ret = new ShardingState();
		int id = 1;
		for (final double cap: shardCapacities) {
			final Shard shard = ShardTest.buildShard(capacityPallet, cap, id++);
			ret.getCommitedState().addShard(shard);
		}
		return ret;
	}
	
	public Map<Spot, Set<Duty>> stageFromTable(final ShardingState table) {
		final Map<Spot, Set<Duty>> ret = new HashMap<>();
		table.getCommitedState().findShards(null, shard-> {
			ret.put(new Spot(shard), Collections.emptySet());
		});
		return ret;
	}
	

	@Test
	public void testOther() throws Exception {
		// some basic domain
		final Set<ShardEntity> ents = dutiesWithWeights(6, 6, 6, 6);
		final Set<Duty> duties = dutiesFromEntities(ents);		
		final Pallet p1 = Pallet.builder("1")
				.with(new FairWeightBalancer.Metadata(FairWeightBalancer.Dispersion.EVEN, PreSort.DATE))
				.build();
		
		// only creations at backstage
		final Map<EntityEvent, Set<Duty>> stage = new HashMap<>();
		stage.put(EntityEvent.CREATE, duties);
		stage.put(EntityEvent.REMOVE, Collections.emptySet());
		
		final ShardingState table = emptyTableWithShards(p1, 10, 10, 10, 10);
		
		final Set<Shard> shards = new HashSet<>();
		table.getCommitedState().findShards(null, shards::add);
		final Migrator migra = MigratorTest.migrator(shards, ents, p1);
		new FairWeightBalancer().balance(p1, stageFromTable(table), stage, migra);		
		assertTrue(migra.getOverrides().size()>=3);
		
		// this case shows cluster capacity slightly over total duty weight
		// which scatters the duty more close to intuition
		// this's due to balancer's fairness algorithm, which is trying
		// to fullfill the cluster so all shards receive proportional weight
		// causing all the shards to reach max capacity at the same time
		// instead of the smaller shards to reach limit early on than bigger ones.
		
		for (Override o: migra.getOverrides()) {			
			if (o.getShard().getShardID().getId().equals("1")) {
				assertTrue(o.getEntities().stream()
						.filter(e->e.getDuty().getId().equals("1"))
						.count()==1);
			} else if (o.getShard().getShardID().getId().equals("2")) {
				assertTrue(o.getEntities().stream()
						.filter(e->e.getDuty().getId().startsWith("2"))
						.count()==1);
			} else if (o.getShard().getShardID().getId().equals("3")) {
				assertTrue(o.getEntities().stream()
						.filter(e->e.getDuty().getId().startsWith("3"))
						.count()==1);
			} else if (o.getShard().getShardID().getId().equals("4")) {
				System.out.println(o.getEntities());
				assertTrue(o.getEntities().stream()
						.filter(e->e.getDuty().getId().startsWith("4"))
						.count()==1);
			}
			
		}

	}

}
