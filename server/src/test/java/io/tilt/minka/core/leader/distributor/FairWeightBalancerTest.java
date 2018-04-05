package io.tilt.minka.core.leader.distributor;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

import org.junit.Test;

import io.tilt.minka.ShardTest;
import io.tilt.minka.api.Duty;
import io.tilt.minka.api.DutyBuilder;
import io.tilt.minka.api.Pallet;
import io.tilt.minka.api.PalletBuilder;
import io.tilt.minka.core.leader.PartitionTable;
import io.tilt.minka.core.leader.balancer.FairWeightBalancer;
import io.tilt.minka.core.leader.distributor.Balancer.NetworkLocation;
import io.tilt.minka.domain.EntityEvent;
import io.tilt.minka.domain.Shard;
import io.tilt.minka.domain.ShardEntity;

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
	
	public static Duty<String> buildDutyWithWeight(long weight, String idi) {
		return DutyBuilder.<String>
			builder(idi, "1")
				.with(weight)
				.build();
	}

	public static Set<Duty<?>> dutiesFromEntities(final Set<ShardEntity> set) {
		final Set<Duty<?>> duties = new HashSet<>(
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
		final Set<Duty<?>> duties = dutiesFromEntities(ents);		
		final Pallet p1 = PalletBuilder.builder("1")
				.with(new FairWeightBalancer.Metadata())
				.build();
		
		// only creations at backstage
		final Map<EntityEvent, Set<Duty<?>>> backstage = new HashMap<>();
		backstage.put(EntityEvent.CREATE, duties);
		backstage.put(EntityEvent.REMOVE, Collections.emptySet());
		
		final PartitionTable table = emptyTableWithShards(p1, 5000, 5000);
		
		final Migrator migra1 = MigratorTest.migrator(new HashSet<>(table.getScheme().getShards()), ents, p1);
		new FairWeightBalancer().balance(p1, stageFromTable(table), backstage, migra1);		
		final Override v1o1 = migra1.getOverrides().get(0);
		final Override v1o2 = migra1.getOverrides().get(1);

		final Migrator migra2 = MigratorTest.migrator(new HashSet<>(table.getScheme().getShards()), ents, p1);
		new FairWeightBalancer().balance(p1, stageFromTable(table), backstage, migra2);
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
		final Set<Duty<?>> duties = dutiesFromEntities(ents);		
		final Pallet p1 = PalletBuilder.builder("1")
				.with(new FairWeightBalancer.Metadata())
				.build();
		
		// only creations at backstage
		final Map<EntityEvent, Set<Duty<?>>> backstage = new HashMap<>();
		backstage.put(EntityEvent.CREATE, duties);
		backstage.put(EntityEvent.REMOVE, Collections.emptySet());
		
		// only empty shards at scheme
		final PartitionTable table = emptyTableWithShards(p1, 5000, 5000, 5000);
		final Migrator migra1 = MigratorTest.migrator(new HashSet<>(table.getScheme().getShards()), ents, p1);
		new FairWeightBalancer().balance(p1, stageFromTable(table), backstage, migra1);		
		if (migra1.getOverrides().size()!=3) {
			int u = 0;
		}
		for (final Override o: migra1.getOverrides()) {
			assertTrue("balancer didnt returned a fair amount of duties when all same weight", 
					o.getEntities().size()==4);
		}
		
		assertFalse(migra1.getOverrides().get(0).equals(migra1.getOverrides().get(1)));
		assertFalse(migra1.getOverrides().get(0).equals(migra1.getOverrides().get(2)));
		assertFalse(migra1.getOverrides().get(1).equals(migra1.getOverrides().get(2)));

	}

	
	@Test
	public void testLowScattering() throws Exception {
		// some basic domain
		
		final Set<ShardEntity> ents = someEntitiesWithOddOrder();
		final Set<Duty<?>> duties = dutiesFromEntities(ents);		
		final Pallet p1 = PalletBuilder.builder("1")
				.with(new FairWeightBalancer.Metadata(FairWeightBalancer.Dispersion.EVEN, Balancer.PreSort.WEIGHT))
				.build();
		
		// only creations at backstage
		final Map<EntityEvent, Set<Duty<?>>> backstage = new HashMap<>();
		backstage.put(EntityEvent.CREATE, duties);
		backstage.put(EntityEvent.REMOVE, Collections.emptySet());
		
		final PartitionTable table = emptyTableWithShards(p1, 4000, 500, 10);
		final Migrator migra = MigratorTest.migrator(new HashSet<>(table.getScheme().getShards()), ents, p1);
		new FairWeightBalancer().balance(p1, stageFromTable(table), backstage, migra);		
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
		final Set<Duty<?>> duties = dutiesFromEntities(ents);		
		final Pallet p1 = PalletBuilder.builder("1")
				.with(new FairWeightBalancer.Metadata(FairWeightBalancer.Dispersion.EVEN, Balancer.PreSort.WEIGHT))
				.build();
		
		// only creations at backstage
		final Map<EntityEvent, Set<Duty<?>>> backstage = new HashMap<>();
		backstage.put(EntityEvent.CREATE, duties);
		backstage.put(EntityEvent.REMOVE, Collections.emptySet());
		
		final PartitionTable table = emptyTableWithShards(p1, 2900, 500, 12);
		
		final Migrator migra = MigratorTest.migrator(new HashSet<>(table.getScheme().getShards()), ents, p1);
		new FairWeightBalancer().balance(p1, stageFromTable(table), backstage, migra);		
		assertTrue(migra.getOverrides().size()==3);
		
		// this case shows cluster capacity slightly over total duty weight
		// which scatters the duty more close to intuition
		// this's due to balancer's fairness algorithm, which is trying
		// to fullfill the cluster so all shards receive proportional weight
		// causing all the shards to reach max capacity at the same time
		// instead of the smaller shards to reach limit early on than bigger ones.
		
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
	public PartitionTable emptyTableWithShards(
			final Pallet capacityPallet, 
			final double...shardCapacities) throws Exception {
		
		final PartitionTable ret = new PartitionTable();
		int id = 1;
		for (final double cap: shardCapacities) {
			final Shard shard = ShardTest.buildShard(capacityPallet, cap, id++);
			ret.getScheme().addShard(shard);
		}
		return ret;
	}
	
	public Map<NetworkLocation, Set<Duty<?>>> stageFromTable(final PartitionTable table) {
		final Map<NetworkLocation, Set<Duty<?>>> ret = new HashMap<>();
		for (final Shard shard: table.getScheme().getShards()) {
			ret.put(new NetworkLocation(shard), Collections.emptySet());
		}
		return ret;
	}
	

	@Test
	public void testOther() throws Exception {
		// some basic domain
		final Set<ShardEntity> ents = dutiesWithWeights(2, 2, 2, 2);
		final Set<Duty<?>> duties = dutiesFromEntities(ents);		
		final Pallet p1 = PalletBuilder.builder("1")
				.with(new FairWeightBalancer.Metadata(FairWeightBalancer.Dispersion.EVEN, Balancer.PreSort.DATE))
				.build();
		
		// only creations at backstage
		final Map<EntityEvent, Set<Duty<?>>> backstage = new HashMap<>();
		backstage.put(EntityEvent.CREATE, duties);
		backstage.put(EntityEvent.REMOVE, Collections.emptySet());
		
		final PartitionTable table = emptyTableWithShards(p1, 10, 10, 10, 10);
		System.out.println(new HashSet<>(table.getScheme().getShards()));

		final Migrator migra = MigratorTest.migrator(new HashSet<>(table.getScheme().getShards()), ents, p1);
		new FairWeightBalancer().balance(p1, stageFromTable(table), backstage, migra);		
		assertTrue(migra.getOverrides().size()==4);
		
		// this case shows cluster capacity slightly over total duty weight
		// which scatters the duty more close to intuition
		// this's due to balancer's fairness algorithm, which is trying
		// to fullfill the cluster so all shards receive proportional weight
		// causing all the shards to reach max capacity at the same time
		// instead of the smaller shards to reach limit early on than bigger ones.
		
		for (Override o: migra.getOverrides()) {
			//System.out.println(o.getShard().getShardID().getStringIdentity() + " : " + o.getEntities());
			/**
			if (o.getShard().getShardID().getStringIdentity().equals("1")) {
				assertTrue(o.getEntities().stream()
						.filter(e->e.getDuty().getId().equals("1"))
						.count()==1);
			} else if (o.getShard().getShardID().getStringIdentity().equals("2")) {
				assertTrue(o.getEntities().stream()
						.filter(e->e.getDuty().getId().startsWith("2"))
						.count()==1);
			} else if (o.getShard().getShardID().getStringIdentity().equals("3")) {
				assertTrue(o.getEntities().stream()
						.filter(e->e.getDuty().getId().startsWith("3"))
						.count()==1);
			} else if (o.getShard().getShardID().getStringIdentity().equals("4")) {
				assertTrue(o.getEntities().stream()
						.filter(e->e.getDuty().getId().startsWith("4"))
						.count()==1);
			}
			*/
		}

	}

}
