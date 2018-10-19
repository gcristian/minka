package io.tilt.minka;

import static com.google.common.collect.Sets.newHashSet;
import static io.tilt.minka.TestUtils.assertDistribution;
import static io.tilt.minka.TestUtils.balancers;
import static io.tilt.minka.TestUtils.buildCluster;
import static io.tilt.minka.TestUtils.createServer;
import static io.tilt.minka.TestUtils.duties;
import static io.tilt.minka.TestUtils.pickAServer;
import static io.tilt.minka.TestUtils.prototypeConfig;
import static io.tilt.minka.TestUtils.shutdownServers;
import static java.lang.Thread.sleep;
import static java.util.Arrays.asList;
import static java.util.Collections.emptySet;
import static junit.framework.Assert.assertNotNull;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.junit.Test;

import io.tilt.minka.api.Config;
import io.tilt.minka.api.Server;
import io.tilt.minka.core.leader.balancer.Balancer.BalancerMetadata;
import io.tilt.minka.model.Duty;
import io.tilt.minka.model.Pallet;


public class ClusterResilienceTest {

	private final Pallet p = Pallet.builder("p-huc").build();
	private final Set<Pallet> pallets = newHashSet(p);
	private final Set<Duty> duties = duties(p, 50);
	private final Config proto = prototypeConfig();
	private final long wait = proto.beatToMs(10);

	@Test
	/**
	 * proves there's always a leader
	 * proves the leader learns the distribution status from surviving followers
	 * proves followers redirect their beats to the new leader
	 * proves duties are always distributed at anytime, though in different shards 
	 */
	public void test_jumping_leader() throws Exception {
		proto.getBootstrap().setNamespace("test_jumping_leader");
		int size = 4;
		
		final Set<ServerWhitness> cluster = buildCluster(size, proto, pallets, duties);	
		sleep(wait * 4);
		
		ServerWhitness leader = null;
		for (int i = 0; i < size-1; i++) {
			if (leader==null) {
				leader = pickAServer(cluster, true);
			}
			// prove leaders are being elected in order of apparition
			assertEquals("iteration " + i, String.valueOf(i), leader.getServer().getConfig().getBootstrap().getServerTag());
			leader.getServer().shutdown();
			sleep(wait * 5);
			cluster.remove(leader);
			assertDistribution(cluster, duties);
			// prove there is a leader
			assertNotNull("iteration " + i, (leader = pickAServer(cluster, true))!=null);
		}
		leader.getServer().shutdown();
	}
		
	@Test
	public void test_cluster_falling_apart_all_balancers() throws Exception {
		for (BalancerMetadata meta: balancers()) {
			test_cluster_falling_apart(meta);
		}
	}
	
	
	/**
	 * proves a decaying cluster of all its members but the leader
	 * while all duties still kept distributed
	 */
	public void test_cluster_falling_apart(final BalancerMetadata meta) throws Exception {
		
		proto.getBootstrap().setNamespace("test_cluster_falling_apart");
		final Set<ServerWhitness> set = buildCluster(5, proto, pallets, duties);
		sleep(wait * 3);
		
		final ServerWhitness leader = pickAServer(set, true);
		
		final Iterator<ServerWhitness> it = set.iterator();
		while (it.hasNext()) {
			final ServerWhitness sw = it.next();
			if (!sw.getServer().getClient().isCurrentLeader()) {
				sw.getServer().shutdown();
				it.remove();
				sleep(wait * 5);
				// in anytime all duties must be kept distributed 
				assertDistribution(set, duties);
			}
		}
		
		// finally the only standing shard has them all
		assertDistribution(Arrays.asList(leader), duties);
		leader.getServer().shutdown();
	}
	
	@Test
	/**
	 * proves shards may appear and disappear anytime in the same time
	 * while all duties are still kept distributed
	 * proves proctor and distributor phases are never suspended
	 * proves the leader acknowledges every situation
	 */
	public void test_cluster_highly_unstable() throws Exception {
		proto.getBootstrap().setNamespace("test_cluster_highly_unstable");
		// start with 3 (surely 1st will be leader)
		final ServerWhitness a1 = createServer(1, proto, duties, pallets, "a1");
		final ServerWhitness a2 = createServer(2, proto, duties, pallets, "a2");
		final ServerWhitness a3 = createServer(3, proto, duties, pallets, "a3");
		sleep(wait * 3);
		assertDistribution(asList(a1,a2,a3), duties);
	
		a2.getServer().shutdown();
		// immediately launch a new serie of shards
		final ServerWhitness a4 = createServer(4, proto, duties, pallets, "a4");
		sleep(wait * 5);
		
		assertDistribution(asList(a1,a3,a4), duties);
		
		a3.getServer().shutdown();
		final ServerWhitness a5 = createServer(5, proto, duties, pallets, "a5");
		final ServerWhitness a6 = createServer(6, proto, duties, pallets, "a6");
		sleep(wait * 3);
		
		assertDistribution(asList(a1,a4,a5,a6), duties);
		
		a5.getServer().shutdown();
		a6.getServer().shutdown();
		sleep(wait * 3);		
		
		assertDistribution(asList(a1,a4), duties);

		a4.getServer().shutdown();
		
		sleep(wait * 3);
		
		assertTrue(a1.getServer().getClient().isCurrentLeader());
		assertEquals(duties, a1.getCurrent());
		
		assertDistribution(asList(a1), duties);
		
		a1.getServer().shutdown();
	}

	@Test
	/**
	 * proves replication feature works well that is, client posted CRUD after 1st
	 * distribution (duties loaded from delegates) survive the fall of their current
	 * leader, and get accounted as a previous state and distribution by the new
	 * leader.
	 */
	public void test_duties_after_load_survive_leader_fall_smoke() throws Exception {
		proto.getBootstrap().setNamespace("tito-testito");
		// start with 3 (surely 1st will be leader)
		
		final Set<ServerWhitness> set = buildCluster(2, proto, pallets, emptySet());
		final Collection<Duty> duties = duties(p, 20);
		
		sleep(wait * 5);
		
		set.iterator().next().getServer().getClient().fireAndForget().addAll(duties);
		
		sleep(wait * 5);
 		//assertDistribution(set, duties);
	
		final ServerWhitness leader = TestUtils.pickAServer(set, true);
		leader.getServer().shutdown();
		
		sleep(wait * 10);
		set.remove(leader);
		assertDistribution(set, duties);
				
		shutdownServers(set);
	}

	@Test
	public void test_duties_after_load_survive_leader_fall_heavy() throws Exception {
		proto.getBootstrap().setNamespace("tito-testito");
		
		final int servers = 5;
		final Set<ServerWhitness> set = buildCluster(servers, proto, pallets, emptySet());
		final Collection<Duty> duties = duties(p, 10);
		sleep(wait * 10);
		set.iterator().next().getServer().getClient().fireAndForget().addAll(duties);
		for (int i = 0 ; i < servers; i ++) {
			sleep(wait * 10);
	 		assertDistribution(set, duties);
			final ServerWhitness leader = TestUtils.pickAServer(set, true);
			leader.getServer().shutdown();
			set.remove(leader);
		}
		
 		//assertDistribution(set, duties);		
		//shutdownServers(set);
	}

	@Test
	public void test_cluster_size_increase_decrease_all_balancers() throws Exception {
		for (BalancerMetadata meta: balancers()) {
			test_cluster_size_increase_decrease(meta);
		}
	}
	
	/**
	 * proves the cluster increases and decreases members
	 * but all duties are always distributed
	 * proves the distribution is never suspended 
	 */
	public void test_cluster_size_increase_decrease(final BalancerMetadata meta) throws Exception {
		final Pallet p = Pallet.builder("p-huc").with(meta).build();
		final Set<Pallet> pallets = newHashSet(p);

		int count = 1;
		proto.getBootstrap().setNamespace("test_cluster_size_increase_decrease");
		final Set<ServerWhitness> cluster = buildCluster(count, proto, pallets, duties);
		
		sleep(wait * 3); 
		
		// then increase 1 server each period 
		for (;count < 10; count++) {
			cluster.add(createServer(count, proto, duties, pallets, String.valueOf(count)));
			sleep(wait * 3);
		}
		
		sleep(wait * 3);
		
		// all duties must be uniquely distributed
		final Set<Duty> current = new HashSet<>();
		for (ServerWhitness sw: cluster) {
			assertTrue(sw.getEverCaptured().size()>0);
			current.addAll(sw.getCurrent());
		}
		
		assertEquals(duties, current);
		
		// then decrease 1 server each period
		final Iterator<ServerWhitness> it = cluster.iterator();
		while (it.hasNext()) {
			final ServerWhitness sw = it.next();			
			if (!sw.getServer().getClient().isCurrentLeader()) {
				sw.getServer().shutdown();
				it.remove();
				sleep(wait * 3);
			}
		}
		
		sleep(wait * 5);
		
		// last standing server must have assigned all duties from above quitting servers
		
		assertEquals(1, cluster.size());
		assertEquals(duties.size(), cluster.iterator().next().getCurrent().size());
		assertEquals(duties, cluster.iterator().next().getCurrent());
		
		//Thread.sleep(60 * 60 * 1000);
		
		shutdownServers(cluster, true);
	}

	@Test
	public void test_single_cluster() throws Exception {
		final Config conf = prototypeConfig();
		conf.getBootstrap().setNamespace("test_single_cluster");
		conf.getBootstrap().setEnableCoreDump(true);
		conf.getBootstrap().setCoreDumpFrequency(20);
		conf.getBootstrap().setCoreDumpFilepath("/tmp/minka");
		final Set<ServerWhitness> set = buildCluster(1, conf, pallets, duties);
		sleep(wait * 3);
		assertDistribution(set, duties);
		shutdownServers(set);
	}

}
 