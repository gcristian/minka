package io.tilt.minka;

import static io.tilt.minka.api.ReplyResult.ERROR_ENTITY_ALREADY_EXISTS;
import static io.tilt.minka.api.ReplyResult.ERROR_ENTITY_NOT_FOUND;
import static io.tilt.minka.api.ReplyResult.SUCCESS;
import static io.tilt.minka.api.ReplyResult.SUCCESS_OPERATION_ALREADY_SUBMITTED;
import static io.tilt.minka.api.ReplyResult.SUCCESS_SENT;
import static java.util.Collections.emptySet;
import static java.util.Collections.singleton;
import static org.junit.Assert.assertEquals;

import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Test;

import com.google.common.collect.Sets;

import io.tilt.minka.api.Client;
import io.tilt.minka.api.Config;
import io.tilt.minka.api.Duty;
import io.tilt.minka.api.Pallet;

@SuppressWarnings({"unchecked", "rawtypes"})
public class ClientTest {


	@Test
    public void test_start_full_then_remove_add() throws Exception {

		final Pallet<String> p = Pallet.<String>builder("p-tsftra").build();
		final Set<Pallet<String>> pallets = Sets.newHashSet(p);
		final Set<Duty<String>> duties = TestUtils.duties(p, 12);

		final Config proto = TestUtils.prototypeConfig();
		final long distroWait = proto.beatToMs(10);
		final Set<ServerWhitness> cluster = TestUtils.buildCluster(4, proto, pallets, duties);

		Thread.sleep(distroWait * 5);
		
		final ServerWhitness lead = TestUtils.giveMeAServer(cluster, true);
		
		// =======================================================================
		
		// remove unexisting pallet
		final Client<String, String> leaderCli = lead.getServer().getClient();
		assertEquals(SUCCESS, leaderCli.remove(p).getCause());
		// add new pallet
		assertEquals(SUCCESS, leaderCli.add(p).getCause());
		// add duties
		leaderCli.addAll((Collection)duties, (r)-> assertEquals(ERROR_ENTITY_ALREADY_EXISTS, r.getCause()));
		
		// wait for attaches to happen
		Thread.sleep(distroWait);
		TestUtils.assertCRUDExecuted(TestUtils.Type.add, cluster, duties);
		
		TestUtils.cleanWhitnesses(cluster);
		// remove duties
		leaderCli.removeAll((Collection)duties, (r)-> assertEquals(SUCCESS, r.getCause()));
		// wait for detaches to happen
		Thread.sleep(distroWait);
		// remove again duties		
		leaderCli.removeAll((Collection)duties, (r)-> assertEquals(ERROR_ENTITY_NOT_FOUND, r.getCause()));
		TestUtils.assertCRUDExecuted(TestUtils.Type.remove, cluster, duties);
		
		TestUtils.shutdownServers(cluster);
	}
	
	@Test
    public void test_start_empty_then_add_and_remove() throws Exception {
		
		final Pallet<String> p = Pallet.<String>builder("p-tsetaar").build();
		final Set<Pallet<String>> pallets = Sets.newHashSet(p);
		final Set<Duty<String>> duties = TestUtils.duties(p, 12);
		
		final Config proto = TestUtils.prototypeConfig();
		final long distroWait = proto.beatToMs(10);
		final Set<ServerWhitness> cluster = TestUtils.buildCluster(4, proto, emptySet(), emptySet());

		Thread.sleep(distroWait * 2);
		final ServerWhitness lead = TestUtils.giveMeAServer(cluster, true);
		
		// =======================================================================
		
		// remove unexisting pallet
		final Client<String, String> leaderCli = lead.getServer().getClient();
		assertEquals(ERROR_ENTITY_NOT_FOUND, leaderCli.remove(p).getCause());
		// add new pallet
		assertEquals(SUCCESS, leaderCli.add(p).getCause());
		// set capacities for each server
		for (ServerWhitness sw: cluster) {
			pallets.forEach(pallet->sw.getServer().getEventMapper().setCapacity(pallet, 100));
		}		
		
		// add duties
		leaderCli.addAll((Collection)duties, (r)-> assertEquals(SUCCESS, r.getCause()));
		// wait for attaches to happen
		Thread.sleep(distroWait);
		TestUtils.assertCRUDExecuted(TestUtils.Type.add, cluster, duties);
		
		TestUtils.cleanWhitnesses(cluster);
		// remove duties
		leaderCli.removeAll((Collection)duties, (r)-> assertEquals(SUCCESS, r.getCause()));
		// wait for detaches to happen
		Thread.sleep(distroWait);

		// remove again duties		
		leaderCli.removeAll((Collection)duties, (r)-> assertEquals(ERROR_ENTITY_NOT_FOUND, r.getCause()));
		TestUtils.assertCRUDExecuted(TestUtils.Type.remove, cluster, duties);
		
		TestUtils.shutdownServers(cluster);

	}

	//@Test
	public void test_many_servers_many_distributed_crud() throws Exception {

	}
	@Test
	public void test_distributed_additions_and_servers_decay() throws Exception {
		
		final Pallet<String> p = Pallet.<String>builder("p-tdaasd").build();
		final int serverSize = 4;
		final int dutySizeLoop = 10;
		final int loops = 4;
		final int sizeAll = serverSize * dutySizeLoop * loops;
		
		final Config proto = TestUtils.prototypeConfig();
		final long distroWait = proto.beatToMs(10);
		final Set<ServerWhitness> cluster = TestUtils.buildCluster(serverSize, proto, singleton(p), emptySet());

		Thread.sleep(distroWait * 3);
		
		// add 10 duty 4 times to all servers
		final AtomicInteger id = new AtomicInteger();
		for (int j=0; j<loops; j++) {
			for (ServerWhitness sw: cluster) {
				for (int i = 0 ; i < dutySizeLoop; i ++) {
					final Client<String, String> cli = sw.getServer().getClient();
					assertEquals(
							cli.isCurrentLeader() ? SUCCESS : SUCCESS_SENT,
							cli.add(TestUtils.duty(p, id.incrementAndGet())).getCause());
				}
			}
		}
		
		// wait a reasonable time for distribution to happen
		Thread.sleep(distroWait * 4);
		
		// default balancer will evenly spread duties to all servers
		final Set<Integer> numbers = new HashSet<>();
		for (ServerWhitness sw: cluster) {
			final int x = sw.getCaptured().size();
			if (x==39 || x==41 || x ==0) {
				int i = 9;
			}
			assertEquals(dutySizeLoop * loops, x);
			assertEquals(0l, sw.getReleased().size());
			// record them
			sw.getCaptured().forEach(d->numbers.add(Integer.parseInt(d.getId())));
		}
		
		assertEquals(numbers.size(), sizeAll);
		
		// cut half cluster without the leader
		int cuttingSize = serverSize / 2;
		final Iterator<ServerWhitness> it = cluster.iterator();
		while (it.hasNext()) {
			final ServerWhitness sw = it.next();
			if (cuttingSize>0) {
				if (!sw.getServer().getClient().isCurrentLeader()) {
					cuttingSize--;
					sw.getServer().shutdown();
					it.remove();
				}
			} else {
				break;
			}
		}

		// wait a reasonable time for distribution to happen
		Thread.sleep(distroWait * 5);

		// at least half the duties has been reassigned
		numbers.clear();
		for (ServerWhitness sw: cluster) {
			sw.getCaptured().forEach(d->numbers.add(Integer.parseInt(d.getId())));
		}
		// all duties still assigned
		assertEquals(numbers.size(), sizeAll);
		
		// kill last follower
		final ServerWhitness follower = TestUtils.giveMeAServer(cluster, false);
		follower.getServer().shutdown();
		cluster.remove(follower);
		
		// wait for reassign
		Thread.sleep(distroWait * 5);
		
		// the only shard standing is the leader and has all the duties so far
		assertEquals(TestUtils.giveMeAServer(cluster, true).getCaptured().size(), sizeAll);
		
		TestUtils.shutdownServers(cluster);
	}
	
	
	@Test
    public void test_start_empty_then_repeat_add_and_removes() throws Exception {
		
		final Pallet<String> p = Pallet.<String>builder("p-tsetraar").build();
		final Set<Pallet<String>> pallets = Sets.newHashSet(p);
		final Set<Duty<String>> duties = TestUtils.duties(p, 12);
		
		final Config proto = TestUtils.prototypeConfig();
		final long distroWait = proto.beatToMs(10);
		final Set<ServerWhitness> cluster = TestUtils.buildCluster(4, proto, emptySet(), emptySet());

		Thread.sleep(distroWait * 2);
		
		final ServerWhitness lead = TestUtils.giveMeAServer(cluster, true);
		
		// =======================================================================
		
		// remove unexisting pallet
		final Client<String, String> leaderCli = lead.getServer().getClient();
		assertEquals(ERROR_ENTITY_NOT_FOUND, leaderCli.remove(p).getCause());
		// add new pallet
		assertEquals(SUCCESS, leaderCli.add(p).getCause());
		
		// set capacities for each server
		for (ServerWhitness sw: cluster) {
			pallets.forEach(pallet->sw.getServer().getEventMapper().setCapacity(pallet, 100));
		}
		
		// add an already added pallet
		assertEquals(ERROR_ENTITY_ALREADY_EXISTS, leaderCli.add(p).getCause());
		
		// add duties
		leaderCli.addAll((Collection)duties, (r)-> assertEquals(SUCCESS, r.getCause()));
		// add already added duties
		leaderCli.addAll((Collection)duties, (r)-> assertEquals(SUCCESS_OPERATION_ALREADY_SUBMITTED, r.getCause()));
		// wait for attaches to happen
		Thread.sleep(distroWait);

		// add already added duties	
		leaderCli.addAll((Collection)duties, (r)-> assertEquals(ERROR_ENTITY_ALREADY_EXISTS, r.getCause()));
		TestUtils.assertCRUDExecuted(TestUtils.Type.add, cluster, duties);
		
		TestUtils.cleanWhitnesses(cluster);
		// remove duties
		leaderCli.removeAll((Collection)duties, (r)-> assertEquals(SUCCESS, r.getCause()));
		// remove again d uties
		leaderCli.removeAll((Collection)duties, (r)-> assertEquals(SUCCESS_OPERATION_ALREADY_SUBMITTED, r.getCause()));
		// wait for detaches to happen
		Thread.sleep(distroWait);

		// remove again duties
		leaderCli.removeAll((Collection)duties, (r)-> assertEquals(ERROR_ENTITY_NOT_FOUND, r.getCause()));
		TestUtils.assertCRUDExecuted(TestUtils.Type.remove, cluster, duties);
		
		TestUtils.shutdownServers(cluster);
	}
	
}
 