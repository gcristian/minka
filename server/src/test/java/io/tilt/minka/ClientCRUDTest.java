package io.tilt.minka;

import static com.google.common.collect.Sets.newHashSet;
import static io.tilt.minka.TestUtils.assertCRUDExecuted;
import static io.tilt.minka.TestUtils.buildCluster;
import static io.tilt.minka.TestUtils.cleanWhitnesses;
import static io.tilt.minka.TestUtils.duties;
import static io.tilt.minka.TestUtils.duty;
import static io.tilt.minka.TestUtils.pickAServer;
import static io.tilt.minka.TestUtils.prototypeConfig;
import static io.tilt.minka.TestUtils.shutdownServers;
import static io.tilt.minka.api.ReplyValue.ERROR_ENTITY_ALREADY_EXISTS;
import static io.tilt.minka.api.ReplyValue.ERROR_ENTITY_NOT_FOUND;
import static io.tilt.minka.api.ReplyValue.SUCCESS;
import static io.tilt.minka.api.ReplyValue.SUCCESS_OPERATION_ALREADY_SUBMITTED;
import static io.tilt.minka.api.ReplyValue.SENT_SUCCESS;
import static java.lang.Thread.sleep;
import static java.util.Collections.emptySet;
import static java.util.Collections.singleton;
import static org.junit.Assert.assertEquals;

import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Test;

import io.tilt.minka.api.Client;
import io.tilt.minka.api.Config;
import io.tilt.minka.api.Duty;
import io.tilt.minka.api.Pallet;

public class ClientCRUDTest {

	@Test
    public void test_smoke() throws Exception {

		final Pallet p = Pallet.builder("p-tsftra").build();
		final Set<Pallet> pallets = newHashSet(p);
		final Set<Duty> duties = duties(p, 12);

		final Config proto = prototypeConfig();
		proto.getBootstrap().setBeatUnitMs(300);
		proto.getBootstrap().setNamespace("smoke-test");
		final long distroWait = proto.beatToMs(10);
		final Set<ServerWhitness> cluster = buildCluster(3, proto, pallets, duties);
		sleep(1000* 60 * 60);
	}
    
	@Test
    public void test_start_full_then_remove_add() throws Exception {

		final Pallet p = Pallet.builder("p-tsftra").build();
		final Set<Pallet> pallets = newHashSet(p);
		final Set<Duty> duties = duties(p, 12);

		final Config proto = prototypeConfig();
		proto.getBootstrap().setNamespace("test_start_full_then_remove_add");
		final long distroWait = proto.beatToMs(10);
		final Set<ServerWhitness> cluster = buildCluster(4, proto, pallets, duties);

		sleep(distroWait * 5);
		
		final ServerWhitness lead = pickAServer(cluster, true);
		
		// =======================================================================
		
		// remove unexisting pallet
		final Client leaderCli = lead.getServer().getClient();
		assertEquals(SUCCESS, leaderCli.fireAndForget().remove(p).getValue());
		// add new pallet
		assertEquals(SUCCESS, leaderCli.fireAndForget().add(p).getValue());
		// add duties
		leaderCli.fireAndForget().addAll(duties).forEach(r-> assertEquals(ERROR_ENTITY_ALREADY_EXISTS, r.getValue()));
		
		// wait for attaches to happen
		sleep(distroWait * 3);
		assertCRUDExecuted(TestUtils.Type.add, cluster, duties);
		
		cleanWhitnesses(cluster);
		// remove duties
		leaderCli.fireAndForget().removeAll(duties).forEach(r-> assertEquals(SUCCESS, r.getValue()));
		// wait for detaches to happen
		sleep(distroWait);
		// remove again duties		
		leaderCli.fireAndForget().removeAll(duties).forEach(r-> assertEquals(ERROR_ENTITY_NOT_FOUND, r.getValue()));
		assertCRUDExecuted(TestUtils.Type.remove, cluster, duties);
		
		shutdownServers(cluster, true);
	}
	
	@Test
    public void test_start_empty_then_add_and_remove() throws Exception {
		
		final Pallet p = Pallet.builder("p-tsetaar").build();
		final Set<Pallet> pallets = newHashSet(p);
		final Set<Duty> duties = duties(p, 12);
		
		final Config proto = prototypeConfig();
		proto.getBootstrap().setNamespace("test_start_empty_then_add_and_remove");
		final long distroWait = proto.beatToMs(10);
		final Set<ServerWhitness> cluster = buildCluster(4, proto, emptySet(), emptySet());

		sleep(distroWait * 2);
		final ServerWhitness lead = pickAServer(cluster, true);
		
		// =======================================================================
		
		// remove unexisting pallet
		final Client leaderCli = lead.getServer().getClient();
		assertEquals(ERROR_ENTITY_NOT_FOUND, leaderCli.fireAndForget().remove(p).getValue());
		// add new pallet
		assertEquals(SUCCESS, leaderCli.fireAndForget().add(p).getValue());
		// set capacities for each server
		for (ServerWhitness sw: cluster) {
			pallets.forEach(pallet->sw.getServer().getEventMapper().setCapacity(pallet, 100));
		}		
		
		// add duties
		leaderCli.fireAndForget().addAll(duties).forEach(r-> assertEquals(SUCCESS, r.getValue()));
		// wait for attaches to happen
		sleep(distroWait);
		assertCRUDExecuted(TestUtils.Type.add, cluster, duties);
		
		cleanWhitnesses(cluster);
		// remove duties
		leaderCli.fireAndForget().removeAll(duties).forEach(r-> assertEquals(SUCCESS, r.getValue()));
		// wait for detaches to happen
		sleep(distroWait);

		// remove again duties		
		leaderCli.fireAndForget().removeAll(duties).forEach(r-> assertEquals(ERROR_ENTITY_NOT_FOUND, r.getValue()));
		assertCRUDExecuted(TestUtils.Type.remove, cluster, duties);
		
		shutdownServers(cluster, true);
	}

	//@Test
	public void test_many_servers_many_distributed_crud() throws Exception {

	}
	@Test
	public void test_distributed_additions_and_servers_decay() throws Exception {
		
		final Pallet p = Pallet.builder("p-tdaasd").build();
		final int serverSize = 4;
		final int dutySizeLoop = 10;
		final int loops = 4;
		final int sizeAll = serverSize * dutySizeLoop * loops;
		
		final Config proto = prototypeConfig();
		proto.getBootstrap().setNamespace("test_distributed_additions_and_servers_decay");
		final long distroWait = proto.beatToMs(10);
		final Set<ServerWhitness> cluster = buildCluster(serverSize, proto, singleton(p), emptySet());

		sleep(distroWait * 3);
		
		// add 10 duty 4 times to all servers
		final AtomicInteger id = new AtomicInteger();
		for (int j=0; j<loops; j++) {
			for (ServerWhitness sw: cluster) {
				for (int i = 0 ; i < dutySizeLoop; i ++) {
					final Client cli = sw.getServer().getClient();
					assertEquals(
							cli.isCurrentLeader() ? SUCCESS : SENT_SUCCESS,
							cli.fireAndForget().add(duty(p, id.incrementAndGet())).getValue());
				}
			}
		}
		
		// wait a reasonable time for distribution to happen
		sleep(distroWait * 4);
		
		// default balancer will evenly spread duties to all servers
		final Set<Integer> numbers = new HashSet<>();
		for (ServerWhitness sw: cluster) {
			final int x = sw.getEverCaptured().size();
			if (x==39 || x==41 || x ==0) {
				int i = 9;
			}
			assertEquals(dutySizeLoop * loops, x);
			assertEquals(0l, sw.getEverReleased().size());
			// record them
			sw.getEverCaptured().forEach(d->numbers.add(Integer.parseInt(d.getId())));
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
		sleep(distroWait * 5);

		// at least half the duties has been reassigned
		numbers.clear();
		for (ServerWhitness sw: cluster) {
			sw.getEverCaptured().forEach(d->numbers.add(Integer.parseInt(d.getId())));
		}
		// all duties still assigned
		assertEquals(numbers.size(), sizeAll);
		
		// kill last follower
		final ServerWhitness follower = pickAServer(cluster, false);
		follower.getServer().shutdown();
		cluster.remove(follower);
		
		// wait for reassign
		sleep(distroWait * 5);
		
		// the only shard standing is the leader and has all the duties so far
		assertEquals(pickAServer(cluster, true).getEverCaptured().size(), sizeAll);
		
		shutdownServers(cluster, true);
	}
	
	@Test
    public void test_start_empty_then_repeat_add_and_removes() throws Exception {
		
		final Pallet p = Pallet.<String>builder("p-tsetraar").build();
		final Set<Pallet> pallets = newHashSet(p);
		final Set<Duty> duties = duties(p, 12);
		
		final Config proto = prototypeConfig();
		proto.getBootstrap().setNamespace("test_start_empty_then_repeat_add_and_removes");
		final long distroWait = proto.beatToMs(10);
		final Set<ServerWhitness> cluster = buildCluster(4, proto, emptySet(), emptySet());

		sleep(distroWait * 2);
		
		final ServerWhitness lead = pickAServer(cluster, true);
		
		// =======================================================================
		
		// remove unexisting pallet
		final Client leaderCli = lead.getServer().getClient();
		assertEquals(ERROR_ENTITY_NOT_FOUND, leaderCli.fireAndForget().remove(p).getValue());
		// add new pallet
		assertEquals(SUCCESS, leaderCli.fireAndForget().add(p).getValue());
		
		// set capacities for each server
		for (ServerWhitness sw: cluster) {
			pallets.forEach(pallet->sw.getServer().getEventMapper().setCapacity(pallet, 100));
		}
		
		// add an already added pallet
		assertEquals(ERROR_ENTITY_ALREADY_EXISTS, leaderCli.fireAndForget().add(p).getValue());
		
		// add duties
		leaderCli.fireAndForget().addAll(duties).forEach(r-> assertEquals(SUCCESS, r.getValue()));
		// add already added duties
		leaderCli.fireAndForget().addAll(duties).forEach(r-> assertEquals(SUCCESS_OPERATION_ALREADY_SUBMITTED, r.getValue()));
		// wait for attaches to happen
		sleep(distroWait);

		// add already added duties	
		leaderCli.fireAndForget().addAll(duties).forEach(r-> assertEquals(ERROR_ENTITY_ALREADY_EXISTS, r.getValue()));
		assertCRUDExecuted(TestUtils.Type.add, cluster, duties);
		
		cleanWhitnesses(cluster);
		// remove duties
		leaderCli.fireAndForget().removeAll(duties).forEach(r-> assertEquals(SUCCESS, r.getValue()));
		// remove again d uties
		leaderCli.fireAndForget().removeAll(duties).forEach(r-> assertEquals(SUCCESS_OPERATION_ALREADY_SUBMITTED, r.getValue()));
		// wait for detaches to happen
		sleep(distroWait);

		// remove again duties
		leaderCli.fireAndForget().removeAll(duties).forEach(r-> assertEquals(ERROR_ENTITY_NOT_FOUND, r.getValue()));
		assertCRUDExecuted(TestUtils.Type.remove, cluster, duties);
		
		shutdownServers(cluster, true);
	}
	
}
 