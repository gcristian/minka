package io.tilt.minka;

import static java.util.Collections.emptySet;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import org.junit.Test;

import io.tilt.minka.api.Config;
import io.tilt.minka.api.Duty;
import io.tilt.minka.api.EventMapper;
import io.tilt.minka.api.Pallet;
import io.tilt.minka.api.ReplyResult;
import io.tilt.minka.api.Server;

public class ClientTest {


	@Test
    public void start_empty_then_add_and_remove() throws Exception {

		final Config c = new Config();
		c.getBootstrap().setBeatUnitMs(100l);
		final Set<ServerWhitness> cluster = buildCluster(4, emptySet(), emptySet());
		// wait for leadership
		Thread.sleep(c.beatToMs(10));
		
		final Pallet<String> p = Pallet.<String>builder("p1").build();
		final Set<Pallet<String>> pallets = new HashSet<>();
		pallets.add(p);
		final Set<Duty<String>> duties = duties(p, 50);
		
		final ServerWhitness lead = anyServer(cluster, true);
		
		assertEquals(lead.getServer().getClient().remove(p).getCause(), ReplyResult.ERROR_ENTITY_NOT_FOUND);
		assertEquals(lead.getServer().getClient().add(p).getCause(), ReplyResult.SUCCESS);
		assertEquals(lead.getServer().getClient().add(p).getCause(), ReplyResult.ERROR_ENTITY_ALREADY_EXISTS);
		
		for (ServerWhitness w: cluster) {
			w.getServer().getEventMapper().setCapacity(p, 100);
		}
		
		lead.getServer().getClient().addAll((Collection)duties, (r)-> {
			assertEquals(ReplyResult.SUCCESS, r.getCause());
		});
		// should not allow me
		lead.getServer().getClient().addAll((Collection)duties, (r)-> {
			assertEquals(ReplyResult.SUCCESS_OPERATION_ALREADY_SUBMITTED, r.getCause());
		});

		// wait distributor
		Thread.sleep(c.beatToMs(20));

		// should not allow me
		
		lead.getServer().getClient().addAll((Collection)duties, (r)-> {
			assertEquals(ReplyResult.ERROR_ENTITY_ALREADY_EXISTS, r.getCause());
		})
		;

		Set<Duty<String>> accum = new HashSet<>(duties);
		for (ServerWhitness w: cluster) {
			assertTrue(w.getCaptured().size()>0);
			assertTrue(w.getReleased().size()==0);
			assertTrue(accum.removeAll(w.getCaptured()));
		}
		assertTrue(accum.isEmpty());
		
		cleanWhitnesses(cluster);
		
		lead.getServer().getClient().removeAll((Collection)duties, (r)->{
			System.out.println(r.getMessage());
			assertEquals(ReplyResult.SUCCESS, r.getCause());
		});
		lead.getServer().getClient().removeAll((Collection)duties, (r)->{
			System.out.println(r.getMessage());
			assertEquals(ReplyResult.SUCCESS_OPERATION_ALREADY_SUBMITTED, r.getCause());
		});
		
		// wait distributor
		Thread.sleep(c.beatToMs(10));

		
		lead.getServer().getClient().removeAll((Collection)duties, (r)->{
			System.out.println(r.getMessage());
			assertEquals(ReplyResult.ERROR_ENTITY_NOT_FOUND, r.getCause());
		});
		

		accum = new HashSet<>(duties);
		for (ServerWhitness w: cluster) {
			assertTrue(w.getCaptured().size()==0);
			assertTrue(accum.removeAll(w.getReleased()));
		}
		assertTrue(accum.isEmpty());
		
		killAllServers(cluster);
	}
	

	private static Set<Duty<String>> duties(final Pallet<String> p, final int size) {
		final Set<Duty<String>> set = new HashSet<>();
		for (int i = 0; i < size; i++) {
			set.add(Duty.<String>builder(String.valueOf(i), p.getId()).with(1).build());			
		}
		return set;
	}
    

	private static void killAllServers(final Set<ServerWhitness> cluster) {
		for (ServerWhitness w: cluster) {
			w.getServer().shutdown();
		}
	}

	private static void cleanWhitnesses(final Set<ServerWhitness> cluster) {
		for (ServerWhitness w: cluster) {
			w.getCaptured().clear();
			w.getReleased().clear();
		}
	}
	
	public static ServerWhitness anyServer(final Set<ServerWhitness> list, final boolean leader) {
		ServerWhitness ret = null;
		for (ServerWhitness w: list) {
			final boolean isL = w.getServer().getClient().isCurrentLeader();
			if (leader && isL || !leader && !isL) {
				return w;
			}
		}
		return ret;
	}

	private static Set<ServerWhitness> buildCluster(
			final int size,
			final Set<Pallet<String>> pallets,
			final Set<Duty<String>> set) {
		final Set<ServerWhitness> whitnesses = new HashSet<>();
		for (int i = 0 ; i < size; i++) {
			whitnesses.add(server(set, pallets, String.valueOf(i)));
		}
		return whitnesses;
	}

	private static ServerWhitness server(
			final Set<Duty<String>> duties, 
			final Set<Pallet<String>> pallets,
			final String tag) {
		final Config config = new Config("localhost:2181");
		config.getBootstrap().setServiceName("testito");
		config.getBootstrap().setDropVMLimit(true);
		config.getBootstrap().setBeatUnitMs(100l);

		final Server<String, String> server = new Server<String, String>(config);
		final Set<Duty<String>> captured = new HashSet<>();
		final Set<Duty<String>> released = new HashSet<>();
		
		final EventMapper<String, String> mapper = server.getEventMapper()
			.onPalletLoad(() -> pallets)
			.onLoad(()-> duties)
			.onCapture(captured::addAll)
			.onRelease(released::addAll)
			;
		if (!pallets.isEmpty()) {
			for (Pallet<String> p: pallets) {
				mapper.setCapacity(p, 100);
			}
		}
		mapper
			.setLocationTag(tag)
			.done();
		return new ServerWhitness(server, captured, released);
	}

	public static class ServerWhitness {
		private final Server<String, String> server;
		private final Set<Duty<String>> captured;
		private final Set<Duty<String>> released;
		public ServerWhitness(
				final Server<String, String> server, 
				final Set<Duty<String>> captured,
				final Set<Duty<String>> released) {
			super();
			this.server = server;
			this.captured = captured;
			this.released = released;
		}
		public Server<String, String> getServer() {
			return server;
		}
		public Set<Duty<String>> getCaptured() {
			return captured;
		}
		public Set<Duty<String>> getReleased() {
			return released;
		}
	}

}
 