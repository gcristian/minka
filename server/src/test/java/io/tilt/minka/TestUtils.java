package io.tilt.minka;

import static org.junit.Assert.assertTrue;

import java.util.HashSet;
import java.util.Set;

import org.junit.Assert;

import io.tilt.minka.api.Config;
import io.tilt.minka.api.Duty;
import io.tilt.minka.api.EventMapper;
import io.tilt.minka.api.Pallet;
import io.tilt.minka.api.Server;

public class TestUtils {

	public static Config prototypeConfig() {
		final Config prototypeConfig = new Config();
		prototypeConfig.getBootstrap().setServiceName("client-test");
		prototypeConfig.getBootstrap().setBeatUnitMs(100l);
		return prototypeConfig;
	}

	public static void shutdownServers(final Set<ServerWhitness> cluster) {
		for (ServerWhitness w: cluster) {
			w.getServer().shutdown();
		}
		try {
			Thread.sleep(1000l);			
		} catch (Exception e) {
		}
	}

	public static Set<ServerWhitness> buildCluster(
			final int size,
			final Config config,
			final Set<Pallet<String>> pallets,
			final Set<Duty<String>> duties) throws InterruptedException {
		
		final Set<ServerWhitness> cluster = new HashSet<>();
		for (int i = 0 ; i < size; i++) {
			cluster.add(createServer(config, duties, pallets, String.valueOf(i)));
		}
		
		int max = 999;
		while (max-->0) {
			for (ServerWhitness w: cluster) {
				if (w.getServer().getClient().isCurrentLeader()) {
					return cluster;
				}
			}
			Thread.sleep(50l);
		}
		Assert.fail("no leader chosen");
		return null;
	}

	public static ServerWhitness createServer(
			final Config refConfig,
			final Set<Duty<String>> duties, 
			final Set<Pallet<String>> pallets,
			final String tag) {
		
		final Config ownConfig = new Config(refConfig.getBootstrap().getZookeeperHostPort());
		ownConfig.getBootstrap().setServiceName(refConfig.getBootstrap().getServiceName());
		ownConfig.getBootstrap().setBeatUnitMs(refConfig.getBootstrap().getBeatUnitMs());
		ownConfig.getBootstrap().setDropVMLimit(true);

		final Set<Duty<String>> captured = new HashSet<>();
		final Set<Duty<String>> released = new HashSet<>();
		
		final Server<String, String> server = new Server<String, String>(ownConfig);
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
		try {
			Thread.sleep(500l);
		} catch (InterruptedException e) {
		}
		return new ServerWhitness(server, captured, released);
	}

	public static enum Type {
		add, remove
	}
	
	public static void assertCRUDExecuted(
			final Type type, 
			final Set<ServerWhitness> cluster, 
			final Set<Duty<String>> duties) {
		final Set<Duty<String>> check = new HashSet<>(duties);
		for (ServerWhitness w: cluster) {
			Set<Duty<String>> collModified = w.getCaptured();
			Set<Duty<String>> collUnmodified = w.getReleased();
			if (type==Type.remove) {
				collUnmodified = w.getCaptured();
				collModified = w.getReleased();
			}
			assertTrue("captured/released should have contents", collModified.size()>0);
			assertTrue("captured/released should be empty", collUnmodified.size()==0);
			assertTrue("captured/released is uncompleted", check.removeAll(collModified));
		}
		assertTrue(check.isEmpty());
	}
	

	public static Set<Duty<String>> duties(final Pallet<String> p, final int size) {
		final Set<Duty<String>> set = new HashSet<>();
		for (int i = 0; i < size; i++) {
			set.add(duty(p, i));			
		}
		return set;
	}

	public static Duty<String> duty(final Pallet<String> p, final int id) {
		return Duty.<String>builder(String.valueOf(id), p.getId()).with(1).build();
	}
    

	public static void cleanWhitnesses(final Set<ServerWhitness> cluster) {
		for (ServerWhitness w: cluster) {
			w.getCaptured().clear();
			w.getReleased().clear();
		}
	}
	
	public static ServerWhitness giveMeAServer(final Set<ServerWhitness> list, final boolean leader) {
		ServerWhitness ret = null;
		for (ServerWhitness w: list) {
			final boolean isL = w.getServer().getClient().isCurrentLeader();
			if ((leader && isL) || (!leader && !isL)) {
				return w;
			}
		}
		return ret;
	}

	

}
 