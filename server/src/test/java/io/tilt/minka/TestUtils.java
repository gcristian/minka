package io.tilt.minka;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

import org.junit.Assert;

import io.tilt.minka.api.Config;
import io.tilt.minka.api.Duty;
import io.tilt.minka.api.EventMapper;
import io.tilt.minka.api.Pallet;
import io.tilt.minka.api.Server;
import io.tilt.minka.core.leader.balancer.Balancer.BalancerMetadata;
import io.tilt.minka.core.leader.balancer.SizeEqualizer;
import io.tilt.minka.core.leader.balancer.WeightEqualizer;
import io.tilt.minka.core.leader.balancer.FairWeightToCapacity;

public class TestUtils {

	public static Config prototypeConfig() {
		final Config prototypeConfig = new Config();
		prototypeConfig.getBootstrap().setNamespace("client-test");
		// after stocking and committreees cannot reduce BEAT size below 100 (was 50 before)
		prototypeConfig.getBootstrap().setBeatUnitMs(100l);
		return prototypeConfig;
	}

	public static void shutdownServers(final Collection<ServerWhitness> cluster, boolean leaderLast) {
		ServerWhitness l = null;
		for (ServerWhitness w: cluster) {
			if (w.getServer().getClient().isCurrentLeader()) {
				l = w;
			} else {
				w.getServer().shutdown();
			}
		}
		if (l!=null) {
			l.getServer().shutdown();
		}
		try {
			Thread.sleep(1000l);
		} catch (Exception e) {
		}
	}
	public static void shutdownServers(final Collection<ServerWhitness> cluster) {
		shutdownServers(cluster, false);
	}

	public static Collection<BalancerMetadata> balancers() {
		return Arrays.asList(
				new SizeEqualizer.Metadata(), 
				new FairWeightToCapacity.Metadata(),
				new WeightEqualizer.Metadata()
				);
	}

	public static Set<ServerWhitness> buildCluster(
			final int size,
			final Config config,
			final Set<Pallet> pallets,
			final Set<Duty> duties) throws InterruptedException {
		
		final Set<ServerWhitness> cluster = new HashSet<>();
		for (int i = 0 ; i < size; i++) {
			cluster.add(createServer(i, config, duties, pallets, String.valueOf(i)));
		}
		
		int max = 999;
		while (max-->0) {
			if (!cluster.isEmpty()) {
				for (ServerWhitness w: cluster) {
					if (w.getServer().getClient().isCurrentLeader()) {
						return cluster;
					}
				}
				
			}
			Thread.sleep(50l);
		}
		Assert.fail("no leader chosen");
		return null;
	}
	
	public static ServerWhitness createServer(
			final int portIncrement,
			final Config refConfig,
			final Set<Duty> duties, 
			final Set<Pallet> pallets,
			final String tag) {
		
		final Config ownConfig = new Config(refConfig.getBootstrap().getZookeeperHostPort());
		ownConfig.getBroker().setEnablePortFallback(false);
		ownConfig.getBroker().setHostAndPort("localhost", 7000 + portIncrement);
		ownConfig.getBootstrap().setServerTag(tag);
		ownConfig.getBootstrap().setNamespace(refConfig.getBootstrap().getNamespace());
		ownConfig.getBootstrap().setBeatUnitMs(refConfig.getBootstrap().getBeatUnitMs());
		ownConfig.getBootstrap().setDropVMLimit(true);
		
		ownConfig.getBootstrap().setEnableCoreDump(refConfig.getBootstrap().isEnableCoreDump());
		ownConfig.getBootstrap().setCoreDumpFrequency(refConfig.getBootstrap().getCoreDumpFrequency());
		ownConfig.getBootstrap().setCoreDumpFilepath(refConfig.getBootstrap().getCoreDumpFilepath());

		final Set<Duty> everCaptured = new HashSet<>();
		final Set<Duty> everReleased = new HashSet<>();
		final Set<Duty> current = new HashSet<>();
		final Object[] o = {null};
		final Server server = new Server(ownConfig);
		final EventMapper mapper = server.getEventMapper();
		for (Pallet p: pallets) {
			mapper.setCapacity(p, 100);
		}
		mapper.onPalletLoad(() -> pallets)
			.onActivation(()->{})
			.onDeactivation(()->{})
			.onTransfer((a,b)->{})
			.onLoad(()-> duties)
			.onPalletRelease(p->o[0]=p)
			.onPalletCapture(p->o[0]=p)
			.onCapture(d-> { 
				everCaptured.addAll(d);
				current.addAll(d);
			})
			.onRelease(d-> {
				everReleased.addAll(d);
				current.removeAll(d);
			})
			.done();
		sleep();
		return new ServerWhitness(server, everCaptured, everReleased, current);
	}

	private static void sleep() {
		try {
			Thread.sleep(500l);
		} catch (InterruptedException e) {
		}
	}

	public static enum Type {
		add, remove
	}
	
	public static void assertCRUDExecuted(
			final Type type, 
			final Collection<ServerWhitness> cluster, 
			final Collection<Duty> duties) {
		final Set<Duty> check = new HashSet<>(duties);
		for (ServerWhitness w: cluster) {
			Set<Duty> collModified = w.getEverCaptured();
			Set<Duty> collUnmodified = w.getEverReleased();
			if (type==Type.remove) {
				collUnmodified = w.getEverCaptured();
				collModified = w.getEverReleased();
			}
			assertTrue("captured/released should have contents", collModified.size()>0);
			assertTrue("captured/released should be empty", collUnmodified.size()==0);
			assertTrue("captured/released is uncompleted", check.removeAll(collModified));
		}
		assertTrue(check.isEmpty());
	}
	
	public static void assertDistribution(
			final Collection<ServerWhitness> cluster,
			final Collection<Duty> duties) {
		final Set<Duty> tmp = new HashSet<>();
		for (ServerWhitness w: cluster) {
			assertTrue("whitness without current (0)", w.getCurrent().size()>0);
			assertTrue(tmp.addAll(w.getCurrent()));
		}
		final String prep = "cluster-size:" + cluster.size() + ";duties-size:" + duties.size();
		assertEquals(prep + " diff. size of expected distro:", duties.size(), tmp.size());
		assertEquals(prep + " diff. content of expected distro:", duties, tmp);
	}
	

	public static Set<Duty> duties(final Pallet p, final int size) {
		final Set<Duty> set = new HashSet<>();
		for (int i = 0; i < size; i++) {
			set.add(duty(p, i));			
		}
		return set;
	}

	public static Duty duty(final Pallet p, final int id) {
		return Duty.builder(String.valueOf(id), p.getId()).with(1).build();
	}
    

	public static void cleanWhitnesses(final Collection<ServerWhitness> cluster) {
		for (ServerWhitness w: cluster) {
			w.getEverCaptured().clear();
			w.getEverReleased().clear();
		}
	}
	
	public static ServerWhitness pickAServer(final Collection<ServerWhitness> list, final boolean leader) {
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
 