package io.tilt.minka;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import org.junit.Assert;

import io.tilt.minka.api.Config;
import io.tilt.minka.api.Duty;
import io.tilt.minka.api.EventMapper;
import io.tilt.minka.api.Pallet;
import io.tilt.minka.api.Server;
import io.tilt.minka.core.leader.balancer.Balancer.BalancerMetadata;
import io.tilt.minka.core.leader.balancer.EvenSizeBalancer;
import io.tilt.minka.core.leader.balancer.EvenWeightBalancer;
import io.tilt.minka.core.leader.balancer.FairWeightBalancer;

public class TestUtils {

	public static Config prototypeConfig() {
		final Config prototypeConfig = new Config();
		prototypeConfig.getBootstrap().setNamespace("client-test");
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
				new EvenSizeBalancer.Metadata(), 
				new FairWeightBalancer.Metadata(),
				new EvenWeightBalancer.Metadata());
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
		ownConfig.getBootstrap().setServerTag(tag);
		ownConfig.getBootstrap().setNamespace(refConfig.getBootstrap().getNamespace());
		ownConfig.getBootstrap().setBeatUnitMs(refConfig.getBootstrap().getBeatUnitMs());
		ownConfig.getBootstrap().setDropVMLimit(true);
		
		ownConfig.getBootstrap().setEnableCoreDump(refConfig.getBootstrap().isEnableCoreDump());
		ownConfig.getBootstrap().setCoreDumpFrequency(refConfig.getBootstrap().getCoreDumpFrequency());
		ownConfig.getBootstrap().setCoreDumpFilepath(refConfig.getBootstrap().getCoreDumpFilepath());

		final Set<Duty<String>> everCaptured = new HashSet<>();
		final Set<Duty<String>> everReleased = new HashSet<>();
		final Set<Duty<String>> current = new HashSet<>();
		final Object[] o = {null};
		final Server<String, String> server = new Server<String, String>(ownConfig);
		final EventMapper<String, String> mapper = server.getEventMapper();
		for (Pallet<String> p: pallets) {
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
		try {
			Thread.sleep(500l);
		} catch (InterruptedException e) {
		}
		return new ServerWhitness(server, everCaptured, everReleased, current);
	}

	public static enum Type {
		add, remove
	}
	
	public static void assertCRUDExecuted(
			final Type type, 
			final Collection<ServerWhitness> cluster, 
			final Collection<Duty<String>> duties) {
		final Set<Duty<String>> check = new HashSet<>(duties);
		for (ServerWhitness w: cluster) {
			Set<Duty<String>> collModified = w.getEverCaptured();
			Set<Duty<String>> collUnmodified = w.getEverReleased();
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
			final Collection<Duty<String>> duties) {
		final Set<Duty<String>> tmp = new HashSet<>();
		for (ServerWhitness w: cluster) {
			assertTrue(w.getCurrent().size()>0);
			assertTrue(tmp.addAll(w.getCurrent()));
		}
		assertEquals("different size of expected distribution ", duties.size(), tmp.size());
		assertEquals("different content of expected distribution", duties, tmp);
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
 