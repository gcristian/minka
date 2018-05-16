package io.tilt.minka;

import java.util.HashSet;
import java.util.Set;

import org.junit.Test;

import io.tilt.minka.api.Config;
import io.tilt.minka.api.Duty;
import io.tilt.minka.api.Pallet;
import io.tilt.minka.api.Server;

public class ClientTest {

	@Test
	public void test() throws InterruptedException {

		final Pallet<String> p = Pallet.<String>builder("p1").build();
		final Set<Pallet<String>> pallets = new HashSet<>();
		pallets.add(p);
		
		final Set<Duty<String>> set = new HashSet<>();
		for (int i = 0; i < 100; i++) {
			set.add(Duty.<String>builder(String.valueOf(i), p.get()).with(1).build());
			
		}
		
		Server<String, String> s1 = server(set, pallets, "ONE");
		Server<String, String> s2 = server(set, pallets, "TWO");
		//Server<String, String> s3 = server(set, pallets, "THREE");
		
		//Thread.sleep(10000);
		set.forEach(d->s1.getClient().remove(d));
		
		Thread.sleep(60*60*60*1000);
	}

	private Server<String, String> server(
			final Set<Duty<String>> set, 
			final Set<Pallet<String>> pallets, 
			final String tag) {
		final Config c = new Config("localhost:2181");
		c.getBootstrap().setMaxServicesPerMachine(3);
		c.getBootstrap().setServiceName(tag);
		final Server<String, String> s = new Server<String, String>(c);
		
		s.getEventMapper()
			.onPalletLoad(() -> pallets )
			.onLoad(()-> {
				return set;
			})
			.onCapture((d)-> {
				int i = 231;
			})
			.onRelease((d)-> {
				int i = 2312;
			})
			.setCapacity(pallets.iterator().next(), 100)
			.setLocationTag(tag)
			.done();
		return s;
	}


}
 