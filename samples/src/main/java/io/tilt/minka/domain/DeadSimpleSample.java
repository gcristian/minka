
package io.tilt.minka.domain;

import static jersey.repackaged.com.google.common.collect.Sets.newHashSet;

import java.util.Set;
import java.util.TreeSet;

import io.tilt.minka.api.Duty;
import io.tilt.minka.api.DutyBuilder;
import io.tilt.minka.api.Minka;
import io.tilt.minka.api.PalletBuilder;

public class DeadSimpleSample {

	@SuppressWarnings("unchecked")
	public static void main(String[] args) throws Exception {
		
		/**
		 * Example for the simplest distribution use case in 9 lines  
		 */

		// create a dummy duty to check at last for it's reception
		final Duty<String> helloWorld = DutyBuilder.<String>builder("helloworld", "group").build();

		// to save those duties assigned by minka to this shard 
		final Set<Duty<String>> myDuties = new TreeSet<>();
		
		// create a minka server with all default TCP/port values
		final Minka<String, String> minka = new Minka<>();		
		// create a dummy pallet to group the helloWorld duty
		// on production environtment we should build duties loding source data from a database
		minka.onPalletLoad(()-> newHashSet(PalletBuilder.<String>builder("group").build()));		
		// holds the duties to be reported in case this shard becomes the leader  
		// on production environtment we should build duties loding source data from a database
		minka.onDutyLoad(()-> newHashSet(helloWorld));

		// map the taking duties action
		minka.onDutyCapture(duties->myDuties.addAll(duties));
		// map the releasing duties from this shard (hardly as there's no rebalance we can hope here)
		minka.onDutyRelease(duties->myDuties.removeAll(duties));
		// minka will continuously ask for those running duties to ensure we havent lost them
		minka.onDutyReport(()->myDuties);
		// release the bootstrap process so minka can start
		minka.load();
		
		Thread.sleep(5000);
		// after a while, given this's the only shard, minka will give us the initially loaded duty
		assert myDuties.contains(helloWorld);
		
		// create another one
		final Duty<String> another = DutyBuilder.<String>builder("another", "group").build();
		minka.getClient().add(another);
		
		Thread.sleep(5000);
		// after a while the distribution process, will deliver it to us
		assert myDuties.contains(another);
		
		minka.destroy();
		assert myDuties.isEmpty();

	}
}
