/**
 * Copyright (c) 2011-2015 Zauber S.A. -- All rights reserved
 */

package io.tilt.minka.delegates;

import java.util.HashSet;
import java.util.Random;
import java.util.Set;

import io.tilt.minka.api.Duty;
import io.tilt.minka.api.DutyBuilder;
import io.tilt.minka.api.Pallet;
import io.tilt.minka.api.Pallet.Storage;
import io.tilt.minka.api.PalletBuilder;
import io.tilt.minka.core.leader.distributor.Balancer.BalancerMetadata;
import io.tilt.minka.core.leader.distributor.Balancer.Strategy;
import io.tilt.minka.core.leader.distributor.EvenLoadBalancer;
import io.tilt.minka.core.leader.distributor.RoundRobinBalancer;
import io.tilt.minka.core.leader.distributor.SpillOverBalancer;

public class MultiPalletRandomSample extends BaseSampleDelegate {

	private static final long serialVersionUID = 305399302612484891L;
	private static final Random rnd = new Random();
	
	private static int dutiesSize = 20; //Math.max(rnd.nextInt(1000), 10);
	private static int palletsSize = 2; //Math.min(rnd.nextInt(25), (dutiesSize/2)+2);

	@Override
	protected Set<Duty<String>> getDuties() {
		final Set<Duty<String>> duties = new HashSet<>();
		for (int i = 1; i <= dutiesSize; i++) {
			final int pid = Math.max(rnd.nextInt(palletsSize), 1);
			final long weight = pid != 1 ? Math.max(rnd.nextInt(99000), 999l) : 1;
			duties.add(DutyBuilder.build(String.class, String.valueOf(i), String.valueOf(pid), weight));
		}
		return duties;
	}

	@Override
	protected Set<Pallet<String>> getPallets() {
		final Set<Pallet<String>> pallets = new HashSet<>();
		for (int pid = 1; pid <= palletsSize; pid++) {
			int num = rnd.nextInt(3);
			BalancerMetadata meta = num==1 ? 
					new SpillOverBalancer.SpilloverMetadata() : num ==2 ? 
					new RoundRobinBalancer.RoundrobinMetadata() : 
					new EvenLoadBalancer.EvenLoadMetadata();
			pallets.add(PalletBuilder.build(
					String.valueOf(pid), String.class, meta, Storage.CLIENT_DEFINED, "payload"));
		}
		return pallets;
	}

}