/**
 * Copyright (c) 2011-2015 Zauber S.A. -- All rights reserved
 */

package io.tilt.minka;

import java.util.HashSet;
import java.util.Random;
import java.util.Set;

import io.tilt.minka.api.Duty;
import io.tilt.minka.api.DutyBuilder;
import io.tilt.minka.api.Pallet;
import io.tilt.minka.api.Pallet.Storage;
import io.tilt.minka.api.PalletBuilder;
import io.tilt.minka.core.leader.distributor.Balancer.Strategy;
import io.tilt.minka.core.leader.distributor.impl.EvenSizeBalancer;
import io.tilt.minka.core.leader.distributor.impl.EvenWeightBalancer;
import io.tilt.minka.delegates.BaseSampleDelegate;

public class DemoDelegate extends BaseSampleDelegate {

	public DemoDelegate() throws Exception {
		super();
		// TODO Auto-generated constructor stub
	}

	private static final long serialVersionUID = 305399302612484891L;

	private static int TOTAL_DUTIES = 30;
	private static int TOTAL_PALLETS = 5;

	@Override
	public Set<Duty<String>> buildDuties() {
		final Random rnd = new Random();
		final Set<Duty<String>> duties = new HashSet<>();
		for (int i = 1; i <= TOTAL_DUTIES; i++) {
			final int pid = Math.max(rnd.nextInt(TOTAL_PALLETS), 1);
			// final int pid = rnd.nextBoolean() ? 1 : 2;
			final long weight = pid != 1 ? Math.max(rnd.nextInt(99000), 999l) : 1;
			duties.add(DutyBuilder.build(String.class, String.valueOf(i), String.valueOf(pid), weight));
		}
		return duties;
	}

	@Override
	public Set<Pallet<String>> buildPallets() {
		final Set<Pallet<String>> pallets = new HashSet<>();
		for (int pid = 1; pid <= TOTAL_PALLETS; pid++) {
			pallets.add(PalletBuilder.build(String.valueOf(pid), String.class,
				pid != 1 ? new EvenWeightBalancer.Metadata(): new EvenSizeBalancer.Metadata(), 
					Storage.CLIENT_DEFINED, "payload"));
		}
		return pallets;
	}

	@Override
	public void init() throws Exception {
		// TODO Auto-generated method stub
		
	}

}