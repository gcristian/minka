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
import io.tilt.minka.core.leader.distributor.Balancer.Strategy;

public class MultiPalletRandomSample extends BaseSampleDelegate {

	private static final long serialVersionUID = 305399302612484891L;

	private static int TOTAL_DUTIES = 100;
	private static int TOTAL_PALLETS = 10;
	private static final Random rnd = new Random();

	@Override
	protected Set<Duty<String>> getDuties() {
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
	protected Set<Pallet<String>> getPallets() {
		final Set<Pallet<String>> pallets = new HashSet<>();
		for (int pid = 1; pid <= TOTAL_PALLETS; pid++) {
			Strategy strategy = null;
			if (pid == 1) {
				strategy = Strategy.EVEN_WEIGHT;
			} else {
				strategy = Strategy.ROUND_ROBIN;
			}
			pallets.add(PalletBuilder.build(
					String.valueOf(pid), String.class, strategy, Storage.CLIENT_DEFINED, "payload"));
		}
		return pallets;
	}

}