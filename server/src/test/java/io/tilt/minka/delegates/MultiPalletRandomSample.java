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
import io.tilt.minka.core.leader.distributor.EvenWeightBalancer;
import io.tilt.minka.core.leader.distributor.EvenSizeBalancer;
import io.tilt.minka.core.leader.distributor.SpillOverBalancer;

public class MultiPalletRandomSample extends BaseSampleDelegate {

	public MultiPalletRandomSample() throws Exception {
		super();
		// TODO Auto-generated constructor stub
	}

	private static final long serialVersionUID = 305399302612484891L;
	private static final Random rnd = new Random();
	
	private static int dutiesSize = 500; // Math.max(rnd.nextInt(1000), 10);
	private static int palletsSize = 50; // Math.min(rnd.nextInt(25), (dutiesSize/2)+2);

	@Override
	public Set<Duty<String>> buildDuties() {
		final Set<Duty<String>> duties = new HashSet<>();
		for (int i = 1; i <= dutiesSize; i++) {
			final int pid = Math.max(rnd.nextInt(palletsSize), 1);
			final long weight = pid != 1 ? Math.max(rnd.nextInt(99000), 999l) : 1;
			duties.add(DutyBuilder.build(String.class, String.valueOf(i), String.valueOf(pid), weight));
		}
		return duties;
	}

	@Override
	public Set<Pallet<String>> buildPallets() {
		
		final Set<Pallet<String>> pallets = new HashSet<>();
		for (int pid = 1; pid <= palletsSize; pid++) {
			int num = rnd.nextInt(3);	
			BalancerMetadata meta = num==1 ? 
					new SpillOverBalancer.Metadata() : num ==2 ? 
					new EvenSizeBalancer.Metadata() : 
					new EvenWeightBalancer.Metadata();
			pallets.add(PalletBuilder.build(
					String.valueOf(pid), String.class, meta, Storage.CLIENT_DEFINED, "payload"));
		}
		return pallets;
	}

	
	@Override
	public double getTotalCapacity(Pallet<?> pallet) {
		if (Long.parseLong(pallet.getId())<=16) {
			return 500; 
		} else if (Long.parseLong(pallet.getId())>16 && (Long.parseLong(pallet.getId())<=32)) {
			return Long.parseLong(pallet.getId()) + 10;
		} else {
			return Long.parseLong(pallet.getId()) + 100;
		}			
	}
	
	public static class TestDataset {
		
	}

	@Override
	public void init() throws Exception {
		// TODO Auto-generated method stub
		
	}

}