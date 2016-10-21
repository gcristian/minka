package io.tilt.minka.api;

import java.util.Set;

/** 
 * an always awaiting never ready delegate
 * the intention is to allow Minka context loading and holding follower initialization 
 * */
public class AwaitingDelegate implements PartitionMaster<String, String> {

	@Override
	public boolean isReady() {
		return false;
	}

	@Override
	public void take(Set<Duty<String>> duties) {
		throw new IllegalStateException();
	}

	@Override
	public void release(Set<Duty<String>> duties) {
		throw new IllegalStateException();
	}

	@Override
	public Set<Duty<String>> reportTaken() {
		throw new IllegalStateException();
	}

	@Override
	public Set<Duty<String>> loadDuties() {
		throw new IllegalStateException();
	}

	@Override
	public Set<Pallet<String>> loadPallets() {
		throw new IllegalStateException();
	}

	@Override
	public double getTotalCapacity(Pallet<?> pallet) {
		throw new IllegalStateException();
	}

}
