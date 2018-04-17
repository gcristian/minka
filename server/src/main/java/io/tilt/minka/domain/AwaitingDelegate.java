package io.tilt.minka.domain;

import java.util.Set;

import io.tilt.minka.api.Duty;
import io.tilt.minka.api.Pallet;

/** 
 * an always awaiting never ready delegate
 * the intention is to allow Minka context loading and holding follower initialization 
 * */
public class AwaitingDelegate implements PartitionMaster<String, String> {

	private final static String MESSAGE = "this's a transitional delegate: should not end up here";
	@Override
	public boolean isReady() {
		return false;
	}

	@Override
	public void capture(Set<Duty<String>> duties) {
		throw new IllegalStateException(MESSAGE);
	}

	@Override
	public void release(Set<Duty<String>> duties) {
		throw new IllegalStateException(MESSAGE);
	}

	@Override
	public Set<Duty<String>> loadDuties() {
		throw new IllegalStateException(MESSAGE);
	}

	@Override
	public Set<Pallet<String>> loadPallets() {
		throw new IllegalStateException(MESSAGE);
	}

	@Override
	public double getTotalCapacity(Pallet<String> pallet) {
		throw new IllegalStateException(MESSAGE);
	}

	@Override
	public void capturePallet(Set<Pallet<String>> pallets) {
		throw new IllegalStateException(MESSAGE);
	}

	@Override
	public void releasePallet(Set<Pallet<String>> pallets) {
		throw new IllegalStateException(MESSAGE);
	}

}
