package io.tilt.minka.domain;

import java.io.Serializable;

import io.tilt.minka.api.Pallet;

public class Capacity implements Serializable {
	
	private static final long serialVersionUID = 3629069972815094880L;
	
	private final double total;
	private final Pallet pallet;
	
	public Capacity(Pallet pallet, double total) {
		this.total = total;
		this.pallet = pallet;
	}
	
	public double getTotal() {
		return this.total;
	}

	public Pallet getPallet() {
		return this.pallet;
	}
	
	@Override
	public String toString() {
		return "P:" + pallet.toString() + " W:" + total;
	}
}