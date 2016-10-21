/**
 *  Copyright (c) 2011-2015 Zauber S.A.  -- All rights reserved
 */

package io.tilt.minka.domain;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang.Validate;

import io.tilt.minka.api.Pallet;

public class ShardCapacity implements Serializable {

	private static final long serialVersionUID = -8554620809270441100L;
	
	private final Map<Pallet<?>, Capacity> capacityByPallet;
	private final Shard shard;

	public ShardCapacity(final Shard shard, final Set<ShardEntity> pallets) {
		super();
		Validate.notNull(shard);
		Validate.notNull(pallets);
		this.capacityByPallet = new HashMap<>();
		pallets.forEach(p-> capacityByPallet.put(p.getPallet(), null));
		this.shard = shard;
	}
	
	public Shard getShard() {
		return this.shard;
	}
	
	public Map<Pallet<?>, Capacity> getCapacities() {
		return this.capacityByPallet;
	}
	
	public static class Capacity implements Serializable {
		private static final long serialVersionUID = 3629069972815094880L;
		private double total;
		private double available;
		private Pallet<?> pallet;
		public Capacity(Pallet<?> pallet, double total) {
			this(pallet, total, -1);
		}
		public Capacity(Pallet<?> pallet, double total, double available) {
			super();
			this.total = total;
			this.available = available;
			this.pallet = pallet;
		}
		public double getTotal() {
			return this.total;
		}
		public double getAvailable() {
			return this.available;
		}
		public Pallet<?> getPallet() {
			return this.pallet;
		}
		@Override
		public String toString() {
		return "P:" + pallet.toString() + " W:" + total;
		}
	}
	
}
