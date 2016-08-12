/**
 * Copyright (c) 2011-2015 Zauber S.A. -- All rights reserved
 */

package io.tilt.minka.core.leader.distributor;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.tilt.minka.domain.ShardEntity;

public class PalletCollector {

	private static final Logger logger = LoggerFactory.getLogger(Arranger.class);

	final private Map<ShardEntity, Set<ShardEntity>> pallets;
	final private Map<String, ShardEntity> palletById;

	protected PalletCollector() {
		this.pallets = new HashMap<ShardEntity, Set<ShardEntity>>();
		this.palletById = new HashMap<>();
	}

	protected PalletCollector(Set<ShardEntity> duties, Set<ShardEntity> palletSet) {
		this();
		Validate.notNull(duties);
		Validate.notNull(palletSet);
		palletSet.forEach(p -> this.palletById.put(p.getPallet().getId(), ShardEntity.create(p.getPallet())));
		duties.forEach(e -> add(e, this.palletById.get(e.getDuty().getPalletId())));
	}

	protected void add(ShardEntity entity, ShardEntity pallet) {
		Validate.notNull(pallet, "Cannot handle a null pallet, all duties require a pallet !");
		Validate.notNull(entity, "Cannot handle a null entity!");
		Set<ShardEntity> set = this.pallets.get(pallet);
		if (set == null) {
			this.pallets.put(pallet, set = new HashSet<>());
		}
		set.add(entity);
	}

	protected Iterator<Set<ShardEntity>> getIterator() {
		if (this.pallets.isEmpty()) {
			throw new IllegalAccessError("empty source !!");
		}
		return this.pallets.values().iterator();
	}

	protected ShardEntity getPallet(String id) {
		return this.palletById.get(id);
	}

	protected Set<ShardEntity> getDuties(ShardEntity pallet) {
		return this.pallets.get(pallet);
	}
}
