/**
 * Copyright (c) 2011-2015 Zauber S.A. -- All rights reserved
 */

package io.tilt.minka.core.leader.distributor;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collector;
import java.util.stream.Collectors;

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
		Validate.notEmpty(palletSet);
		palletSet.forEach(p -> this.palletById.put(p.getPallet().getId(), ShardEntity.create(p.getPallet())));
		for (ShardEntity se : duties) {
			try {
				if (logger.isDebugEnabled()) {
					logger.info("{}: Loooking for pallet: {}: on duty: {}", getClass().getSimpleName(),
							se.getDuty().getPalletId(), se.getDuty());
				}
				add(se, this.palletById.get(se.getDuty().getPalletId()));
			} catch (Exception e) {
				logger.error("Collecting pallets", e);
			}
		}
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

	protected Iterator<Set<ShardEntity>> getPalletsIterator() {
		return this.pallets.values().iterator();
	}

	protected ShardEntity getPallet(String id) {
		return this.palletById.get(id);
	}

	protected Set<ShardEntity> getDuties(ShardEntity pallet) {
		Set<ShardEntity> p = this.pallets.get(pallet);
		if (p == null) {
			p = new HashSet<>();
		}
		return p;
	}
}
