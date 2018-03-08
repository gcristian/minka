/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package io.tilt.minka.core.leader.distributor;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.lang.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.util.concurrent.AtomicDouble;

import io.tilt.minka.api.Pallet;
import io.tilt.minka.core.leader.PartitionTable;
import io.tilt.minka.core.leader.balancer.BalancingException;
import io.tilt.minka.core.leader.distributor.Balancer.Strategy;
import io.tilt.minka.domain.EntityEvent;
import io.tilt.minka.domain.Shard;
import io.tilt.minka.domain.Shard.ShardState;
import io.tilt.minka.domain.ShardCapacity.Capacity;
import io.tilt.minka.domain.ShardEntity;
import io.tilt.minka.domain.EntityState;

/** 
 * Media for {@linkplain Balancer} to request transfers and overrides on the {@linkplain Plan}
 * while ignoring its internals and relying on this for coherence and consistency 
 * It helps to balancer extensibility.
 * 
 * everything ends up in a: plan.ship(...)
 *
 * Is a rule that if the balancers have a stable behaviour: this class will keep the 
 * plan unchanged at  execute(), unless there's a CRUD operation from client.
 * 
 * @author Cristian Gonzalez
 * @since Oct 29, 2016
 */
public class Migrator {

	protected static final Logger log = LoggerFactory.getLogger(Migrator.class);
	
	private final PartitionTable table;
	private final Plan roadmap;
	private final Pallet<?> pallet;
	private Boolean isWeightedPallet;
	private List<Override> overrides;
	private List<Transfer> transfers;
	
	protected Migrator(final PartitionTable table, final Plan roadmap, final Pallet<?> pallet) {
		super();
		Validate.notNull(pallet);
		Validate.notNull(roadmap);
		Validate.notNull(table);
		this.table = table;
		this.roadmap = roadmap;
		this.pallet = pallet;
	}

	/* specifically transfer from a Source to a Target */
	public final void transfer(final Shard target, final ShardEntity entity) {
		Validate.notNull(target);
		Validate.notNull(entity);
		transfer_(null, target, entity);
	}
	public final void transfer(final Shard source, final Shard target, final ShardEntity entity) {
		Validate.notNull(source);
		Validate.notNull(target);
		Validate.notNull(entity);
		transfer_(source, target, entity);
	}
	private final void transfer_(final Shard source, final Shard target, final ShardEntity entity) throws BalancingException {
		if (this.transfers == null ) {
			this.transfers = new ArrayList<>();
		}
		validateTransfer(source, target, entity);
		checkDuplicate(entity);
		if (log.isInfoEnabled()) {
			log.info("{}: Requesting Transfer: {} from: {} to: {}", getClass().getSimpleName(), entity.toBrief(), 
				source==null ? "[new]":source, target);
		}
		transfers.add(new Transfer(source, target ,entity));
	}
	/* explicitly override a shard's content, client must look after consistency ! */
	public final void override(final Shard shard, final Set<ShardEntity> cluster) {
		Validate.notNull(shard);
		Validate.notNull(cluster);
		if (this.overrides == null) {
			this.overrides = new ArrayList<>();
		}
		cluster.forEach(d->checkDuplicate(d));
		final double remainingCap = validateOverride(shard, cluster);
		if (!cluster.isEmpty() && log.isInfoEnabled()) {
			log.info("{}: Requesting Override: {}, remain cap: {}, with {}", getClass().getSimpleName(), shard, remainingCap, 
				ShardEntity.toStringIds(cluster));
		}
		overrides.add(new Override(pallet, shard, new LinkedHashSet<>(cluster), remainingCap));
	}
	private double validateOverride(final Shard target, final Set<ShardEntity> cluster) {
		for (Override ov: overrides) {
			if (ov.getShard().equals(target)) {
				throw new BalancingException("bad override: this shard: %s has already being overrided !", target);
			}
		}
		return checkSuitable(target, cluster);
	}

	private double checkSuitable(final Shard target, final Set<ShardEntity> cluster) {
		double remainingCap = 0;
		if (isWeightedPallet()) {
			final AtomicDouble accum = new AtomicDouble(0);
			final Capacity cap = target.getCapacities().get(pallet);
			if (cap!=null) {
				cluster.forEach(d->accum.addAndGet(d.getDuty().getWeight()));
				if (cap.getTotal() < accum.get()) {
					throw new BalancingException("bad override: overwhelming weight!: %s (max capacity: %s, shard: %s)", 
							accum, cap.getTotal(), target);
				} else {
					remainingCap = cap.getTotal() - accum.get();
				}
			}
		}
		return remainingCap;
	}
	
	private boolean isWeightedPallet() {
		if (isWeightedPallet == null) {
			for (Strategy strat: Balancer.Strategy.values()) {
				if (strat.getBalancer().equals(pallet.getMetadata().getBalancer()) && 
						strat.getWeighted()==Balancer.Weighted.YES) {
					return isWeightedPallet = true;
				}
			}
			isWeightedPallet = false;
		}
		return isWeightedPallet;
	}
	
	private final void validateTransfer(final Shard source, final Shard target, final ShardEntity entity) {
		if (!pallet.getId().equals(entity.getDuty().getPalletId())) {
			throw new BalancingException("bad transfer: duty %s doesnt belong to pallet: %s", entity, pallet);
		}
		if (table.getStage().getDutiesByShard(target).contains(entity)) {
			throw new BalancingException("bad transfer: duty %s already exist in target shard: %s", entity, target);
		}
		if (source==null) {
			final Shard location = table.getStage().getDutyLocation(entity);
			if (location !=null && !(location.getState()==ShardState.GONE || location.getState()==ShardState.QUITTED)) {
				throw new BalancingException("bad transfer: duty %s has a source, must be transferred from "
						+ "its current location: %s", entity, location);
			}
		} else if (source.equals(target)) {
			throw new BalancingException("bad transfer: duty %s has the same source and target");
		}
		if (entity.getLastEvent()==EntityEvent.REMOVE) {
			throw new BalancingException("bad transfer: duty: %s is marked for deletion, cannot be balanced", entity);
		}
		for (ShardEntity duty: table.getNextStage().getDutiesCrud(EntityEvent.REMOVE, EntityState.PREPARED)) {
			if (duty.equals(entity)) {
				throw new BalancingException("bad transfer: duty: %s is just marked for deletion, cannot be balanced", entity);
			}
		}
	}

	private void checkDuplicate(final ShardEntity entity) {
		if (inOverrides(entity)) {
			throw new BalancingException("duty: %s already in an override !", entity);
		}
		if (inTransfers(entity)) {
			throw new BalancingException("duty: %s already in a transfer !", entity);
		}
	}
	
	public boolean isEmpty() {
		return overrides==null && transfers==null;
	}
	
	/** effectively write overrided and transfered operations to roadmap object 
	 * @return if the execution effectively generated any changes */ 
	protected final boolean execute() {
		boolean anyChange = false;
		if (isEmpty()) {
			log.warn("{}: Nothing to execute (empty)", getClass().getSimpleName());
			return false;
		}
		log.info("{}: Evaluating transfers: {} and overrides: {}", getClass().getSimpleName(), 
				transfers!=null ? transfers.size() : 0, overrides!=null ? overrides.size() : 0);
		if (overrides!=null) {
			checkExclusions(); // balancers using transfers only make delta changes, without reassigning
			for (final Override ov: overrides) {			
				anyChange = ov.compute(roadmap, table);
			}
		}
		if (transfers!=null) {
			for (Transfer tr: transfers) {
				anyChange|=tr.compute(roadmap, table);
			}
		}
		return anyChange;
	}

	private boolean unfairlyIgnored(ShardEntity duty) {
		if (isWeightedPallet()) {
			if (overrides!=null) {
				for (Override ov: overrides) {
					if (ov.getRemainingCap()>=duty.getDuty().getWeight()) {
						log.warn("{}: Override on: {} has a remaining cap: {} (out of :{}), enough to lodge "
								+ "duty: {}", getClass().getSimpleName(), ov.getShard(), ov.getRemainingCap(), 
								ov.getShard().getCapacities().get(pallet).getTotal(), duty.toBrief());
						return true;
					}
				}
				// the transfers should also be validated, (overkilling maybe) 
				// the spilloverbal. uses transfers() and could let one creation out because of space
				// and this will fail to prohibit such behaviour
				return false;
			}
		}
		return true;
	}
	private void checkExclusions() {
		for (final ShardEntity duty: table.getNextStage().getDutiesCrud(EntityEvent.CREATE, EntityState.PREPARED)) {
			if (duty.getDuty().getPalletId().equals(pallet.getId()) && 
			        !inTransfers(duty) && 
					!inOverrides(duty) && 
					unfairlyIgnored(duty)) {
				log.warn("bad exclusion: duty: {} was just marked for creation, it must be balanced !", duty.toBrief());
			}
		}
		final Set<ShardEntity> deletions = table.getNextStage().getDutiesCrud(EntityEvent.REMOVE, EntityState.PREPARED);
		for (final ShardEntity curr: table.getStage().getDutiesAttached()) {
			if (curr.getDuty().getPalletId().equals(pallet.getId()) && 
			        !deletions.contains(curr) && 
					!inTransfers(curr) && 
					!inOverrides(curr) && 
					unfairlyIgnored(curr)) {
				log.warn("bad exclusion: duty: " + curr.toBrief() + " is in ptable and was excluded from balancing !");
			}
		}
	}

	private boolean inTransfers(final ShardEntity duty) {
		return transfers!=null && 
		        transfers.stream()
    	            .filter(t->t.getEntity().equals(duty))
    	            .count()>0;
	}

	private boolean inOverrides(final ShardEntity duty) {
		return overrides!=null && 
		        overrides.stream()
		            .filter(o->o.getEntities().contains(duty))
		            .count()>0;
	}

}
