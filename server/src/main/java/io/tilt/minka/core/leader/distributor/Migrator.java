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

import static java.util.Objects.requireNonNull;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.util.concurrent.AtomicDouble;

import io.tilt.minka.api.Duty;
import io.tilt.minka.api.Pallet;
import io.tilt.minka.core.leader.PartitionTable;
import io.tilt.minka.core.leader.balancer.Balancer;
import io.tilt.minka.core.leader.balancer.BalancingException;
import io.tilt.minka.core.leader.balancer.Balancer.NetworkLocation;
import io.tilt.minka.core.leader.balancer.Balancer.Strategy;
import io.tilt.minka.domain.EntityEvent;
import io.tilt.minka.domain.EntityJournal;
import io.tilt.minka.domain.Shard;
import io.tilt.minka.domain.Shard.ShardState;
import io.tilt.minka.domain.ShardCapacity.Capacity;
import io.tilt.minka.domain.ShardEntity;
import io.tilt.minka.domain.ShardIdentifier;
import io.tilt.minka.domain.EntityState;

/** 
 * Balancer's Helper to request {@linkplain Transfer} and {@linkplain Override}
 * of {@linkplain Duty}'s and {@linkplain Pallet}s over {@linkplain Shard}s, 
 * so it can later write a {@linkplain ChangePlan}. 
 * Leveraging the balancer of the distribution process, consistency, validation.
 * 
 * A new plan will not be shipped when balancers do repeatable distributions and there're no 
 * CRUD ops from client, keeping the {@linkplain Scheme} stable and unchanged, 
 * as overrides and transfer only compute deltas according already distributed duties.
 * 
 * @author Cristian Gonzalez
 * @since Oct 29, 2016
 */
public class Migrator {

	protected static final Logger log = LoggerFactory.getLogger(Migrator.class);
	
	private final PartitionTable table;
	private final Pallet<?> pallet;
	private Boolean isWeightedPallet;
	private List<Override> overrides;
	private List<Transfer> transfers;
	
	private Map<Duty<?>, ShardEntity> sourceRefs;
	
	
	protected Migrator(final PartitionTable table, final Pallet<?> pallet, final Set<ShardEntity> duties) {
		super();
		this.table = requireNonNull(table);
		this.pallet = requireNonNull(pallet);
		this.sourceRefs = new HashMap<>(duties.size());
		duties.forEach(d-> sourceRefs.put(d.getDuty(), d));
	}

	/* specifically transfer from a Source to a Target */
	public final void transfer(final NetworkLocation target, final Duty<?> duty) {
		requireNonNull(target);
		requireNonNull(duty);
		transfer_(null, target, duty);
	}
	public final void transfer(final NetworkLocation source, final NetworkLocation target, final Duty<?> duty) {
		requireNonNull(source);
		requireNonNull(target);
		requireNonNull(duty);
		transfer_(source, target, duty);
	}
	private final void transfer_(final NetworkLocation source, final NetworkLocation target, final Duty<?> duty) throws BalancingException {
		if (this.transfers == null ) {
			this.transfers = new LinkedList<>();
		}
		final Shard source_ = deref(source);
		final Shard target_ = deref(target);
		final ShardEntity entity = sourceRefs.get(duty);
		validateTransfer(source_, target_, entity);
		checkDuplicate(entity);
		if (log.isInfoEnabled()) {
			log.info("{}: Requesting Transfer: {} from: {} to: {}", getClass().getSimpleName(), entity.toBrief(), 
				source==null ? "[new]":source, target);
		}
		
		transfers.add(new Transfer(source_, target_, entity));
	}
	
	/** @return facility for balancers to access the entity change and distribution history */
	public List<EntityJournal.Log> getJournal(final Duty<?> duty) {
	    return requireNonNull(sourceRefs.get(duty)).getJournal().getLogs();
	}
	
	/** leave a reason for distribution exclusion */
	public final void stuck(final Duty<?> duty, final NetworkLocation location) {
		requireNonNull(duty);
		final ShardEntity e = sourceRefs.get(duty);
		final ShardIdentifier shard = location.getId();
		e.getJournal().addEvent(
				e.getLastEvent(),
				EntityState.STUCK,
				shard,
				ChangePlan.PLAN_WITHOUT);
	}
	
	/* explicitly override a shard's content, client must look after consistency ! */
	public final void override(final NetworkLocation shard, final Set<Duty<?>> clusterx) {
		requireNonNull(shard);
		requireNonNull(clusterx);
		if (this.overrides == null) {
			this.overrides = new LinkedList<>();
		}
		final Set<ShardEntity> cluster = deref(clusterx);
		cluster.forEach(d->checkDuplicate(d));
		final Shard shard_ = deref(shard);
		final double remainingCap = validateOverride(shard_, cluster);
		if (!cluster.isEmpty() && log.isInfoEnabled()) {
			log.info("{}: Requesting Override: {}, remain cap: {}, with {}", getClass().getSimpleName(), shard_, remainingCap, 
				ShardEntity.toStringIds(cluster));
		}
		overrides.add(new Override(pallet, shard_, new LinkedHashSet<>(cluster), remainingCap));
	}
	private Set<ShardEntity> deref(final Set<Duty<?>> refs) {
		final Set<ShardEntity> reids = new HashSet<>(refs.size());
		refs.forEach(d -> reids.add(sourceRefs.get(d)));
		return reids;
	}
	
	private Shard deref(final NetworkLocation location) {
		for (Shard s: table.getScheme().getShards()) {
			if (s.getShardID().equals(location.getId())) {
				return s;
			}
		}
		return null;
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
		if (table.getScheme().getDutiesByShard(target).contains(entity)) {
			throw new BalancingException("bad transfer: duty %s already exist in target shard: %s", entity, target);
		}
		if (source==null) {
			final Shard location = table.getScheme().getDutyLocation(entity);
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
		for (ShardEntity duty: table.getBackstage().getDutiesCrud(EntityEvent.REMOVE, EntityState.PREPARED)) {
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
	
	boolean isEmpty() {
		return overrides==null && transfers==null;
	}
	
	/** effectively write overrided and transfered operations to plan 
	 * @return if the execution effectively generated any changes */ 
	final boolean write(final ChangePlan changePlan) {
		boolean anyChange = false;
		if (isEmpty()) {
			return false;
		}
		log.info("{}: Evaluating changes: {} transfers / {} overrides", getClass().getSimpleName(), 
				transfers!=null ? transfers.size() : 0, overrides!=null ? overrides.size() : 0);
		if (overrides!=null) {
			if (!anyExclusions()) {
				for (final Override ov : overrides) {
					anyChange |= ov.apply(changePlan, table);
				}
			}
		}
		if (transfers!=null) {
			// by now transfers dont use weights and capacities but SpillOver which is almost deprecated
			// balancers using transfers only make delta changes, without reassigning
			for (Transfer tr: transfers) {
				anyChange|=tr.apply(changePlan, table);
			}
		}
		return anyChange;
	}

	private boolean anyExclusions() {
	    boolean ret = false;
		for (final ShardEntity duty: table.getBackstage().getDutiesCrud(EntityEvent.CREATE, EntityState.PREPARED)) {
			if (duty.getDuty().getPalletId().equals(pallet.getId()) && 
					!inTransfers(duty) && 
					!inOverrides(duty) && 
					unfairlyIgnored(duty)) {
				ret = true;
				log.error("bad exclusion: duty: {} was just marked for creation, it must be balanced !", duty.toBrief());
			}
		}
		final Set<ShardEntity> deletions = table.getBackstage().getDutiesCrud(EntityEvent.REMOVE, EntityState.PREPARED);
		for (final ShardEntity curr: table.getScheme().getDutiesAttached()) {
			if (curr.getDuty().getPalletId().equals(pallet.getId()) && 
			        !deletions.contains(curr) && 
					!inTransfers(curr) && 
					!inOverrides(curr) && 
					unfairlyIgnored(curr)) {
				ret = true;
				log.warn("bad exclusion: duty: " + curr.toBrief() + " is in ptable and was excluded from balancing !");
			}
		}
		return ret;
	}
	
	private boolean unfairlyIgnored(ShardEntity duty) {
		if (isWeightedPallet()) {
			if (overrides!=null) {
				for (Override ov: overrides) {
					if (ov.getRemainingCap()>=duty.getDuty().getWeight()) {
						log.warn("{}: Override on: {} has a remaining cap: {} (out of :{}), enough to lodge duty: {}", 
								getClass().getSimpleName(), 
								ov.getShard(), 
								ov.getRemainingCap(), 
								ov.getShard().getCapacities().get(pallet).getTotal(), 
								duty.toBrief());
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

	private boolean inTransfers(final ShardEntity duty) {
		if (transfers==null || transfers.isEmpty()) {
			return false;
		}
		for (Transfer t: transfers) {
			if (t.getEntity().equals(duty)) {
				return true;
			}
		}
		
		return false;
	}

	private boolean inOverrides(final ShardEntity duty) {
		if (overrides==null || overrides.isEmpty()) {
			return false;
		}
		for (Override o: overrides) {
			if (o.getEntities().contains(duty)) {
				return true;
			}
		}
		return false;
	}

	List<Override> getOverrides() {
		return overrides;
	}
	
	List<Transfer> getTransfers() {
		return transfers;
	}

}
