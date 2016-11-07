/**
 *  Copyright (c) 2011-2015 Zauber S.A.  -- All rights reserved
 */

package io.tilt.minka.core.leader.distributor;

import static io.tilt.minka.domain.ShardEntity.State.PREPARED;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.lang.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.util.concurrent.AtomicDouble;

import io.tilt.minka.api.BalancingException;
import io.tilt.minka.api.Pallet;
import io.tilt.minka.core.leader.PartitionTable;
import io.tilt.minka.core.leader.distributor.Balancer.Strategy;
import io.tilt.minka.domain.EntityEvent;
import io.tilt.minka.domain.Shard;
import io.tilt.minka.domain.ShardCapacity.Capacity;
import io.tilt.minka.domain.ShardEntity;
import io.tilt.minka.domain.ShardEntity.State;
import io.tilt.minka.domain.ShardState;

/** 
 * Media for {@linkplain Balancer} to request transfers and overrides on the {@linkplain Roadmap}
 * while ignoring its internals and relying on this for coherence and consistency 
 * It helps to balancer extensibility.
 * 
 * everything ends up in a: roadmap.ship(...)
 * 
 * In case the duty already exists in target shard, nothing will happen.
 * So if all balancers in all pallets has always the same behaviour, 
 * and there're no CRUD operations, the partitiontable should not change.
 * 
 * @author Cristian Gonzalez
 * @since Oct 29, 2016
 */
public class Migrator {

	private final Logger log = LoggerFactory.getLogger(getClass());
	
	private final PartitionTable table;
	private final Roadmap roadmap;
	private final Pallet<?> pallet;
	private Boolean isWeightedPallet;
	private List<Override> overrides;
	private List<Transfer> transfers;
	
	protected Migrator(final PartitionTable table, final Roadmap roadmap, final Pallet<?> pallet) {
		super();
		Validate.notNull(pallet);
		Validate.notNull(roadmap);
		Validate.notNull(table);
		this.table = table;
		this.roadmap = roadmap;
		this.pallet = pallet;
	}

	/** specifically transfer from a Source to a Target */
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
		log.info("{}: Requesting Transfer: {} from: {} to: {}", getClass().getSimpleName(), entity.toBrief(), 
				source==null ? "[new]":source, target);
		transfers.add(new Transfer(source, target ,entity));
	}
	/** explicitly override a shard's content, client must look after consistency ! */
	public final void override(final Shard shard, final Set<ShardEntity> cluster) {
		Validate.notNull(shard);
		Validate.notNull(cluster);
		if (this.overrides == null) {
			this.overrides = new ArrayList<>();
		}
		cluster.forEach(d->checkDuplicate(d));
		final double remainingCap = validateOverride(shard, cluster);
		log.info("{}: Requesting Override: {}, remain cap: {}, with {}", getClass().getSimpleName(), shard, remainingCap, 
				ShardEntity.toStringIds(cluster));
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
					throw new BalancingException("bad override: duties weight: %s are way heavier than shard: %s, capacity: %s", 
							accum, target, cap.getTotal());
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
				if (strat.getBalancer().equals(pallet.getStrategy().getBalancer()) && 
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
		if (entity.getDutyEvent()==EntityEvent.REMOVE) {
			throw new BalancingException("bad transfer: duty: %s is marked for deletion, cannot be balanced", entity);
		}
		for (ShardEntity duty: table.getNextStage().getDutiesCrudWithFilters(EntityEvent.REMOVE, State.PREPARED)) {
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
	
	/** effectively write overrided and transfered operations to roadmap object */ 
	protected final void execute() {
		if (overrides==null && transfers ==null) {
			log.warn("{}: nothing to execute: migrator without overrides or transfers !", getClass().getSimpleName());
			return;
		}
		log.info("{}: Evaluating {} transfers and {} overrides", getClass().getSimpleName(), 
				transfers!=null ? transfers.size() : 0, overrides!=null ? overrides.size() : 0);
		if (overrides!=null) {
			checkExclusions(); // balancers using transfers only make delta changes, without reassigning
			for (final Override ov: overrides) {			
				final Set<ShardEntity> current = table.getStage().getDutiesByShard(ov.getPallet(), ov.getShard());
				if (log.isDebugEnabled()) {
					log.debug("{}: cluster built {}", getClass().getSimpleName(), ov.getEntities());
					log.debug("{}: currents at shard {} ", getClass().getSimpleName(), current);
				}
				dettachDelta(ov.getEntities(), ov.getShard(), current);
				attachDelta(ov.getEntities(), ov.getShard(), current);	
			}
		}
		if (transfers!=null) {
			transfers.forEach(t->dettachAttach(t));
		}
	}

	/* dettach in prev. source, attach to next target */
	private void dettachAttach(final Transfer tr) {
		final ShardEntity entity = tr.getEntity();
		final Shard location = table.getStage().getDutyLocation(entity);
		if (location!=null && location.equals(tr.getSource())) {
			// the ptable stays the same
			return;
		}
		if (tr.getSource()!=null) {
			tr.getEntity().registerEvent(EntityEvent.DETACH, PREPARED);
			roadmap.ship(tr.getSource(), entity);
		}
		ShardEntity assign = ShardEntity.copy(entity);
		assign.registerEvent(EntityEvent.ATTACH, PREPARED);
		roadmap.ship(tr.getTarget(), assign);
		log.info("{}: Executing transfer from: {} to: {}, Duty: {}", getClass().getSimpleName(),
			tr.getSource()!=null ? tr.getSource().getShardID() : "[new]", tr.getTarget().getShardID(), assign.toString());
	}

	private boolean unfairlyIgnored(ShardEntity duty) {
		if (isWeightedPallet()) {
			if (overrides!=null) {
				for (Override ov: overrides) {
					if (ov.getRemainingCap()>=duty.getDuty().getWeight()) {
						log.warn("{}: Override of shard: {} has a remaining cap: {} (out of original:{}), enough to lodge "
								+ "duty weight: {}", getClass().getSimpleName(), ov.getShard(), ov.getRemainingCap(), 
								ov.getShard().getCapacities().get(pallet).getTotal(), duty.getDuty().getWeight());
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
		for (final ShardEntity duty: table.getNextStage().getDutiesCrudWithFilters(EntityEvent.CREATE, State.PREPARED)) {
			if (duty.getDuty().getPalletId().equals(pallet.getId()) && !inTransfers(duty) && 
					!inOverrides(duty) && unfairlyIgnored(duty)) {
				log.warn("bad exclusion: duty: %s was just marked for creation, it must be balanced !", duty);
			}
		}
		Set<ShardEntity> deletions = table.getNextStage().getDutiesCrudWithFilters(EntityEvent.REMOVE, State.PREPARED);
		for (final ShardEntity curr: table.getStage().getDutiesAttached()) {
			if (curr.getDuty().getPalletId().equals(pallet.getId()) && !deletions.contains(curr) && 
					!inTransfers(curr) && !inOverrides(curr) && unfairlyIgnored(curr)) {
				log.warn("bad exclusion: duty: " + curr + " is in ptable and was excluded from balancing !");
			}
		}
	}

	private boolean inTransfers(final ShardEntity duty) {
		return transfers!=null && transfers.stream().filter(t->t.getEntity().equals(duty)).count()>0;
	}

	private boolean inOverrides(final ShardEntity duty) {
		return overrides!=null && overrides.stream().filter(o->o.getEntities().contains(duty)).count()>0;
	}

	protected static class Override {
		private final Pallet<?> pallet;
		private final Shard shard;
		private final Set<ShardEntity> entities;
		private final double remainingCap;
		protected Override(final Pallet<?> pallet, Shard shard, final Set<ShardEntity> entities, final double remainingCap) {
			super();
			this.pallet = pallet;
			this.shard = shard;
			this.entities = entities;
			this.remainingCap = remainingCap;;
		}
		public Pallet<?> getPallet() {
			return this.pallet;
		}
		public Shard getShard() {
			return this.shard;
		}
		public Set<ShardEntity> getEntities() {
			return this.entities;
		}
		public double getRemainingCap() {
			return this.remainingCap;
		}
	}
	
	protected static class Transfer {
		private final Shard source;
		private final Shard target;
		private final ShardEntity entity;
		protected Transfer(Shard source, Shard target, ShardEntity entity) {
			super();
			this.source = source;
			this.target = target;
			this.entity = entity;
		}
		public Shard getSource() {
			return this.source;
		}
		public Shard getTarget() {
			return this.target;
		}
		public ShardEntity getEntity() {
			return this.entity;
		}
	}

	/* dettach anything living in the shard outside what's coming
	 * null or empty cluster translates to: dettach all existing */
	private final void dettachDelta(final Set<ShardEntity> clusterSet, final Shard shard, final Set<ShardEntity> currents) {
		List<ShardEntity> detaching = clusterSet==null ? new ArrayList<>(currents) :
			currents.stream().filter(i -> !clusterSet.contains(i)).collect(Collectors.toList());

		if (detaching.isEmpty() && log.isDebugEnabled()) {
			log.info("{}: Override-dettach shard: {} has no Detachings (calculated are all already attached)",
					getClass().getSimpleName(), shard);
		}

		StringBuilder logg = new StringBuilder();
		for (ShardEntity detach : detaching) {
			// copy because in latter cycles this will be assigned
			// so they're traveling different places
			final ShardEntity copy = ShardEntity.copy(detach);
			copy.registerEvent(EntityEvent.DETACH, PREPARED);
			roadmap.ship(shard, copy);
			logg.append(copy.getEntity().getId()).append(", ");
		}
		if (logg.length() > 0) {
			log.info("{}: Executing Override-dettach from: {}, duties: (#{}) {}", getClass().getSimpleName(), shard.getShardID(),
				detaching.size(), logg.toString());
		}
	}

	/* attach what's not already living in that shard */
	private final void attachDelta(final Set<ShardEntity> clusterSet, final Shard shard, final Set<ShardEntity> currents) {
		StringBuilder logg;
		if (clusterSet != null) {
			final List<ShardEntity> attaching = clusterSet.stream().filter(i -> !currents.contains(i))
					.collect(Collectors.toList());

			if (attaching.isEmpty() && log.isDebugEnabled()) {
				log.info("{}: Override-attach shard: {} has no New Attachments (calculated are all already attached)",
						getClass().getSimpleName(), shard);
			}
			logg = new StringBuilder();
			for (ShardEntity attach : attaching) {
				// copy because in latter cycles this will be assigned
				// so they're traveling different places
				final ShardEntity copy = ShardEntity.copy(attach);
				copy.registerEvent(EntityEvent.ATTACH, PREPARED);
				roadmap.ship(shard, copy);
				logg.append(copy.getEntity().getId()).append(", ");
			}
			if (logg.length() > 0) {
				log.info("{}: Executing Override-attach shard: {}, duty: (#{}) {}", getClass().getSimpleName(), shard.getShardID(),
					attaching.size(), logg.toString());
			}
		}
	}
	
}
