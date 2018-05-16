package io.tilt.minka.core.leader.data;

import java.io.Serializable;
import java.time.Instant;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.tilt.minka.api.Duty;
import io.tilt.minka.api.Entity;
import io.tilt.minka.api.Pallet;
import io.tilt.minka.core.leader.SchemeSentry;
import io.tilt.minka.domain.EntityEvent;
import io.tilt.minka.domain.EntityState;
import io.tilt.minka.domain.ShardEntity;
import io.tilt.minka.domain.EntityJournal.Log;

/** 
 * Temporal state of modifications willing to be added to the {@linkplain Scheme}
 * including inconsistencies detected by the sentry
 * Only maintainers: {@linkplain SchemeSentry} and {@linkplain SchemeRepository}
 * */
public class Backstage {

	private static final Logger logger = LoggerFactory.getLogger(Backstage.class);

    // creations and removes willing to be attached or detached to/from shards.
	final Map<Pallet<?>, ShardEntity> palletCrud;
	final Map<Duty<?>, ShardEntity> dutyCrud;
	// absences in shards's reports
	private Map<Duty<?>, ShardEntity> dutyMissings;
	// fallen shards's duties
	private Map<Duty<?>, ShardEntity> dutyDangling;
	private Instant snaptake;
	
	// read-only snapshot for ChangePlanBuilder thread (not to be modified, backstage remains MASTER)
	private Backstage snapshot;
	private boolean stealthChange;
	
	/** @return a frozen state of Backstage, so message-events threads 
	 * (threadpool-size independently) can still modify the instance for further change plans */
	public synchronized Backstage snapshot() {
		if (snapshot==null) {
			final Backstage tmp = new Backstage();
			tmp.dutyCrud.putAll(this.dutyCrud);
			tmp.dutyDangling.putAll(this.dutyDangling);
			tmp.dutyMissings.putAll(this.dutyMissings);
			tmp.palletCrud.putAll(this.palletCrud);
			tmp.snaptake = Instant.now();
			this.snapshot = tmp;
		}
		return snapshot;
	}
	
	public void dropSnapshot() {
		this.snapshot = null;
	}
	
	/** @return true if the last entity event ocurred before the last snapshot creation */
	public boolean after(final ShardEntity e) {
		if (snaptake==null) {
			throw new RuntimeException("bad call");
		}
		final Log last = e.getJournal().getLast();
		return last==null || snaptake.toEpochMilli() >=last.getHead().getTime();
	}

	public Backstage() {
		this.palletCrud = new HashMap<>();
		this.dutyCrud = new HashMap<>();
		this.dutyMissings = new HashMap<>();
		this.dutyDangling = new HashMap<>();
	}

	public void stealthChange(final boolean value) {
		this.stealthChange = value;
	}
	/** @return true when the backstage has changes worthy of distribution phase run  */
	public boolean isStealthChange() {
		return this.stealthChange;
	}
	
	/** @return read only set */
	public Collection<ShardEntity> getDutiesDangling() {
		return Collections.unmodifiableCollection(this.dutyDangling.values());
	}
	
	/** @return true if added for the first time else is being replaced */
	public boolean addDangling(final Collection<ShardEntity> dangling) {
		boolean added = false; 
		for (ShardEntity d: dangling) {
			added |= this.dutyDangling.put(d.getDuty(), d) == null;
		}
		stealthChange |= added;
		return added;
	}
	public boolean addDangling(final ShardEntity dangling) {
		final boolean added = this.dutyDangling.put(dangling.getDuty(), dangling) == null;
		stealthChange |= added;
		return added;

	}
	public void cleanAllocatedDanglings() {
		if (snapshot!=null && !dutyDangling.isEmpty()) {
			remove(snapshot.dutyDangling, dutyDangling, s->s.getLastState() != EntityState.STUCK);
		}
	}
	private void remove(
			final Map<? extends Entity<? extends Serializable>, ShardEntity> deletes, 
			final Map<? extends Entity<? extends Serializable>, ShardEntity> target, 
			final Predicate<ShardEntity> test) {
		for (Map.Entry<? extends Entity<? extends Serializable>, ShardEntity> e: deletes.entrySet()) {
			if (test.test(e.getValue())) {
				target.remove(e.getKey());
			}
		}			
	}
	
	public int accountCrudDuties() {
		return this.dutyCrud.size();
	}

	/* add it for the next Distribution cycle consideration */
	/** @return true if added for the first time else is being replaced */
	public boolean addCrudDuty(final ShardEntity duty) {
		// the uniqueness of it's wrapped object doesnt define the uniqueness of the wrapper
		// updates and transfer go in their own manner
		final boolean added = dutyCrud.put(duty.getDuty(), duty) == null;
		stealthChange |= added;
		return added;

	}
	public void addAllCrudDuty(final Collection<ShardEntity> coll, final BiConsumer<Duty<?>, Boolean> callback) {
		// the uniqueness of it's wrapped object doesnt define the uniqueness of the wrapper
		// updates and transfer go in their own manner
		boolean added = false; 
		for (ShardEntity e: coll) {
			final boolean v = dutyCrud.put(e.getDuty(), e) == null;
			callback.accept(e.getDuty(), v);
			added |= v;
		}
		stealthChange |= added;
	}
	
	public Collection<ShardEntity> getDutiesCrud() {
		return Collections.unmodifiableCollection(this.dutyCrud.values());
	}
	public boolean removeCrud(final ShardEntity entity) {
		boolean removed = false;
		final ShardEntity candidate = dutyCrud.get(entity.getDuty());
		// consistency check: only if they're the same action 
		if (candidate!=null) {// && candidate.getLastEvent()==entity.getLastEvent().getRootCause()) {
			removed =this.dutyCrud.remove(entity.getDuty()) == null;
			stealthChange |= removed;
		}
		return removed;
	}

	/** @return read only set */
	public Collection<ShardEntity> getDutiesMissing() {
		return Collections.unmodifiableCollection(this.dutyMissings.values());
	}
	
	public void clearAllocatedMissing() {
		if (snapshot!=null && !dutyMissings.isEmpty()) {
			remove(snapshot.dutyMissings, dutyMissings, s->s.getLastState() != EntityState.STUCK);
		}
	}
	
	public boolean addMissing(final Collection<ShardEntity> duties) {
		boolean added = false;
		for (ShardEntity d: duties) {
			added |= dutyMissings.put(d.getDuty(), d) == null;
		}
		stealthChange |= added;
		return true;

	}

	public boolean addMissing(final ShardEntity duty) {
		final boolean replaced = this.dutyMissings.put(duty.getDuty(), duty) !=null;
		stealthChange |= replaced;
		return true;
	}

	public void removeCrudDuties() {
		this.dutyCrud.clear();
		stealthChange = true;
	}
	public int sizeDutiesCrud(final Predicate<EntityEvent> event, final Predicate<EntityState> state) {
		final int[] size = new int[1];
		onEntitiesCrud(getDutiesCrud(), event, state, e->size[0]++);
		return size[0];
	}
	public void findDutiesCrud(
			final Predicate<EntityEvent> event, 
			final Predicate<EntityState> state, 
			final Consumer<ShardEntity> consumer) {
		onEntitiesCrud(getDutiesCrud(), event, state, consumer);
	}
	public void findPalletsCrud(
			final Predicate<EntityEvent> event, 
			final Predicate<EntityState> state, 
			final Consumer<ShardEntity> consumer) {
		onEntitiesCrud(palletCrud.values(), event, state, consumer);
	}
	
	private void onEntitiesCrud(
			final Collection<ShardEntity> coll, 
			final Predicate<EntityEvent> eventPredicate, 
			final Predicate<EntityState> statePredicate, 
			final Consumer<ShardEntity> consumer) {
		coll.stream()
			.filter(e -> (eventPredicate == null || eventPredicate.test(e.getJournal().getLast().getEvent())) 
				&& (statePredicate == null || (statePredicate.test(e.getJournal().getLast().getLastState()))))
			.forEach(consumer);
	}
	public ShardEntity getCrudByDuty(final Duty<?> duty) {
		return this.dutyCrud.get(duty);
	}

	public void logStatus() {
		if (!dutyMissings.isEmpty()) {
			logger.warn("{}: {} Missing duties: [ {}]", getClass().getSimpleName(), dutyMissings.size(),
					dutyMissings.toString());
		}
		if (!dutyDangling.isEmpty()) {
			logger.warn("{}: {} Dangling duties: [ {}]", getClass().getSimpleName(), dutyDangling.size(),
					dutyDangling.toString());
		}
		if (dutyCrud.isEmpty()) {
			logger.info("{}: no CRUD duties", getClass().getSimpleName());
		} else {
			logger.info("{}: with {} CRUD duties: [ {}]", getClass().getSimpleName(), dutyCrud.size(),
					ShardEntity.toStringIds(getDutiesCrud()));
		}
	}
	
}