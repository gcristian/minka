package io.tilt.minka.core.leader.data;

import static java.util.Arrays.asList;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Predicate;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.tilt.minka.api.Duty;
import io.tilt.minka.api.Entity;
import io.tilt.minka.api.Pallet;
import io.tilt.minka.core.leader.StateSentry;
import io.tilt.minka.domain.CommitTree;
import io.tilt.minka.domain.CommitTree.Log;
import io.tilt.minka.domain.EntityEvent;
import io.tilt.minka.domain.EntityRecord;
import io.tilt.minka.domain.EntityState;
import io.tilt.minka.domain.ShardEntity;
import io.tilt.minka.shard.Shard;
import io.tilt.minka.shard.ShardIdentifier;

/** 
 * Temporal state of modifications willing to be added to the {@linkplain CommitedState}
 * including inconsistencies detected by the sentry
 * Only maintainers: {@linkplain StateSentry} and {@linkplain UncommitedRepository}
 * */
public class UncommitedChanges {

	private static final Logger logger = LoggerFactory.getLogger(UncommitedChanges.class);

	private final Map<ShardIdentifier, Set<EntityRecord>> previousState = new HashMap<>();
	private final Map<Shard, Set<ShardEntity>> previousDomain;

    // creations and removes willing to be attached or detached to/from shards.
	private final Map<Pallet, ShardEntity> palletCrud;
	final Map<Duty, ShardEntity> dutyCrud;
	// absences in shards's reports
	private Map<Duty, ShardEntity> dutyMissings;
	// fallen shards's duties
	private Map<Duty, ShardEntity> dutyDangling;
	private Instant snaptake;
	
	// read-only snapshot for ChangePlanBuilder thread (not to be modified, stage remains MASTER)
	private UncommitedChanges snapshot;
	private boolean stealthChange;
	private boolean snap = false;
	private Instant lastStealthChange;
	
	public UncommitedChanges() {
		this.palletCrud = new HashMap<>();
		this.dutyCrud = new HashMap<>();
		this.dutyMissings = new HashMap<>();
		this.dutyDangling = new HashMap<>();
		this.previousDomain = new HashMap<>();
	}

	/** @return a frozen state of stage, so message-events threads 
	 * (threadpool-size independently) can still modify the instance for further change plans */
	public synchronized UncommitedChanges snapshot() {
		checkNotOnSnap();
		if (snapshot==null) {
			final UncommitedChanges tmp = new UncommitedChanges();
			tmp.dutyCrud.putAll(this.dutyCrud);
			tmp.dutyDangling.putAll(this.dutyDangling);
			tmp.dutyMissings.putAll(this.dutyMissings);
			tmp.palletCrud.putAll(this.palletCrud);
			tmp.snaptake = Instant.now();
			tmp.snap = true;
			this.snapshot = tmp;
		}
		return snapshot;
	}

	private void checkNotOnSnap() {
		if (snap) {
			throw new IllegalStateException("already a snapshot - bad usage !");
		}
	}
	
	public void dropSnapshot() {
		checkNotOnSnap();
		this.snapshot = null;
	}
	
	/** @return true if the last entity event ocurred before the last snapshot creation */
	public boolean after(final ShardEntity e) {
		if (snaptake==null) {
			throw new RuntimeException("bad call");
		}
		final Log last = e.getCommitTree().getLast();
		return last==null || snaptake.toEpochMilli() >=last.getHead().getTime();
	}

	public void setStealthChange(final boolean value) {
		checkNotOnSnap();
		if (!value && snapshot!=null) {
			// invalidate a turn-off on changes done after last snapshot
			if (this.snaptake.isBefore(this.lastStealthChange)) {
				if (logger.isInfoEnabled()) {
					logger.info("{}: invalidating stealth-change turn-off on changes post-snapshot", 
							getClass().getSimpleName());
				}
				return;
			}
		}
		if (value) {
			turnOnStealth(value);
		} else {
			this.stealthChange = value;
		}
	}
	/** @return true when the stage has changes worthy of distribution phase run  */
	public boolean isStealthChange() {
		checkNotOnSnap();
		return this.stealthChange;
	}
	
	//=======================================================================================
	
	/** guard the report to take it as truth once distribution runs and ShardEntity is loaded */
	public void feedFromPreviousState(final Collection<EntityRecord> records, final Shard where) {
		// changePlan is NULL only before 1st distribution
		// there's been a change of leader: i'm initiating with older followers
		final Map<EntityEvent.Type, StringBuilder> logmap = new HashMap<>(2);
		for (EntityRecord record: records) {
			EntityEvent lpv = learnPreviousDistro(record, where);
			EntityEvent lpd = null;
			if (record.getEntity() != null) {
				lpd = learnPreviousDomain(record.getEntity(), where, false);
			}
			if (logger.isInfoEnabled()) {
				for (EntityEvent ee: asList(lpv, lpd)) {
					if (ee!=null) {
						StringBuilder sb = logmap.get(ee.getType());
						if (sb==null) {
							logmap.put(ee.getType(), sb = new StringBuilder());
						}
						sb.append(record.getId()).append(',');
					}
				}
			}
		}
		if (!logmap.isEmpty()) {
			logmap.entrySet().forEach(e->
					logger.info("{}: Learnt type {} at [{}] {}", getClass().getSimpleName(), e.getKey(), 
							where, e.getValue().toString()));
		}
	}

	private EntityEvent learnPreviousDistro(final EntityRecord record, final Shard where) {
		// TODO add another constraint to avoid blindly trusting the shard
		// check the log is really the last (another shard could say the same, who to trust ?)
		// the beat comes from A, and there's an Attach on A: pretty confident...could be a lattest Attach on B
		// and another beat from B saying the same... and actually being the real trustworthy
		if (record.getCommitTree().exists(where.getShardID().getId(), EntityEvent.ATTACH)) {
			Set<EntityRecord> set = previousState.get(where.getShardID());
			if (set==null) {
				previousState.put(where.getShardID(), set = new HashSet<>());
			}
			if (record.getEntity()!=null) {
				learnPreviousDomain(record.getEntity(), where, false);
			}
			if (set.add(record)) {
				return EntityEvent.ATTACH;
			}
		}
		return null;
	}

	private EntityEvent learnPreviousDomain(final ShardEntity duty, final Shard where, final boolean stockCheck) {
		if (!stockCheck || duty.getCommitTree().exists(where.getShardID().getId(), EntityEvent.STOCK)) {
			Set<ShardEntity> byShard = previousDomain.get(where);
			if (byShard==null) {
				previousDomain.put(where, byShard=new HashSet<>());
			}
			// dont replace it
			final boolean existed = byShard.contains(duty);
			if (!existed) {
				final CommitTree fresh = new CommitTree();
				fresh.addEvent(
					EntityEvent.CREATE, 
					EntityState.PREPARED, 
					where.getShardID(), 
					0);
				duty.replaceTree(fresh);
			}
			if (existed || byShard.add(duty)) {
				return EntityEvent.STOCK;
			}
		}
		return null;
	}

	/** @return previous state's domain-commited entities */
	public Set<ShardEntity> drainLearningDomain() {
		final Set<ShardEntity> drained = new HashSet<>(); 
		for (Collection<ShardEntity> c: previousDomain.values()) {
			drained.addAll(c);
		}
		previousDomain.clear();
		return drained;
	}
	
	
	boolean isPreviousState() {
		return !this.previousState.isEmpty();
	}
	
	boolean isPreviousDomain() {
		return !this.previousDomain.isEmpty();
	}
	
	/** @return TRUE if fixed master's duty with previous state */
	public boolean patchCommitTreesWithLearningDistro(
			final Set<ShardEntity> duties, 
			final BiConsumer<ShardIdentifier, ShardEntity> bc) {
		boolean ret = false;
		if (!previousState.isEmpty()) {
			final EntityEvent event = EntityEvent.ATTACH;
			for (Map.Entry<ShardIdentifier, Set<EntityRecord>> e: previousState.entrySet()) {
				boolean found = false;
				if (logger.isInfoEnabled()) {
					logger.info("{}: Patching scheme ({}) w/previous commit-trees: {}", getClass().getSimpleName(), 
							event, EntityRecord.toStringIds(e.getValue()));
				}
				for (EntityRecord r: e.getValue()) {
					for (ShardEntity d: duties) {
						if (d.getDuty().getId().equals(r.getId())) {
							found = true;
							d.replaceTree(r.getCommitTree());
							bc.accept(e.getKey(), d);
							ret = true;
							break;
						}
					}
					if (!found) {
						logger.error("{}: Shard {} reported an unloaded duty from previous distribution: {}", 
								getClass().getSimpleName(), e.getKey(), r.getId());
					}
				}
			}
			this.previousState.clear();
		}
		return ret;
	}
	

	//=======================================================================================

	/** @return read only set */
	public Collection<ShardEntity> getDutiesDangling() {
		return Collections.unmodifiableCollection(this.dutyDangling.values());
	}
	
	private void turnOnStealth(final boolean value) {
		if (value) {
			stealthChange = true;
			lastStealthChange = Instant.now();
		}
	}
	
	/** @return TRUE if last stealth change timestamp has got far away more than threshold */
	public boolean stealthOverThreshold(final long thresholdMillis) {
		final long diff = Instant.now().getEpochSecond() - lastStealthChange.getEpochSecond();
		return (diff * 1000 ) > thresholdMillis;
		
	}
	
	/** @return true if added for the first time else is being replaced */
	public boolean addDangling(final Collection<ShardEntity> dangling) {
		boolean added = false; 
		for (ShardEntity d: dangling) {
			added |= this.dutyDangling.put(d.getDuty(), d) == null;
		}
		turnOnStealth(added);
		return added;
	}
	
	public boolean addDangling(final ShardEntity dangling) {
		final boolean added = this.dutyDangling.put(dangling.getDuty(), dangling) == null;
		turnOnStealth(added);
		return added;

	}
	public void cleanAllocatedDanglings() {
		checkNotOnSnap();
		if (snapshot!=null && !dutyDangling.isEmpty()) {
			remove(snapshot.dutyDangling, dutyDangling, s->s.getLastState() != EntityState.STUCK);
		}
	}
	private void remove(
			final Map<? extends Entity, ShardEntity> deletes, 
			final Map<? extends Entity, ShardEntity> target, 
			final Predicate<ShardEntity> test) {
		for (Map.Entry<? extends Entity, ShardEntity> e: deletes.entrySet()) {
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
		turnOnStealth(added);
		return added;
	}

	public void addAllCrudDuty(final Collection<ShardEntity> coll, final BiConsumer<Duty, Boolean> callback) {
		// the uniqueness of it's wrapped object doesnt define the uniqueness of the wrapper
		// updates and transfer go in their own manner
		boolean added = false; 
		for (ShardEntity e: coll) {
			final boolean v = dutyCrud.put(e.getDuty(), e) == null;
			callback.accept(e.getDuty(), v);
			added |= v;
		}
		turnOnStealth(added);
	}
	
	public Collection<ShardEntity> getDutiesCrud() {
		return Collections.unmodifiableCollection(this.dutyCrud.values());
	}
	public boolean removeCrud(final ShardEntity entity) {
		boolean removed = false;
		final ShardEntity candidate = dutyCrud.get(entity.getDuty());
		// consistency check: only if they're the same action 
		if (candidate!=null) {// && candidate.getLastEvent()==entity.getLastEvent().getRootCause()) {
			removed =this.dutyCrud.remove(entity.getDuty()) != null;
			turnOnStealth(removed);
		}
		return removed;
	}

	/** @return read only set */
	public Collection<ShardEntity> getDutiesMissing() {
		return Collections.unmodifiableCollection(this.dutyMissings.values());
	}
	
	public void clearAllocatedMissing() {
		checkNotOnSnap();
		if (snapshot!=null && !dutyMissings.isEmpty()) {
			remove(snapshot.dutyMissings, dutyMissings, s->s.getLastState() != EntityState.STUCK);
		}
	}
	
	public boolean addMissing(final Collection<ShardEntity> duties) {
		checkNotOnSnap();
		boolean added = false;
		for (ShardEntity d: duties) {
			added |= dutyMissings.put(d.getDuty(), d) == null;
		}
		turnOnStealth(added);
		return true;
	}

	public void removeCrudDuties() {
		checkNotOnSnap();
		this.dutyCrud.clear();
		turnOnStealth(true);
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
			.filter(e -> (eventPredicate == null || eventPredicate.test(e.getCommitTree().getLast().getEvent())) 
				&& (statePredicate == null || (statePredicate.test(e.getCommitTree().getLast().getLastState()))))
			.forEach(consumer);
	}
	public ShardEntity getCrudByDuty(final Duty duty) {
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