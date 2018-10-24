package io.tilt.minka.core.leader.data;

import static java.util.Collections.synchronizedMap;

import java.time.Instant;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.tilt.minka.core.leader.StateSentry;
import io.tilt.minka.core.leader.distributor.ChangeFeature;
import io.tilt.minka.domain.CommitTree.Log;
import io.tilt.minka.domain.EntityEvent;
import io.tilt.minka.domain.EntityState;
import io.tilt.minka.domain.ShardEntity;
import io.tilt.minka.model.Duty;
import io.tilt.minka.model.Entity;
import io.tilt.minka.model.Pallet;

/** 
 * Temporal state of modifications willing to be added to the {@linkplain CommittedState}
 * including inconsistencies detected by the sentry
 * Only maintainers: {@linkplain StateSentry} and {@linkplain CrudRepository}
 * */
@SuppressWarnings({"unchecked", "serial", "rawtypes"})
public class DirtyState {

	/** split volume in different plans: dont promote more than NN CommitRequests to the next ChangePlan */
	private static final int MAX_CR_PROMOTION_SIZE = 200;
	
	private static final Logger logger = LoggerFactory.getLogger(DirtyState.class);

	private final Map<EntityEvent, Map<Duty, CommitRequest>> commitRequests = synchronizedMap(new HashMap() {{
		put(EntityEvent.ATTACH, synchronizedMap(new HashMap<>()));
		put(EntityEvent.CREATE, synchronizedMap(new HashMap<>()));
		put(EntityEvent.REMOVE, synchronizedMap(new HashMap<>()));
	}});
	
	// creations and removes willing to be attached or detached to/from shards.
	private final Map<Pallet, ShardEntity> palletCrud = synchronizedMap(new HashMap<>());
	// absences in shards's reports
	private Map<Duty, ShardEntity> dutyMissings = synchronizedMap(new HashMap<>());
	// fallen shards's duties
	private Map<Duty, ShardEntity> dutyDangling = synchronizedMap(new HashMap<>());
	private Instant snaptake;
	
	// read-only snapshot for ChangePlanBuilder thread (not to be modified, stage remains MASTER)
	private DirtyState snapshot;
	private boolean stealthChange;
	private boolean snap = false;
	private Instant lastStealthChange;
	
	/* only for featuring traceability*/
	private boolean limitedPromotion;
	private Set<ChangeFeature> clusterFeatures = new HashSet<>(2); 
	
	/** @return a frozen state of stage, so message-events threads 
	 * (threadpool-size independently) can still modify the instance for further change plans */
	public synchronized DirtyState snapshot() {
		checkNotOnSnap();
		if (snapshot==null) {
			final DirtyState snap = new DirtyState();
			// copy CRs to a new limited structure
			for (Map.Entry<EntityEvent, Map<Duty, CommitRequest>> e: commitRequests.entrySet()) {
				snap.commitRequests.put(e.getKey(), limit(snap, e));				
			}
			snap.clusterFeatures.addAll(clusterFeatures);
			clusterFeatures.clear();
			snap.dutyDangling.putAll(this.dutyDangling);
			snap.dutyMissings.putAll(this.dutyMissings);
			snap.palletCrud.putAll(this.palletCrud);
			snap.snaptake = Instant.now();
			snap.snap = true;
			this.snapshot = snap;
		}
		return snapshot;
	}

	/** 
	 * @return new battery of CRs limitted to max after 
	 * removing them from current version for next read 
	 */
	private Map<Duty, CommitRequest> limit(final DirtyState tmp, final Map.Entry<EntityEvent, Map<Duty, CommitRequest>> e) {
		final Map<Duty, CommitRequest> eelimited = e.getValue().entrySet().stream()
				.limit(MAX_CR_PROMOTION_SIZE)
				.collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, 
						(x,y)->x, LinkedHashMap::new));
				
		if (e.getValue().size() > MAX_CR_PROMOTION_SIZE) {
			tmp.limitedPromotion = true;
			// remove current from next plan
			e.getValue().keySet().removeAll(eelimited.keySet());
			logger.warn("{}: Limiting CommitRequests promotion ({} left to next plan)",
					getClass().getSimpleName(), e.getValue().size());
		}
		return eelimited;
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
	public boolean CRUDIsBefore(final ShardEntity e, final EntityEvent ee) {
		if (snaptake==null) {
			throw new RuntimeException("bad call");
		}
		final Log last = e.getCommitTree().existsWithLimit(ee, e.getCommitTree().getCreationTimestamp().getTime());
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
	
	public boolean isLimitedPromotion() {
		return limitedPromotion;
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

	
	// ====================================================================================================
	
	
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
		turnOnStealth(added);
		return added;
	}
	
	public boolean addDangling(final ShardEntity dangling) {
		final boolean added = this.dutyDangling.put(dangling.getDuty(), dangling) == null;
		turnOnStealth(added);
		return added;

	}
	public void cleanAllocatedDanglings(final Predicate<ShardEntity> test) {
		checkNotOnSnap();
		if (snapshot!=null && !dutyDangling.isEmpty()) {
			remove(snapshot.dutyDangling, dutyDangling, s->s.getLastState() != EntityState.STUCK 
					&& test==null || (test!=null && test.test(s)));
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

	/** @return read only set */
	public Collection<ShardEntity> getDutiesMissing() {
		return Collections.unmodifiableCollection(this.dutyMissings.values());
	}
	
	public void clearAllocatedMissing(final Predicate<ShardEntity> test) {
		checkNotOnSnap();
		if (snapshot!=null && !dutyMissings.isEmpty()) {
			remove(snapshot.dutyMissings, dutyMissings, s->s.getLastState() != EntityState.STUCK 
					&& test==null || (test!=null && test.test(s)));
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
	
	// ====================================================================================================
	
	public int commitRequestsSize() {
		return this.commitRequests.get(EntityEvent.CREATE).size()
				+ this.commitRequests.get(EntityEvent.REMOVE).size()
				+ this.commitRequests.get(EntityEvent.ATTACH).size();
	}

	/* add it for the next Distribution cycle consideration */
	/** @return true if added for the first time else is being replaced */
	public void createCommitRequests(
			final EntityEvent ee, 
			final Collection<ShardEntity> coll, 
			final BiConsumer<Duty, Boolean> callback,
			final boolean respondState) {
		
		final Map<Duty, CommitRequest> map = commitRequests.get(ee);
		
		// updates and transfer go in their own manner
		boolean added = false; 
		for (ShardEntity e: coll) {
			final CommitRequest request = new CommitRequest(e, respondState);
			final boolean put = map.put(e.getDuty(), request) == null;
			if (callback!=null) {
				callback.accept(e.getDuty(), put);
			}
			added |= put;
		}
		turnOnStealth(added);
	}
	
	public Collection<ShardEntity> getDutiesCrud() {
		return commitRequests.values().stream()
			.flatMap(x->x.values().stream().map(CommitRequest::getEntity))
			.collect(Collectors.toList());
	}
	
	public EntityEvent existCommitRequest(final Duty duty) {
		for(Map.Entry<EntityEvent, Map<Duty, CommitRequest>> e: commitRequests.entrySet()) {
			if (e.getValue().get(duty)!=null) {
				return e.getKey();
			}
		}
		return null;
	}
	
	/** @return a CR matching {event & entity} if any, and step state to next according event */
	public CommitRequest updateCommitRequest(final EntityEvent event, final ShardEntity entity) {
		return updateCommitRequest(event, entity, null);
	}
	
	public CommitRequest updateCommitRequest(final EntityEvent event, final ShardEntity entity, final CommitState arg) {
		final EntityEvent slot = event.getType()==EntityEvent.Type.CRUD ? event : event.getUserCause();
		final Map<Duty, CommitRequest> map = commitRequests.get(slot);
		CommitRequest request = map.get(entity.getDuty());
		if (request!=null) {
			final CommitState prev = request.getState();
			final CommitState next = arg!=null ? arg : request.getState().next(event);
			if (next!=null) {
				request.setState(next);
				if (next.isEnded()) {
					map.remove(entity.getDuty());
				}
				if (logger.isDebugEnabled()) {
					logger.debug("{}: flowing {} -> {}: {} ({})", getClass().getSimpleName(),
						prev, next, CommitRequest.class.getSimpleName(), 
						request.getEntity().getDuty().getId(), next.isEnded() ? "discarding":"");
				}
			}
		}		
		return request;
	}

	// ====================================================================================================
	

	public int sizeDutiesCrud(final EntityEvent event, final Predicate<EntityState> state) {
		final int[] size = new int[1];
		onEntitiesCrud(event, state, e->size[0]++);
		return size[0];
	}
	public void findDutiesCrud(
			final EntityEvent event, 
			final Predicate<EntityState> state, 
			final Consumer<ShardEntity> consumer) {
		onEntitiesCrud(event, state, consumer);
	}
	public void findPalletsCrud(
			final Predicate<EntityEvent> event, 
			final Predicate<EntityState> state, 
			final Consumer<ShardEntity> consumer) {
		onPalletsCrud(event, state, consumer);
	}
	
	private void onEntitiesCrud(
			final EntityEvent eventPredicate, 
			final Predicate<EntityState> statePredicate, 
			final Consumer<ShardEntity> consumer) {
		final Map<Duty, CommitRequest> x = commitRequests.get(eventPredicate);
		x.values().stream().map(CommitRequest::getEntity)
			.filter(e -> (eventPredicate == null || eventPredicate==(e.getCommitTree().getLast().getEvent())) 
				&& (statePredicate == null || (statePredicate.test(e.getCommitTree().getLast().getLastState()))))
			.forEach(consumer);
	}

	private void onPalletsCrud(
			final Predicate<EntityEvent> eventPredicate, 
			final Predicate<EntityState> statePredicate, 
			final Consumer<ShardEntity> consumer) {
		palletCrud.values().stream()
			.filter(e -> (eventPredicate == null || eventPredicate.test(e.getCommitTree().getLast().getEvent())) 
				&& (statePredicate == null || (statePredicate.test(e.getCommitTree().getLast().getLastState()))))
			.forEach(consumer);
	}

	public void logStatus() {
		if (!dutyMissings.isEmpty()) {
			logger.warn("{}: {} Missing duties: [ {}]", getClass().getSimpleName(), dutyMissings.size(),
					ShardEntity.toDutyStringIds(dutyMissings.keySet()));
		}
		if (!dutyDangling.isEmpty()) {
			logger.warn("{}: {} Dangling duties: [ {}]", getClass().getSimpleName(), dutyDangling.size(),
					ShardEntity.toDutyStringIds(dutyDangling.keySet()));
		}
		if (getSize()==0) {
			logger.info("{}: no CRUD duties", getClass().getSimpleName());
		} else {
			logger.info("{}: with {} CRUD duties", getClass().getSimpleName(), getSize());
		}
	}

	public int getSize() {
		return commitRequests.get(EntityEvent.ATTACH).size()
			+ commitRequests.get(EntityEvent.CREATE).size()
			+ commitRequests.get(EntityEvent.REMOVE).size();
	}
	
	public synchronized void addFeature(final ChangeFeature f) {
		clusterFeatures.add(f);
	}
	public Set<ChangeFeature> getFeatures() {
		return clusterFeatures;
	}
	
}