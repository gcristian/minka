package io.tilt.minka.core.leader.data;

import static java.util.Collections.synchronizedMap;

import java.time.Instant;
import java.util.Arrays;
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
	
	private final Map<EntityState, Map<Duty, ShardEntity>> problems = synchronizedMap(new HashMap() {{
		put(EntityState.DANGLING, synchronizedMap(new HashMap<>()));
		put(EntityState.MISSING, synchronizedMap(new HashMap<>()));
		put(EntityState.STUCK, synchronizedMap(new HashMap<>()));
	}});
	
	// creations and removes willing to be attached or detached to/from shards.
	private final Map<Pallet, ShardEntity> palletCrud = synchronizedMap(new HashMap<>());
		
	// read-only snapshot for ChangePlanBuilder thread (not to be modified, stage remains MASTER)
	private DirtyState snapshot;
	private DirtyState running;
	private Instant snaptake;
	private boolean snap = false;
	
	private Instant lastStealthChange;
	private boolean stealthChange;
	
	/* only for featuring traceability*/
	private boolean limitedPromotion;
	private Set<ChangeFeature> clusterFeatures = new HashSet<>(2); 
		
	/** @return a frozen state of stage, so message-events threads 
	 * (threadpool-size independently) can still modify the instance for further change plans */
	public synchronized DirtyState snapshot() {
		checkNotOnSnap();
		if (snapshot==null) {
			final DirtyState snap = new DirtyState();
			snap.snaptake = Instant.now();
			// copy CRs to a new limited structure
			for (Map.Entry<EntityEvent, Map<Duty, CommitRequest>> e: commitRequests.entrySet()) {
				snap.commitRequests.put(e.getKey(), limited(snap, e.getValue()));				
			}
			snap.clusterFeatures.addAll(clusterFeatures);
			clusterFeatures.clear();
			snap.problems.put(EntityState.DANGLING, new HashMap<>(problems.get(EntityState.DANGLING)));
			snap.problems.put(EntityState.MISSING, new HashMap<>(problems.get(EntityState.MISSING)));
			snap.palletCrud.putAll(this.palletCrud);
			snap.snap = true;
			this.snapshot = snap;
		}
		return snapshot;
	}

	/** 
	 * @return new battery of CRs limitted to max after 
	 * removing them from current version for next read 
	 */
	private Map<Duty, CommitRequest> limited(final DirtyState tmp, final Map<Duty, CommitRequest> value) {
		if (value.size() > MAX_CR_PROMOTION_SIZE) {
			final Map<Duty, CommitRequest> newversion = value.entrySet().stream()
				.limit(MAX_CR_PROMOTION_SIZE)
				.collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, 
						(x,y)->x, LinkedHashMap::new));
			tmp.limitedPromotion = true;
			// remove current from next plan
			value.keySet().removeAll(newversion.keySet());
			logger.warn("{}: Limiting CommitRequests promotion ({} left to next plan)",
					getClass().getSimpleName(), value.size());
			return newversion;
		} else {
			return value;
		}
	}
	
	private void checkNotOnSnap() {
		if (snap) {
			throw new IllegalStateException("already a snapshot - bad usage !");
		}
	}
	/** 
	 * move snapshot ref. to running version until new change plan and clean snapshot reference
	 * required when activates limited promotions, so they have CR states to be found & notified  
	 */
	public void dropSnapshotToRunning() {
		checkNotOnSnap();
		this.running = this.snapshot;
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
	public Collection<ShardEntity> getDisturbance(final EntityState es) {
		return Collections.unmodifiableCollection(this.problems.get(es).values());
	}

	/** @return true if added for the first time else is being replaced */
	public boolean addDisturbance(final EntityState type, final Collection<ShardEntity> problems) {
		boolean added = false; 
		for (ShardEntity d: problems) {
			added |= this.problems.get(type).put(d.getDuty(), d) == null;
		}
		turnOnStealth(added);
		return added;
	}
	
	public boolean addDisturbance(final EntityState type, final ShardEntity problem) {
		final boolean added = this.problems.get(type).put(problem.getDuty(), problem) == null;
		turnOnStealth(added);
		return added;

	}
	public void cleanAllocatedDisturbance(final EntityState type, final Predicate<ShardEntity> test) {
		checkNotOnSnap();
		if (snapshot!=null && !problems.get(type).isEmpty()) {
			final Map<Duty, ShardEntity> deletes = snapshot.problems.get(type);
			final Predicate<ShardEntity> p = s->s.getLastState() != EntityState.STUCK 
					&& test==null || (test!=null && test.test(s));
			remove(deletes, problems.get(type), p);
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
		for (EntityState type: Arrays.asList(EntityState.DANGLING, EntityState.MISSING)) {
			if (!problems.get(type).isEmpty()) {
				logger.warn("{}: {} {} duties: [ {}]", getClass().getSimpleName(), problems.get(type).size(),
					type, ShardEntity.toDutyStringIds(problems.get(type).keySet()));
			}
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
	public DirtyState getRunning() {
		return running;
	}
	
}