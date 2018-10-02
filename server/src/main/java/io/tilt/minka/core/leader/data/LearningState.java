package io.tilt.minka.core.leader.data;

import static io.tilt.minka.domain.EntityEvent.ATTACH;
import static io.tilt.minka.domain.EntityEvent.STOCK;
import static io.tilt.minka.domain.EntityState.COMMITED;
import static java.util.Arrays.asList;

import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.tilt.minka.api.Pallet;
import io.tilt.minka.core.leader.StateSentry;
import io.tilt.minka.core.leader.distributor.ChangePlan;
import io.tilt.minka.domain.CommitTree;
import io.tilt.minka.domain.EntityEvent;
import io.tilt.minka.domain.EntityRecord;
import io.tilt.minka.domain.EntityState;
import io.tilt.minka.domain.ShardEntity;
import io.tilt.minka.shard.Shard;
import io.tilt.minka.shard.ShardIdentifier;

/**
 * Scenario: the leader initiates and the {@linkplain StateSentry} detects heartbeats comming
 * with entities whose CommitTrees have commits with plan-id smaller than first {@linkplain ChangePlan} id.
 * Meaning: leader is in presence of a previous cluster session with an already distributed state. 
 * When: only before 1st distribution, usage and impact does not goes beyond.
 * Action: save distribution and replication reports, until required by {@linkplain PhaseLoader} phase. 
 */
public class LearningState {

	private static final Logger logger = LoggerFactory.getLogger(LearningState.class);

	private final Map<ShardIdentifier, Set<EntityRecord>> distribution = new HashMap<>();
	private final Map<Shard, Set<ShardEntity>> replicas = new HashMap<>();
	
	/** @return TRUE if has learning contents */
	public boolean isEmpty() {
		return distribution.isEmpty() && replicas.isEmpty(); 
	}

	public Map<Shard, Set<ShardEntity>> getReplicasByShard() {
		return replicas;
	}

	/** guard the report to take it as truth once distribution runs and ShardEntities are loaded */
	public void learn(final Collection<EntityRecord> records, final Shard where) {
		// changePlan is NULL only before 1st distribution
		// there's been a change of leader: i'm initiating with older followers
		final Map<EntityEvent.Type, StringBuilder> logmap = new HashMap<>(2);
		for (EntityRecord record: records) {
			EntityEvent lpv = feedDistro(record, where);
			EntityEvent lpd = null;
			// if it's not a distribution report and it has a bundled entity
			if (lpv==null && record.getEntity() != null) {
				lpd = feedReplica(record.getEntity(), where, true);
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

	private EntityEvent feedDistro(final EntityRecord record, final Shard where) {
		EntityEvent ret = null;
		if (record.getCommitTree().isDurable(where.getShardID().getId(), ATTACH, COMMITED)) {
			if (mustBeAdded(record, where)) {
				Set<EntityRecord> set = distribution.get(where.getShardID());
				if (set==null) {
					distribution.put(where.getShardID(), set = new HashSet<>());
				}
				if (set.add(record)) {
					ret = EntityEvent.ATTACH;
				}
			}
			// Heartbeats come with entity on leader reelection (sign of a replica)
			if (record.getEntity()!=null) {
				feedReplica(record.getEntity(), where, false);
			}
		}
		return ret;
	}

	/**
	 * Check record if already reported in different shards: believe the lattest commit tree.
	 * Remove older ones reported by different shards.
	 * 
	 * @return TRUE on rule: if existed and is younger, or didnt existed.
	 */
	private boolean mustBeAdded(final EntityRecord record, final Shard where) {
		for (Map.Entry<ShardIdentifier, Set<EntityRecord>> e: distribution.entrySet()) {
			// check only different shards added already containing record
			if (!e.getKey().equals(where.getShardID()) && e.getValue().contains(record)) {
				final Iterator<EntityRecord> it = e.getValue().iterator();
				while (it.hasNext()) {
					EntityRecord er = it.next();
					if (er.equals(record)) {
						boolean latest = isLatest_(record, er);
						if (latest) {
							it.remove();
							logger.warn("{}: Shard {} has reported a younger commit-tree on {} and will be used.", 
								getClass().getSimpleName(), where, record);
						}
						// olny if was younger
						return latest;
					}
				}
			}
		}
		// didnt existed
		return true;
	}

	private boolean isLatest_(final EntityRecord record, final EntityRecord already) {	
		final Date ts1 = already.getCommitTree().getLast().getHead();
		final Date ts2 = record.getCommitTree().getLast().getHead();
		return ts1.before(ts2);
	}

	private EntityEvent feedReplica(final ShardEntity duty, final Shard where, final boolean durable) {
		
		if (!durable || duty.getCommitTree().isDurable(where.getShardID().getId(), STOCK, COMMITED)) {			
			Set<ShardEntity> byShard = replicas.get(where);
			if (byShard==null) {
				replicas.put(where, byShard=new HashSet<>());
			}
			// dont replace it
			final boolean existed = byShard.contains(duty);
			if (!existed) {
				final CommitTree fresh = new CommitTree();
				fresh.addEvent(
					EntityEvent.CREATE, 
					EntityState.PREPARED, 
					where.getShardID(), 
					CommitTree.PLAN_NA);
				duty.replaceTree(fresh);
			}
			if (existed || byShard.add(duty)) {
				return EntityEvent.STOCK;
			}
		}
		return null;
	}
	
	/** @return composition of passed raws and learnt, using the youngest on colission */
	Set<ShardEntity> mergeWithYoungest(final Set<ShardEntity> rawSet) {
		final Set<ShardEntity> tmp = new HashSet<>(rawSet);
		// very important: adding to the current leadership 
		// the previous leadership's post-load added duties (CRUD) 
		for (ShardEntity drain: drainReplicas()) {
			boolean overwrite = true; // by default all out of new delegate
			for (ShardEntity exRaw: rawSet) {
				if (exRaw.equals(drain)) {
					// preffer last updated version
					overwrite = exRaw.getCommitTree().getLast().getHead().before(
							drain.getCommitTree().getLast().getHead());
					break;
				}
			}
			if (overwrite) {
				tmp.add(drain);
			}
		}
		return tmp;
	}

	/** @return previous state's domain-commited entities */
	Set<ShardEntity> drainReplicas() {
		final Set<ShardEntity> drained = new HashSet<>(); 
		for (Collection<ShardEntity> c: replicas.values()) {
			drained.addAll(c);
		}
		replicas.clear();
		return drained;
	}
	
	/** @return TRUE if fixed master's duty with previous state */
	boolean patchCommitTrees(
			final Set<ShardEntity> duties, 
			final BiConsumer<ShardIdentifier, ShardEntity> bc) {
		
		if (distribution.isEmpty()) {
			return false;
		}
		
		boolean ret = false;
		final EntityEvent event = EntityEvent.ATTACH;
		for (Map.Entry<ShardIdentifier, Set<EntityRecord>> e: distribution.entrySet()) {
			if (logger.isInfoEnabled()) {
				logger.info("{}: Patching scheme ({}) w/previous commit-trees: {}", getClass().getSimpleName(), 
						event, EntityRecord.toStringIds(e.getValue()));
			}
			boolean found = false;
			for (EntityRecord r: e.getValue()) {
				for (ShardEntity d: duties) {
					if (d.getQualifiedId().equals(r.getQualifiedId())) {
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
		this.distribution.clear();
		return ret;
	}
	
	/** @return pallets taken from replicas (overlapped if any) */
	public Set<Pallet> collectPallets() {
		// TODO get the last updated
		final Set<Pallet> ret = new HashSet<>();
		for (Set<ShardEntity> set: this.replicas.values()) {
			for (ShardEntity e: set) {
				if (e.getRelatedEntity()!=null) {
					ret.add(e.getRelatedEntity().getPallet());
				}
			}
		}
		return ret;
	}
	
	/** @return pallets taken from replicas (overlapped if any) */
	public ShardEntity getPalletFromReplicas(final String pid) {
		// TODO get the last updated
		for (Set<ShardEntity> set: this.replicas.values()) {
			for (ShardEntity e: set) {
				if (e.getRelatedEntity()!=null && e.getDuty().getPalletId().equals(pid)) {
					return e.getRelatedEntity();
				}
			}
		}
		return null;
	}

	
}