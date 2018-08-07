package io.tilt.minka.core.leader.data;

import static java.util.Arrays.asList;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
 * Action: save distribution and replication reports, until required by {@linkplain Distributor} phase. 
 */
public class LearningState {

	private static final Logger logger = LoggerFactory.getLogger(LearningState.class);

	private final Map<ShardIdentifier, Set<EntityRecord>> distribution = new HashMap<>();
	private final Map<Shard, Set<ShardEntity>> replicas = new HashMap<>();
	
	/** guard the report to take it as truth once distribution runs and ShardEntities are loaded */
	public void learn(final Collection<EntityRecord> records, final Shard where) {
		// changePlan is NULL only before 1st distribution
		// there's been a change of leader: i'm initiating with older followers
		final Map<EntityEvent.Type, StringBuilder> logmap = new HashMap<>(2);
		for (EntityRecord record: records) {
			EntityEvent lpv = feedDistro(record, where);
			EntityEvent lpd = null;
			if (record.getEntity() != null) {
				lpd = feedReplica(record.getEntity(), where, false);
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
		// TODO add another constraint to avoid blindly trusting the shard
		// check the log is really the last (another shard could say the same, who to trust ?)
		// the beat comes from A, and there's an Attach on A: pretty confident...could be a lattest Attach on B
		// and another beat from B saying the same... and actually being the real trustworthy
		if (record.getCommitTree().exists(where.getShardID().getId(), EntityEvent.ATTACH)) {
			Set<EntityRecord> set = distribution.get(where.getShardID());
			if (set==null) {
				distribution.put(where.getShardID(), set = new HashSet<>());
			}
			if (record.getEntity()!=null) {
				feedReplica(record.getEntity(), where, false);
			}
			if (set.add(record)) {
				return EntityEvent.ATTACH;
			}
		}
		return null;
	}

	private EntityEvent feedReplica(final ShardEntity duty, final Shard where, final boolean stockCheck) {
		if (!stockCheck || duty.getCommitTree().exists(where.getShardID().getId(), EntityEvent.STOCK)) {
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
					0);
				duty.replaceTree(fresh);
			}
			if (existed || byShard.add(duty)) {
				return EntityEvent.STOCK;
			}
		}
		return null;
	}
	
	public Map<Shard, Set<ShardEntity>> getReplicasByShard() {
		return replicas;
	}

	Set<ShardEntity> merge(final Set<ShardEntity> rawSet) {
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
		boolean ret = false;
		if (!distribution.isEmpty()) {
			final EntityEvent event = EntityEvent.ATTACH;
			for (Map.Entry<ShardIdentifier, Set<EntityRecord>> e: distribution.entrySet()) {
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
			this.distribution.clear();
		}
		return ret;
	}
	
}