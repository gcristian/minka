package io.tilt.minka.core.leader.data;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Predicate;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.tilt.minka.api.ConsistencyException;
import io.tilt.minka.api.Duty;
import io.tilt.minka.api.Pallet;
import io.tilt.minka.core.leader.ConcurrentDutyException;
import io.tilt.minka.core.leader.SchemeSentry;
import io.tilt.minka.domain.Capacity;
import io.tilt.minka.domain.EntityEvent;
import io.tilt.minka.domain.NetworkShardIdentifier;
import io.tilt.minka.domain.Shard;
import io.tilt.minka.domain.Shard.Change;
import io.tilt.minka.domain.Shard.ShardState;
import io.tilt.minka.domain.ShardEntity;
import io.tilt.minka.domain.ShardIdentifier;
import io.tilt.minka.domain.ShardReport;
import io.tilt.minka.domain.ShardedPartition;
import io.tilt.minka.utils.CollectionUtils;
import io.tilt.minka.utils.CollectionUtils.SlidingSortedSet;

/**
 * Repr. of distribution scheme after proper confirmation of the followers 
 * Holds the sharding registry of duties.
 * Only maintainer: {@linkplain SchemeSentry}
 */
public class Scheme {

	private static final Logger logger = LoggerFactory.getLogger(Scheme.class);

	private final Map<ShardIdentifier, Set<ShardReport>> previousScheme = new HashMap<>();
	private final Map<ShardIdentifier, Shard> shardsByID;
	private final Map<Shard, ShardedPartition> partitionsByShard;
	final Map<String, ShardEntity> palletsById;
	private final Map<ShardIdentifier, SlidingSortedSet<Shard.Change>> goneShards;
	private boolean stealthChange;
	
	public Scheme() {
		this.goneShards = new HashMap<>();
		this.shardsByID = new HashMap<>();
		this.partitionsByShard = new HashMap<>();
		this.palletsById = new HashMap<>();
	}
	
	public void stealthChange(final boolean value) {
		this.stealthChange = value;
	}
	/** @return true when the scheme has changes worthy of distribution phase run  */
	public boolean isStealthChange() {
		return this.stealthChange;
	}
	
	public void findShards(final Predicate<Shard> test, final Consumer<Shard> consumer) {
		for (Shard sh: shardsByID.values()) {
			if (test == null || test.test(sh) || test == null) {
				consumer.accept(sh);
			}
		}
	}
	public Shard findShard(final Predicate<Shard> test) {
		for (Shard sh: shardsByID.values()) {
			if (test == null || test.test(sh) || test == null) {
				return sh;
			}
		}
		return null;
	}

	/** @return true if any shard passed the predicate */
	public boolean filterShards(final Predicate<Shard> test) {
		return shardsByID.values().stream().anyMatch(test);
	}
	
	public int shardsSize(final Predicate<Shard> test) {
		return test == null
				? shardsByID.size() 
				: (int)shardsByID.values().stream().filter(test).count();
	}

	public ShardEntity getPalletById(final String id) {
		return this.palletsById.get(id);
	}
	public void findPallets(final Consumer<ShardEntity> consumer) {
		palletsById.values().forEach(consumer);
	}
	
	/**
	 * When a Shard has been offline and their dangling duties reassigned
	 * after completing Change's cycles: the shard must be deleted
	 * @param shard	a shard to delete from cluster
	 * @return true if the action was performed
	 */
	public boolean removeShard(final Shard shard) {
		logger.info("{}: Removing Shard {} - bye bye ! come back some day !", getClass().getSimpleName(), shard);
		final Shard rem = this.shardsByID.remove(shard.getShardID());
		final ShardedPartition part = this.partitionsByShard.remove(shard);
		if (rem == null || part == null) {
			logger.error("{}: trying to delete unexisting Shard: {}", getClass().getSimpleName(), shard);
		}
		final boolean changed = rem!=null && part!=null;
		addGoneShard(shard);
		stealthChange |= changed;
		return changed;
	}
	/**
	 * @param shard	to add to the table
	 * @return true if the action was performed
	 */
	public boolean addShard(final Shard shard) {
		if (this.shardsByID.containsKey(shard.getShardID())) {
			logger.error("{}: Inconsistency trying to add an already added Shard {}", getClass().getSimpleName(),
					shard);
			return false;
		} else {
			logger.info("{}: Adding new Shard {}", getClass().getSimpleName(), shard);
			this.shardsByID.put(shard.getShardID(), shard);
			stealthChange = true;
			return true;
		}
	}
	
	/** backup gone shards for history */
	private void addGoneShard(final Shard shard) {
		int max = 5;
		for (final Iterator<Change> it = shard.getChanges().iterator(); it.hasNext() && max>0;max--) {
			SlidingSortedSet<Change> set = goneShards.get(shard.getShardID());
			if (set==null) {
				goneShards.put(shard.getShardID(), set = CollectionUtils.sliding(max));
			}
			set.add(it.next());
		}
	}
	
	public void patchOnPreviousDistribution(final Set<ShardEntity> duties) {
		if (!previousScheme.isEmpty()) {
			final EntityEvent event = EntityEvent.ATTACH;
			for (Map.Entry<ShardIdentifier, Set<ShardReport>> e: previousScheme.entrySet()) {
				boolean found = false;
				if (logger.isInfoEnabled()) {
					logger.info("{}: Patching scheme ({}) w/prev. distribution journals: {}", getClass().getSimpleName(), 
							event, ShardReport.toStringIds(e.getValue()));
				}
				for (ShardReport r: e.getValue()) {
					for (ShardEntity d: duties) {
						if (d.getDuty().getId().equals(r.getId())) {
							found = true;
							d.replaceJournal(r.getJournal());
							write(d, findShard(s->s.getShardID().equals(e.getKey())), event, null);
							break;
						}
					}
					if (!found) {
						logger.error("{}: Shard {} reported an unloaded duty from previous distribution: {}", 
								getClass().getSimpleName(), e.getKey(), r.getId());
					}
				}
			}
			this.previousScheme.clear();
		}
	}
	
	/** guard the report to take it as truth once distribution runs and ShardEntity is loaded */
	public boolean learnPreviousDistribution(final ShardReport duty, final Shard where) {
		boolean ret = false;
		Set<ShardReport> list = previousScheme.get(where.getShardID());
		if (list==null) {
			previousScheme.put(where.getShardID(), list = new HashSet<>());
		}
		
		if (ret = list.add(duty)) {
			stealthChange = true; 
		}
		return ret;
	}
	/**
	 * Account the end of the duty movement operation.
	 * Only access-point to adding and removing duties.
	 * @param duty 		the entity to act on
	 * @param where		the sard where it resides
	 * @param callback	called when writting is possible
	 * @return if there was a Scheme change after the action 
	 */
	public boolean write(final ShardEntity duty, final Shard where, final EntityEvent event, final Runnable callback) {
		final boolean add = event.is(EntityEvent.ATTACH) || event.is(EntityEvent.CREATE);
		final boolean del = !add && (event.is(EntityEvent.DETACH) || event.is(EntityEvent.REMOVE));
		final ShardedPartition part = getPartition(where);
		if (add) {
			checkDuplicationFailure(duty, where);
		} 
		if ((add && part.add(duty)) || (del && part.remove(duty))) {
			stealthChange = true; 
			if (callback!=null) {
				callback.run();
			}
		} else {
			throw new ConsistencyException("Attach failure. Confirmed attach/creation already exists");
		}
		return true;
	}
	
	private void checkDuplicationFailure(final ShardEntity duty, final Shard reporter) {
		for (Shard sh : partitionsByShard.keySet()) {
			if (!sh.equals(reporter) && partitionsByShard.get(sh).contains(duty)) {
				throw new ConcurrentDutyException("Duplication failure: Shard %s tries to Report Duty: %s already "
						+ "in ptable's Shard: %s", reporter, duty, sh);
			}
		}
	}
	/** 
	 * @param shard	the shard to get duties from
	 * @return a copy of duties by shard: dont change duties ! */
	public Collection<ShardEntity> getDutiesByShard(final Shard shard) {
		return getPartition(shard).getDuties();
	}

	public void findDuties(final Shard shard, final Pallet<?> pallet, final Consumer<ShardEntity> consumer) {
		for (ShardEntity e: getPartition(shard).getDuties()) {
			if (pallet==null || e.getDuty().getPalletId().equals(pallet.getId())) {
				consumer.accept(e);
			}
		}
	}
	
	public ShardEntity getByDuty(final Duty<?> duty) {
		ShardEntity ret = null;
		for (final Shard shard : partitionsByShard.keySet()) {
			ret = partitionsByShard.get(shard).getByDuty(duty);
			if (ret!=null) {
				break;
			}
		}
		return ret;
	}
	
	public void findDuties(final Consumer<ShardEntity> consumer) {
		onDuties(null, null, consumer, null, false);
	}
	public void findDutiesByPallet(final Pallet<?> pallet, final Consumer<ShardEntity> consumer) {
		onDuties(pallet, null, consumer, null, false);
	}
	public boolean dutyExists(final ShardEntity e) {
		final boolean ret[] = new boolean[1];
		onDuties(null, null, ee-> ret[0]=true, ee-> ee.equals(e), true);
		return ret[0];
	}
	
	public boolean dutyExistsAt(final ShardEntity e, final Shard shard) {
		return getPartition(shard).getDuties().contains(e);
	}
	
	private void onDuties(final Pallet<?> pallet, 
			final ShardState state, 
			final Consumer<ShardEntity> consumer, 
			final Predicate<ShardEntity> test, 
			final boolean one) {
		for (Shard shard: shardsByID.values()) {
			if (state==null || shard.getState()==state) {
				for (final ShardEntity e: getPartition(shard).getDuties()) {
					if (pallet == null || e.getDuty().getPalletId().equals(pallet.getId())) {
						if (test==null || test.test(e)) {
							consumer.accept(e);
							if (one) {
								return;
							}
						}
					}
				}	
			}
		}
	}
	
	private synchronized ShardedPartition getPartition(final Shard shard) {
		ShardedPartition po = this.partitionsByShard.get(shard);
		if (po == null) {
			this.partitionsByShard.put(shard, po = ShardedPartition.partitionForFollower(shard.getShardID()));
		}
		return po;
	}

	public Shard getShard(NetworkShardIdentifier id) {
		return shardsByID.get(id);
	}

	public Shard findDutyLocation(final ShardEntity se) {
		for (final Shard shard : partitionsByShard.keySet()) {
			for (ShardEntity st : partitionsByShard.get(shard).getDuties()) {
				if (st.equals(se)) {
					return shard;
				}
			}
		}
		return null;
	}
	public Shard findDutyLocation(final String dutyId) {
		for (final Shard shard : partitionsByShard.keySet()) {
			for (ShardEntity st : partitionsByShard.get(shard).getDuties()) {
				if (st.getEntity().getId().equals(dutyId)) {
					return shard;
				}
			}
		}
		return null;
	}
	
	public Shard findDutyLocation(final Duty<?> duty) {
		for (final Shard shard : partitionsByShard.keySet()) {
			final ShardEntity st = partitionsByShard.get(shard).getByDuty(duty);
			if (st!=null) {
				return shard;
			}
		}
		return null;
	}

	public void filterPalletLocations(final ShardEntity pallet, final Consumer<Shard> consumer) {
		for (final Shard shard : partitionsByShard.keySet()) {
			for (ShardEntity pp : partitionsByShard.get(shard).getPallets()) {
				if (pp.equals(pallet)) {
					consumer.accept(shard);
					break;
				}
			}
		}
	}
	
	public int shardsSize() {
		return this.shardsByID.values().size();
	}

	public Map<ShardIdentifier, SlidingSortedSet<Shard.Change>> getGoneShards() {
		return goneShards;
	}
	
	public void logStatus() {
		if (shardsByID.isEmpty()) {
			logger.warn("{}: Status without Shards", getClass().getSimpleName());
		} else {
			if (!logger.isInfoEnabled()) {
				return;
			}
			for (final Shard shard : shardsByID.values()) {
				final ShardedPartition partition = partitionsByShard.get(shard);
				final Map<Pallet<?>, Capacity> capacities = shard.getCapacities();
				if (partition == null) {
					logger.info("{}: {} = Empty", getClass().getSimpleName(), shard);
				} else {
					for (ShardEntity p: palletsById.values()) {
						final StringBuilder sb = new StringBuilder();
						sb.append(shard).append(" Pallet: ").append(p.getPallet().getId());
						final Capacity cap = capacities.get(p.getPallet());
						sb.append(" Size: ").append(partition.getDutiesSize(p.getPallet()));
						sb.append(" Weight/Capacity: ");
						final double[] weight = new double[1];
						partition.getDuties().stream()
							.filter(d->d.getDuty().getPalletId().equals(p.getPallet().getId()))
							.forEach(d->weight[0]+=d.getDuty().getWeight());
						sb.append(weight[0]).append("/");
						sb.append(cap!=null ? cap.getTotal(): "Unreported");
						if (logger.isDebugEnabled()) {
							sb.append(" Duties: [").append(partition.toString()).append("]");
						}
						logger.info("{}: {}", getClass().getSimpleName(), sb.toString());
					}
				}
			}
		}

	}

	/** Read-only access */
	public static class SchemeExtractor {
		private final Scheme reference;

		public SchemeExtractor(final Scheme reference) {
			this.reference = reference;
		}

		public double getCapacity(final Pallet<?> pallet, final Shard quest) {
			double total = 0;
			for (final Shard shard : getShards()) {
				if (quest == null || shard.equals(quest)) {
					final Capacity cap = shard.getCapacities().get(pallet);
					total += cap != null ? cap.getTotal() : 0;
				}
			}
			return total;
		}

		public double getCapacityTotal(final Pallet<?> pallet) {
			return getCapacity(pallet, null);
		}

		public int getSizeTotal() {
			return getSize(null, null);
		}

		public int getSizeTotal(final Pallet<?> pallet) {
			return getSize(pallet, null);
		}

		public int getSize(final Pallet<?> pallet, final Shard quest) {
			int total = 0;
			for (final ShardedPartition part : reference.partitionsByShard.values()) {
				if (quest == null || part.getId().equals(quest.getShardID())) {
					total += (pallet == null ? part.getDuties().size() : part.getDutiesSize(pallet));
				}
			}
			return total;
		}

		public double getWeightTotal(final Pallet<?> pallet) {
			return getWeight(pallet, null);
		}

		public double getWeight(final Pallet<?> pallet, final Shard quest) {
			int total = 0;
			for (final ShardedPartition part : reference.partitionsByShard.values()) {
				if (quest == null || part.getId().equals(quest.getShardID())) {
					total += part.getWeight(pallet);
				}
			}
			return total;
		}

		public Collection<ShardEntity> getPallets() {
			return reference.palletsById.values();
		}

		public Collection<Shard> getShards() {
			return reference.shardsByID.values();
		}

		public int getAccountConfirmed(final Pallet<?> filter) {
			int total = 0;
			for (Shard shard : reference.partitionsByShard.keySet()) {
				if (shard.getState() == ShardState.ONLINE) {
					total += reference.partitionsByShard.get(shard).getDutiesSize(filter);
				}
			}
			return total;
		}
		
	}

}