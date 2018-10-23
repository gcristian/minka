package io.tilt.minka.core.leader.data;

import static java.util.Objects.requireNonNull;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.tilt.minka.api.ConsistencyException;
import io.tilt.minka.api.Duty;
import io.tilt.minka.api.Pallet;
import io.tilt.minka.core.leader.ConcurrentDutyException;
import io.tilt.minka.core.leader.StateSentry;
import io.tilt.minka.domain.EntityEvent;
import io.tilt.minka.domain.ShardEntity;
import io.tilt.minka.domain.ShardedPartition;
import io.tilt.minka.shard.NetworkShardIdentifier;
import io.tilt.minka.shard.Shard;
import io.tilt.minka.shard.ShardCapacity;
import io.tilt.minka.shard.ShardIdentifier;
import io.tilt.minka.shard.ShardState;

/**
 * Representation of the known confirmed status of distribution of duties.
 * Only maintainer: {@linkplain StateSentry}
 */
public class CommittedState {

	private static final Logger logger = LoggerFactory.getLogger(CommittedState.class);

	private final Map<ShardIdentifier, Shard> shardsByID;
	private final Map<Shard, ShardedPartition> partitionsByShard;
	final Map<String, ShardEntity> palletsById;
	private boolean stealthChange;
	
	CommittedState() {
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
			if (test == null || test.test(sh)) {
				consumer.accept(sh);
			}
		}
	}
	public boolean findShardsAnd(final Predicate<Shard> test, final Function<Shard, Boolean> fnc) {
		boolean any = false;
		for (Shard sh: shardsByID.values()) {
			if (test == null || test.test(sh)) {
				any |=fnc.apply(sh);
			}
		}
		return any;
	}

	public Shard findShard(final Predicate<Shard> test) {
		for (Shard sh: shardsByID.values()) {
			if (test == null || test.test(sh)) {
				return sh;
			}
		}
		return null;
	}
	
	public Shard findShard(final String id) {
		return findShard(s->requireNonNull(id).equals(s.getShardID().getId()));
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
	 * after completing Transition's cycles: the shard must be deleted
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
	
	
	public void loadReplicas(final Shard shard, final Set<ShardEntity> replicas) {
		for (ShardEntity replica: replicas) {
			commit(replica, shard, EntityEvent.STOCK);
		}
	}
	
	/**
	 * Account the end of the duty movement operation.
	 * Only access-point to adding and removing duties.
	 * @param duty 		the entity to act on
	 * @param where		the sard where it resides
	 * @param callback	called when writting is possible
	 * @return if there was a CommittedState change after the action 
	 */
	public boolean commit(final ShardEntity duty, final Shard where, final EntityEvent event) {
		final boolean add = event.is(EntityEvent.ATTACH);// || event.is(EntityEvent.CREATE);
		final boolean del = !add && (event.is(EntityEvent.DETACH) || event.is(EntityEvent.REMOVE));
		final ShardedPartition part = getPartition(where);
		boolean impact = false;
		if (add) {
			checkDuplicationFailure(duty, where);
		} 
		if ((add && part.add(duty)) || (del && part.remove(duty))) {
			stealthChange = impact = true; 
		} else if (event.is(EntityEvent.STOCK) && (impact = part.stock(duty))) {
		} else if (event.is(EntityEvent.DROP) && (impact = part.drop(duty))) {
		} else {
			throw new ConsistencyException("Commit failure (" + duty + ") Operation does not apply [" + event + "]");
		}
		return impact;
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
	public Collection<ShardEntity> getReplicasByShard(final Shard shard) {
		return getPartition(shard).getReplicas();
	} 

	public void findDuties(final Shard shard, final Pallet pallet, final Consumer<ShardEntity> consumer) {
		for (ShardEntity e: getPartition(shard).getDuties()) {
			if (pallet==null || e.getDuty().getPalletId().equals(pallet.getId())) {
				consumer.accept(e);
			}
		}
	}
	
	public ShardEntity getByDuty(final Duty duty) {
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
	public void findDutiesByPallet(final Pallet pallet, final Consumer<ShardEntity> consumer) {
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
	
	private void onDuties(final Pallet pallet, 
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
	public Shard findDutyLocation(final String qid) {
		for (final Shard shard : partitionsByShard.keySet()) {
			for (ShardEntity st : partitionsByShard.get(shard).getDuties()) {
				if (st.getQualifiedId().equals(qid)) {
					return shard;
				}
			}
		}
		return null;
	}
	
	public Shard findDutyLocation(final Duty duty) {
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
	
	public void logStatus() {
		if (shardsByID.isEmpty()) {
			logger.warn("{}: Status without Shards", getClass().getSimpleName());
		} else {
			if (!logger.isInfoEnabled()) {
				return;
			}
			for (final Shard shard : shardsByID.values()) {
				final ShardedPartition partition = partitionsByShard.get(shard);
				final Map<Pallet, ShardCapacity> shardCapacities = shard.getCapacities();
				if (partition == null) {
					logger.info("{}: {} = Empty", getClass().getSimpleName(), shard);
				} else {
					for (ShardEntity p: palletsById.values()) {
						final StringBuilder sb = new StringBuilder();
						sb.append(shard).append(" Pallet: ").append(p.getPallet().getId());
						final ShardCapacity cap = shardCapacities.get(p.getPallet());
						sb.append(" Size: ").append(partition.getDutiesSize(p.getPallet()));
						sb.append(" Weight/ShardCapacity: ");
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
		private final CommittedState reference;

		public SchemeExtractor(final CommittedState reference) {
			this.reference = reference;
		}

		public double getCapacity(final Pallet pallet, final Shard quest) {
			double total = 0;
			for (final Shard shard : getShards()) {
				if (quest == null || shard.equals(quest)) {
					final ShardCapacity cap = shard.getCapacities().get(pallet);
					total += cap != null ? cap.getTotal() : 0;
				}
			}
			return total;
		}

		public double getCapacityTotal(final Pallet pallet) {
			return getCapacity(pallet, null);
		}

		public int getSizeTotal() {
			return getSize(null, null);
		}

		public int getSizeTotal(final Pallet pallet) {
			return getSize(pallet, null);
		}

		public int getSize(final Pallet pallet, final Shard quest) {
			int total = 0;
			for (final ShardedPartition part : reference.partitionsByShard.values()) {
				if (quest == null || part.getId().equals(quest.getShardID())) {
					total += (pallet == null ? part.getDuties().size() : part.getDutiesSize(pallet));
				}
			}
			return total;
		}

		public double getWeightTotal(final Pallet pallet) {
			return getWeight(pallet, null);
		}

		public double getWeight(final Pallet pallet, final Shard quest) {
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

		public int getAccountConfirmed(final Pallet filter) {
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