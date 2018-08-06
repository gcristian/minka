package io.tilt.minka.core.leader.data;

import static io.tilt.minka.domain.EntityEvent.CREATE;
import static io.tilt.minka.domain.EntityEvent.REMOVE;
import static io.tilt.minka.domain.EntityState.PREPARED;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.tilt.minka.api.Duty;
import io.tilt.minka.api.Entity;
import io.tilt.minka.api.Pallet;
import io.tilt.minka.api.Reply;
import io.tilt.minka.api.ReplyValue;
import io.tilt.minka.core.leader.distributor.ChangePlan;
import io.tilt.minka.domain.EntityEvent;
import io.tilt.minka.domain.CommitTree.Log;
import io.tilt.minka.shard.Shard;
import io.tilt.minka.shard.ShardIdentifier;
import io.tilt.minka.domain.EntityState;
import io.tilt.minka.domain.ShardEntity;

/**
 * Entry point for outter clients of the {@linkplain UncommitedChanges}
 * Validations and consistency considerations for {@linkplain Client} usage
 */
public class UncommitedRepository {

	private final Logger logger = LoggerFactory.getLogger(getClass());
	private final String classname = getClass().getSimpleName();

	private final Scheme scheme;
	private final ShardIdentifier shardId;

	public UncommitedRepository(final Scheme scheme, final ShardIdentifier shardId) {
		super();
		this.scheme = scheme;
		this.shardId = shardId;
	}

	public Collection<Duty> getDuties() {
		final List<Duty> tmp = new LinkedList<>();
		this.scheme.getCommitedState().findDuties(d->tmp.add(d.getDuty()));
		return tmp;
	}

	public Collection<Pallet> getPallets() {
		final List<Pallet> tmp = new LinkedList<>();
		this.scheme.getCommitedState().findDuties(d->tmp.add(d.getPallet()));
		return tmp;
	}

	////////////////////////// DUTIES

	public void removeAllDuties(final Collection<ShardEntity> coll, final Consumer<Reply> callback) {
		final List<ShardEntity> tmp = new ArrayList<>(coll.size());
		for (ShardEntity remove : coll) {
			final ShardEntity current = scheme.getCommitedState().getByDuty(remove.getDuty());
			if (current != null) {
				tmp.add(remove);
			} else {
				tryCallback(callback, new Reply(ReplyValue.ERROR_ENTITY_NOT_FOUND, remove.getDuty(), null, null,
						String.format("%s: Deletion request not found on scheme: %s", classname, remove.getDuty())));
			}
		}

		scheme.getUncommited().addAllCrudDuty(tmp, (duty, added) -> {
			if (added) {
				tryCallback(callback, new Reply(ReplyValue.SUCCESS, duty, PREPARED, REMOVE, null));
			} else {
				tryCallback(callback, new Reply(ReplyValue.SUCCESS_OPERATION_ALREADY_SUBMITTED, duty, null,
						EntityEvent.REMOVE, String.format("%s: Added already !: %s", classname, duty)));
			}
		});
	}

	/** duties directly from client */
	public boolean loadRawDuties(final Collection<Duty> raw, final Consumer<Reply> callback) {		
		final Set<ShardEntity> rawSet = raw.stream().map(x-> toEntity(x)).collect(Collectors.toSet());
		final Set<ShardEntity> merged = mergeWithLearningDomain(rawSet);
		// patch and write previous commited state
		sharding.getUncommited().patchCommitTreesWithLearningDistro(new HashSet<>(merged), (shard, patch)-> {
			merged.remove(patch);
			// directly commit them as THE true reality			
			scheme.getCommitedState().commit(patch, 
					scheme.getCommitedState().findShard(sid->sid.getShardID().equals(shard)), 
					EntityEvent.ATTACH, 
					null);
		});
		
		if (!merged.isEmpty()) {
			saveAllDuties(merged, callback);
		}
		return !merged.isEmpty();
	}

	public void loadReplicas() {
		for (final Map.Entry<Shard, Set<ShardEntity>> e: 
			scheme.getLearningState().getReplicasByShard().entrySet()) {
			for (ShardEntity replica: e.getValue()) {
				scheme.getCommitedState().commit(replica, e.getKey(), EntityEvent.STOCK, null);
			}
		}
	}

	private ShardEntity toEntity(final Entity e) {
		final ShardEntity.Builder builder = ShardEntity.Builder.builder(e);
		if (e instanceof Duty) {
			ShardEntity pallet = scheme.getCommitedState().getPalletById(((Duty)e).getPalletId());
			builder.withRelatedEntity(pallet);
		}
		final ShardEntity entity = builder.build();
		entity.getCommitTree().addEvent(
				EntityEvent.CREATE, 
				EntityState.PREPARED, 
				this.shardId,
				ChangePlan.PLAN_WITHOUT);
		return entity;
	}
	
	private void tryCallback(final Consumer<Reply> callback, final Reply reply) {
		try {
			callback.accept(reply);
		} catch (Exception e) {
			logger.warn("{}: reply consumer throwed exception: ", classname, e.getMessage());
		}
	}
		
	public void saveAllDuties(final Collection<ShardEntity> coll, final Consumer<Reply> callback) {
		final List<ShardEntity> tmp = new ArrayList<>(coll.size());
		for (final ShardEntity duty: coll) {
			Reply ret = null;
			if (presentInPartition(duty)) {
				ret = new Reply(ReplyValue.ERROR_ENTITY_ALREADY_EXISTS, duty.getDuty(), null, null, null);
				tryCallback(callback, ret);
			} else {
				final ShardEntity pallet = scheme.getCommitedState().getPalletById(duty.getDuty().getPalletId());
				if (pallet==null) {
					tryCallback(callback, new Reply(ReplyValue.ERROR_ENTITY_INCONSISTENT, duty.getDuty(), null, null, 
							String.format("%s: Skipping Crud Event %s: Pallet ID :%s set not found or yet created", classname,
								EntityEvent.CREATE, null, duty.getDuty().getPalletId())));
				} else {
					final ShardEntity newone = ShardEntity.Builder
							.builder(duty.getDuty())
							.withRelatedEntity(pallet)
							.build();
					// decorate with origin source (only for tracking purposes)
					copyOrigin(duty, newone);
					newone.getCommitTree().addEvent(
							EntityEvent.CREATE, 
							EntityState.PREPARED, 
							this.shardId,
							ChangePlan.PLAN_WITHOUT);
					tmp.add(newone);					
				}
			}
		}
		
		final StringBuilder sb = new StringBuilder(tmp.size() * 5+1);
		scheme.getUncommited().addAllCrudDuty(tmp, (duty, added)-> {
			if (added) {
				if (logger.isInfoEnabled()) {
					sb.append(duty).append(',');
				}
				tryCallback(callback, new Reply(ReplyValue.SUCCESS, duty, PREPARED, CREATE, null));
			} else {
				tryCallback(callback, new Reply(ReplyValue.SUCCESS_OPERATION_ALREADY_SUBMITTED, duty, null, 
						EntityEvent.CREATE, String.format("%s: Added already !: %s", classname, duty)));
			}
		});
		if (sb.length()>0) {
			logger.info("{}: Added New Duties: {}", classname, sb.toString());
		}
	}

	private void copyOrigin(ShardEntity source, final ShardEntity target) {
		final Log origin = source.getCommitTree().getLast();
		final ShardIdentifier originId = fromShardId(origin.getTargetId());
		if (originId!=null) {
			target.getCommitTree().addEvent(
					origin.getEvent(), 
					origin.getLastState(), 
					originId, 
					origin.getPlanId());
		} else {
			logger.error("{}: Origin Shard sender of duties not found: {}", classname, origin.getTargetId());
		}
	}

	private ShardIdentifier fromShardId(final String id) {
		final ShardIdentifier[] ret = {null};
		scheme.getCommitedState().findShards(null, s->{
			if (s.getShardID().getId().equals(id)) {
				ret[0] = s.getShardID();
			}
		});
		return ret[0];
	}

	////////////////////////// PALLETS
	
	public void removeAllPallet(final Collection<ShardEntity> coll, final Consumer<Reply> callback) {
	    for (ShardEntity pallet: coll) {
	        final ShardEntity p = scheme.getCommitedState().getPalletById(pallet.getEntity().getId());
    		if (p==null) {
    			tryCallback(callback, new Reply(ReplyValue.ERROR_ENTITY_NOT_FOUND, pallet.getEntity(), null, null, 
    					String.format("%s: Skipping remove not found in CommitedState: %s", 
    							getClass().getSimpleName(), pallet.getEntity().getId())));
    		} else {
    		    final boolean done = scheme.addCrudPallet(pallet);
    		    tryCallback(callback, new Reply(done ? ReplyValue.SUCCESS : ReplyValue.ERROR_ENTITY_NOT_FOUND, 
                    pallet.getEntity(), PREPARED, REMOVE, null));
    		}
	    }
	}
	
	/** pallets directly from client */
	public void loadRawPallets(final Collection<Pallet> coll, final Consumer<Reply> callback) {
		saveAllPallets(coll.stream().map(x-> toEntity(x)).collect(Collectors.toList()), callback);
	}
	public void saveAllPallets(final Collection<ShardEntity> coll, final Consumer<Reply> callback) {
		for (ShardEntity p: coll) {
    		final ShardEntity already = scheme.getCommitedState().getPalletById(p.getEntity().getId());
    		if (already==null) {
    	        if (logger.isInfoEnabled()) {
    	            logger.info("{}: Adding New Pallets: {} with Balancer: {}", classname, p.getPallet(), 
    	                p.getPallet().getMetadata());
    	        }
    	        final boolean added = scheme.addCrudPallet(p);
    	        tryCallback(callback, new Reply(added ? ReplyValue.SUCCESS : ReplyValue.SUCCESS_OPERATION_ALREADY_SUBMITTED, 
                        p.getEntity(), null, EntityEvent.CREATE, 
                        String.format("%s: Added %s: %s", classname, added ? "": "already", p.getPallet())));
    		} else {
    			tryCallback(callback, new Reply(ReplyValue.ERROR_ENTITY_ALREADY_EXISTS, p.getEntity(), null, EntityEvent.CREATE, 
                        String.format("%s: Skipping creation already in CommitedState: %s", 
                                getClass().getSimpleName(), p.getEntity().getId())));
    		}
		}
		
	}
	
	private boolean presentInPartition(final ShardEntity duty) {
		final Shard shardLocation = scheme.getCommitedState().findDutyLocation(duty.getDuty());
		return shardLocation != null && shardLocation.getState().isAlive();
	}

}
