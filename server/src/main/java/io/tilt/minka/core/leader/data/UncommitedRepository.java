package io.tilt.minka.core.leader.data;

import static io.tilt.minka.domain.EntityEvent.CREATE;
import static io.tilt.minka.domain.EntityEvent.REMOVE;
import static io.tilt.minka.domain.EntityState.PREPARED;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
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
				tryCallback(callback, Reply.notFound(remove.getEntity()));
			}
		}

		scheme.getUncommited().addAllCrudDuty(tmp, (duty, added) -> {
			if (added) {
				tryCallback(callback, Reply.success(duty, true));
			} else {
				tryCallback(callback, Reply.alreadySubmitted(duty));
			}
		});
	}
	
	/** duties directly from client */
	public boolean loadRawDuties(final Collection<Duty> raw, final Consumer<Reply> callback) {		
		final Set<ShardEntity> rawSet = raw.stream().map(x-> toEntity(x)).collect(Collectors.toSet());
		final Set<ShardEntity> merged = scheme.getLearningState().merge(rawSet);
		// patch and write previous commited state
		scheme.getLearningState().patchCommitTrees(new HashSet<>(merged), (shard, patch)-> {
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
			if (presentInPartition(duty)) {
				tryCallback(callback, Reply.alreadyExists(duty.getDuty()));
			} else {
				final ShardEntity pallet = scheme.getCommitedState().getPalletById(duty.getDuty().getPalletId());
				if (pallet==null) {
					tryCallback(callback, Reply.inconsistent(duty.getDuty()));
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
				tryCallback(callback, Reply.success(duty, true));
			} else {
				tryCallback(callback, Reply.alreadySubmitted(duty));
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
    			tryCallback(callback, Reply.notFound(p.getPallet()));
    		} else {
    		    final boolean done = scheme.addCrudPallet(pallet);
    		    tryCallback(callback, done ? 
    		    		Reply.success(pallet.getPallet(), true) 
    		    		: Reply.notFound(pallet.getPallet()));
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
    	        tryCallback(callback, Reply.success(p.getEntity(), added));
    		} else {
    			tryCallback(callback, Reply.alreadyExists(p.getEntity()));
    		}
		}
		
	}
	
	private boolean presentInPartition(final ShardEntity duty) {
		final Shard shardLocation = scheme.getCommitedState().findDutyLocation(duty.getDuty());
		final boolean somewhere= shardLocation != null && shardLocation.getState().isAlive();
		if (!somewhere) {
			return scheme.getCurrentPlan().hasMigration(duty.getDuty());
		} else {
			return false;
		}
	}

}
