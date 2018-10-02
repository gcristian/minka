package io.tilt.minka.core.leader.data;

import static java.lang.String.format;

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

import io.tilt.minka.api.Client;
import io.tilt.minka.api.Duty;
import io.tilt.minka.api.Entity;
import io.tilt.minka.api.Pallet;
import io.tilt.minka.api.Reply;
import io.tilt.minka.domain.CommitTree;
import io.tilt.minka.domain.CommitTree.Log;
import io.tilt.minka.domain.EntityEvent;
import io.tilt.minka.domain.EntityState;
import io.tilt.minka.domain.ShardEntity;
import io.tilt.minka.shard.Shard;
import io.tilt.minka.shard.ShardIdentifier;

/**
 * Entry point for outter clients of the {@linkplain DirtyState}
 * Validations and consistency considerations for {@linkplain Client} usage
 */
public class CrudController {

	private static final Logger logger = LoggerFactory.getLogger(CrudController.class);
	private final String classname = getClass().getSimpleName();

	private final Scheme scheme;
	private final ShardIdentifier shardId;

	public CrudController(final Scheme scheme, final ShardIdentifier shardId) {
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

	public void removeAllDuties(final Collection<ShardEntity> coll, final Consumer<Reply> callback, final boolean respondState) {
		final List<ShardEntity> tmp = new ArrayList<>(coll.size());
		for (ShardEntity remove : coll) {
			final ShardEntity current = scheme.getCommitedState().getByDuty(remove.getDuty());
			if (current != null || presentInPartition(remove)) {
				tmp.add(remove);
			} else {
				respond(callback, Reply.notFound(remove.getEntity()));
			}
		}

		scheme.getDirty().createCommitRequests(EntityEvent.REMOVE, tmp, (duty, added) -> {
			if (added) {
				respond(callback, Reply.success(duty, true));
			} else {
				respond(callback, Reply.alreadySubmitted(duty));
			}
		}, respondState);
	}
	
	/** duties directly from client */
	public boolean loadRawDuties(final Collection<Duty> raw, final Consumer<Reply> callback) {		
		final Set<ShardEntity> rawSet = raw.stream().map(x-> toEntity(x)).collect(Collectors.toSet());
		final Set<ShardEntity> merged = scheme.getLearningState().mergeWithYoungest(rawSet);
		// patch and write previous commited state
		scheme.getLearningState().patchCommitTrees(new HashSet<>(merged), (shard, patch)-> {
			// directly commit them as THE true reality			
			final Shard found = scheme.getCommitedState().findShard(sid->sid.getShardID().equals(shard));
			if (found!=null) {
				scheme.getCommitedState().commit(patch, found, EntityEvent.ATTACH);
				merged.remove(patch);
			} else {
				logger.warn("{}: Duty ({}) from unknown shard: {} cannot take CT patch", patch, shard);
			}
		});
		
		if (!merged.isEmpty()) {
			saveAllDuties(merged, callback, false);
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
				CommitTree.PLAN_NA);
		return entity;
	}
	
	private static void respond(final Consumer<Reply> callback, final Reply reply) {
		try {
			callback.accept(reply);
		} catch (Exception e) {
			logger.warn("{}: reply consumer throwed exception: ", CrudController.class.getSimpleName(), e.getMessage());
		}
	}
	
	public void saveAllDuties(final Collection<ShardEntity> coll, final Consumer<Reply> callback, final boolean respondState) {
		final List<ShardEntity> tmp = new ArrayList<>(coll.size());
		for (final ShardEntity duty: coll) {
			if (presentInPartition(duty)) {
				respond(callback, Reply.alreadyExists(duty.getDuty()));
			} else {
				final String pid = duty.getDuty().getPalletId();
				ShardEntity pallet = scheme.getCommitedState().getPalletById(pid);
				if (pallet==null) {
					pallet = scheme.getLearningState().getPalletFromReplicas(pid);
				}
				
				if (pallet==null) {
					respond(callback, Reply.inconsistent(duty.getDuty(), 
							format("Pallet ID :%s not found or yet created", pid)));
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
							CommitTree.PLAN_NA);
					tmp.add(newone);					
				}
			}
		}
		
		final StringBuilder sb = new StringBuilder(tmp.size() * 5+1);
		scheme.getDirty().createCommitRequests(EntityEvent.CREATE, tmp, (duty, added)-> {
			if (added) {
				if (logger.isInfoEnabled()) {
					sb.append(duty).append(',');
				}
				respond(callback, Reply.success(duty, true));
			} else {
				respond(callback, Reply.alreadySubmitted(duty));
			}
		}, respondState);
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
    			respond(callback, Reply.notFound(pallet.getPallet()));
    		} else {
    		    final boolean done = scheme.addCrudPallet(pallet);
    		    respond(callback, done ? 
    		    		Reply.success(pallet.getPallet(), true) 
    		    		: Reply.notFound(pallet.getPallet()));
    		}
	    }
	}
	
	/** pallets directly from client */
	public boolean loadRawPallets(final Collection<Pallet> coll, final Consumer<Reply> callback) {
		return saveAllPallets(coll.stream().map(x-> toEntity(x)).collect(Collectors.toList()), callback);
	}
	/** @return TRUE when at least 1 pallet was loaded */
	public boolean saveAllPallets(final Collection<ShardEntity> coll, final Consumer<Reply> callback) {
		boolean any = false;
		for (ShardEntity p: coll) {
    		final ShardEntity already = scheme.getCommitedState().getPalletById(p.getEntity().getId());
    		if (already==null) {
    	        if (logger.isInfoEnabled()) {
    	            logger.info("{}: Adding New Pallets: {} with Balancer: {}", classname, p.getPallet(), 
    	                p.getPallet().getMetadata());
    	        }
    	        final boolean added = scheme.addCrudPallet(p);
    	        respond(callback, Reply.success(p.getEntity(), added));
    	        any |=added;
    		} else {
    			respond(callback, Reply.alreadyExists(p.getEntity()));
    		}
		}
		return any;
	}
	
	private boolean presentInPartition(final ShardEntity duty) {
		final Shard shardLocation = scheme.getCommitedState().findDutyLocation(duty.getDuty());
		boolean ret = shardLocation != null && shardLocation.getState().isAlive();
		if (!ret) {
			if (scheme.getCurrentPlan()!=null) {
				ret = scheme.getCurrentPlan().hasMigration(duty.getDuty());
			}
		}
		return ret;
	}

}
