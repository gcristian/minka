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
import java.util.TreeSet;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.tilt.minka.api.Duty;
import io.tilt.minka.api.Entity;
import io.tilt.minka.api.Pallet;
import io.tilt.minka.api.Reply;
import io.tilt.minka.api.ReplyResult;
import io.tilt.minka.core.leader.distributor.ChangePlan;
import io.tilt.minka.domain.EntityEvent;
import io.tilt.minka.domain.EntityJournal.Log;
import io.tilt.minka.domain.EntityState;
import io.tilt.minka.domain.Shard;
import io.tilt.minka.domain.ShardEntity;
import io.tilt.minka.domain.ShardIdentifier;

public class SchemeRepository {

	private final Logger logger = LoggerFactory.getLogger(getClass());
	private final String classname = getClass().getSimpleName();

	private final ShardingScheme scheme;
	private final ShardIdentifier shardId;

	public SchemeRepository(final ShardingScheme shardingScheme, final ShardIdentifier shardId) {
		super();
		this.scheme = shardingScheme;
		this.shardId = shardId;
	}

	public Collection<Duty<?>> getDuties() {
		final List<Duty<?>> tmp = new LinkedList<>();
		this.scheme.getScheme().findDuties(d->tmp.add(d.getDuty()));
		return tmp;
	}

	public Collection<Pallet<?>> getPallets() {
		final List<Pallet<?>> tmp = new LinkedList<>();
		this.scheme.getScheme().findDuties(d->tmp.add(d.getPallet()));
		return tmp;
	}

	private ShardEntity toEntity(final Entity<?> e) {
		final ShardEntity.Builder builder = ShardEntity.Builder.builder(e);
		final ShardEntity entity = builder.build();
		entity.getJournal().addEvent(
				EntityEvent.CREATE, 
				EntityState.PREPARED, 
				this.shardId,
				ChangePlan.PLAN_WITHOUT);
		return entity;
	}

	////////////////////////// DUTIES

	public void removeAllDuties(final Collection<ShardEntity> coll, final Consumer<Reply> callback) {
		final List<ShardEntity> tmp = new ArrayList<>(coll.size());
		for (ShardEntity se : coll) {
			final ShardEntity current = scheme.getScheme().getByDuty(se.getDuty());
			if (current != null) {
				tmp.add(current);
			} else {
				tryCallback(callback, new Reply(ReplyResult.ERROR_ENTITY_NOT_FOUND, se.getDuty(), null, null,
						String.format("%s: Deletion request not found on scheme: %s", classname, se.getDuty())));
			}
		}

		scheme.getBackstage().addAllCrudDuty(tmp, (duty, added) -> {
			if (added) {
				tryCallback(callback, new Reply(ReplyResult.SUCCESS, duty, PREPARED, REMOVE, null));
			} else {
				tryCallback(callback, new Reply(ReplyResult.SUCCESS_OPERATION_ALREADY_SUBMITTED, duty, null,
						EntityEvent.REMOVE, String.format("%s: Added already !: %s", classname, duty)));
			}
		});
	}

	public void saveAllDutiesRaw(final Collection<Duty<?>> coll, final Consumer<Reply> callback) {
		saveAllDuties(coll.stream().map(x-> toEntity(x)).collect(Collectors.toList()), callback);
	}
	
	public void tryCallback(final Consumer<Reply> callback, final Reply reply) {
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
				ret = new Reply(ReplyResult.ERROR_ENTITY_ALREADY_EXISTS, duty.getDuty(), null, null, null);
				tryCallback(callback, ret);
			} else {
				final ShardEntity pallet = scheme.getScheme().getPalletById(duty.getDuty().getPalletId());
				if (pallet==null) {
					tryCallback(callback, new Reply(ReplyResult.ERROR_ENTITY_INCONSISTENT, duty.getDuty(), null, null, 
							String.format("%s: Skipping Crud Event %s: Pallet ID :%s set not found or yet created", classname,
								EntityEvent.CREATE, null, duty.getDuty().getPalletId())));
				} else {
					final ShardEntity newone = ShardEntity.Builder
							.builder(duty.getDuty())
							.withRelatedEntity(pallet)
							.build();
					// decorate with origin source (only for tracking purposes)
					copyOrigin(duty, newone);
					newone.getJournal().addEvent(
							EntityEvent.CREATE, 
							EntityState.PREPARED, 
							this.shardId,
							ChangePlan.PLAN_WITHOUT);
					tmp.add(newone);					
				}
			}
		}
		
		scheme.getBackstage().addAllCrudDuty(tmp, (duty, added)-> {
			if (added) {
				if (logger.isInfoEnabled()) {
					logger.info("{}: Adding New Duty: {}", classname, duty);
				}
				tryCallback(callback, new Reply(ReplyResult.SUCCESS, duty, PREPARED, CREATE, null));
			} else {
				tryCallback(callback, new Reply(ReplyResult.SUCCESS_OPERATION_ALREADY_SUBMITTED, duty, null, 
						EntityEvent.CREATE, String.format("%s: Added already !: %s", classname, duty)));
			}
		});
	}

	private void copyOrigin(ShardEntity source, final ShardEntity target) {
		final Log origin = source.getJournal().getLast();
		final ShardIdentifier originId = fromShardId(origin.getTargetId());
		if (originId!=null) {
			target.getJournal().addEvent(
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
		scheme.getScheme().findShards(null, s->{
			if (s.getShardID().getId().equals(id)) {
				ret[0] = s.getShardID();
			}
		});
		return ret[0];
	}

	////////////////////////// PALLETS
	
	public void removeAllPallet(final Collection<ShardEntity> coll, final Consumer<Reply> callback) {
	    for (ShardEntity pallet: coll) {
	        final ShardEntity p = scheme.getScheme().getPalletById(pallet.getEntity().getId());
    		if (p==null) {
    			tryCallback(callback, new Reply(ReplyResult.ERROR_ENTITY_NOT_FOUND, pallet.getEntity(), null, null, 
    					String.format("%s: Skipping remove not found in Scheme: %s", 
    							getClass().getSimpleName(), pallet.getEntity().getId())));
    		} else {
    		    final boolean original = scheme.addCrudPallet(pallet);
    		    tryCallback(callback, new Reply(original ? ReplyResult.SUCCESS : ReplyResult.SUCCESS_OPERATION_ALREADY_SUBMITTED, 
                    pallet.getEntity(), PREPARED, REMOVE, null));
    		}
	    }
	}
	public void saveAllPalletsRaw(final Collection<Pallet<?>> coll, final Consumer<Reply> callback) {
		saveAllPallets(coll.stream().map(x-> toEntity(x)).collect(Collectors.toList()), callback);
	}
	public void saveAllPallets(final Collection<ShardEntity> coll, final Consumer<Reply> callback) {
		for (ShardEntity p: coll) {
    		final ShardEntity already = scheme.getScheme().getPalletById(p.getEntity().getId());
    		if (already==null) {
    	        if (logger.isInfoEnabled()) {
    	            logger.info("{}: Adding New Pallets: {} with Balancer: {}", classname, p.getPallet(), 
    	                p.getPallet().getMetadata());
    	        }
    	        final boolean added = scheme.addCrudPallet(p);
    	        tryCallback(callback, new Reply(added ? ReplyResult.SUCCESS : ReplyResult.SUCCESS_OPERATION_ALREADY_SUBMITTED, 
                        p.getEntity(), null, EntityEvent.CREATE, 
                        String.format("%s: Added %s: %s", classname, added ? "": "already", p.getPallet())));
    		} else {
    			tryCallback(callback, new Reply(ReplyResult.ERROR_ENTITY_ALREADY_EXISTS, p.getEntity(), null, EntityEvent.CREATE, 
                        String.format("%s: Skipping creation already in Scheme: %s", 
                                getClass().getSimpleName(), p.getEntity().getId())));
    		}
		}
		
	}
	
	private boolean presentInPartition(final ShardEntity duty) {
		final Shard shardLocation = scheme.getScheme().findDutyLocation(duty.getDuty());
		return shardLocation != null && shardLocation.getState().isAlive();
	}

}
