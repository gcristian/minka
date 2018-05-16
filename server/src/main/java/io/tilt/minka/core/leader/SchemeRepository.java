package io.tilt.minka.core.leader;

import static io.tilt.minka.domain.EntityEvent.CREATE;
import static io.tilt.minka.domain.EntityEvent.REMOVE;
import static io.tilt.minka.domain.EntityState.PREPARED;

import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.tilt.minka.api.Duty;
import io.tilt.minka.api.Entity;
import io.tilt.minka.api.Pallet;
import io.tilt.minka.api.Reply;
import io.tilt.minka.api.ReplyResult;
import io.tilt.minka.core.leader.distributor.ChangePlan;
import io.tilt.minka.domain.EntityEvent;
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

	public void savePallets(final Collection<Pallet<?>> pallets) {
		List<String> skipped = null;
		for (Pallet<?> p: pallets) {
			final Reply res = savePallet(toCreatedEntity(p));
			if (res.getCause()!=ReplyResult.SUCCESS) {
				if (skipped==null) {
					skipped = new LinkedList<>();
				}
				skipped.add(res.toString());
			}
		}
		if (skipped!=null && logger.isInfoEnabled()) {
			logger.info("{}: Skipping Pallet CRUD already in scheme: {}", classname, skipped);
		}
	}

	public void saveDuties(final Collection<Duty<?>> duities) {
		List<String> skipped = null;;
		for (Duty<?> duty: duities) {
			final Reply res = saveDuty(toCreatedEntity(duty));
			if (res.getCause()!=ReplyResult.SUCCESS) {
				if (skipped==null) {
					skipped = new LinkedList<>();
				}
				skipped.add(res.toString());
			}
		}
		if (skipped!=null && logger.isInfoEnabled()) {
			logger.info("{}: Skipping Duty CRUD already in PTable: {}", classname, skipped);
		}
	}

	public static void main(String[] args) throws InterruptedException {
		while(true) {
			Thread.sleep(1000l);
			System.out.println(System.currentTimeMillis());
		}
	}
	
	private ShardEntity toCreatedEntity(final Entity<?> e) {
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

	public Reply removeDuty(final ShardEntity duty) {
		Reply ret = null;
		final ShardEntity current  = scheme.getScheme().getByDuty(duty.getDuty());
		if (current!=null) {
			final boolean added = scheme.getBackstage().addCrudDuty(duty);
			if (added) {
				ret = new Reply(ReplyResult.SUCCESS, duty.getDuty(), PREPARED, REMOVE, null);
			} else {
				ret = new Reply(ReplyResult.SUCCESS_OPERATION_ALREADY_SUBMITTED, 
						duty.getDuty(), null, EntityEvent.REMOVE, 
						String.format("%s: Added already !: %s", classname, duty.getDuty()));
			}
		} else {
			ret = new Reply(ReplyResult.ERROR_ENTITY_NOT_FOUND, duty.getDuty(), null, null, 
					String.format("%s: Deletion request not found on scheme: %s", classname, duty.getDuty()));
		}
		return ret;
	}

	public Reply saveDuty(final ShardEntity duty) {
		Reply ret = null;
		if (presentInPartition(duty)) {
			ret = new Reply(ReplyResult.ERROR_ENTITY_ALREADY_EXISTS, duty.getDuty(), null, null, null);
		} else {
			final ShardEntity pallet = scheme.getScheme().getPalletById(duty.getDuty().getPalletId());
			if (pallet!=null) {
				final ShardEntity newone = ShardEntity.Builder
						.builder(duty.getDuty())
						.withRelatedEntity(pallet)
						.build();
				newone.getJournal().addEvent(
						EntityEvent.CREATE, 
						EntityState.PREPARED, 
						this.shardId,
						ChangePlan.PLAN_WITHOUT);
				if (scheme.getBackstage().addCrudDuty(newone)) {
					if (logger.isInfoEnabled()) {
						logger.info("{}: Adding New Duty: {}", classname, newone);
					}
					ret = new Reply(ReplyResult.SUCCESS, duty.getDuty(), PREPARED, CREATE, null);
				} else {
					ret = new Reply(ReplyResult.SUCCESS_OPERATION_ALREADY_SUBMITTED, duty.getDuty(), null, 
							EntityEvent.CREATE, String.format("%s: Added already !: %s", classname, duty));
				}
			} else {
				ret = new Reply(ReplyResult.ERROR_ENTITY_INCONSISTENT, duty.getDuty(), null, null, 
						String.format("%s: Skipping Crud Event %s: Pallet ID :%s set not found or yet created", classname,
							EntityEvent.CREATE, null, duty.getDuty().getPalletId()));
			}
		}
		return ret;
	}


	////////////////////////// PALLETS
	
	public Reply removePallet(final ShardEntity pallet) {
		Reply reply = null;
		final ShardEntity p = scheme.getScheme().getPalletById(pallet.getEntity().getId());
		if (p==null) {
			reply = new Reply(ReplyResult.ERROR_ENTITY_NOT_FOUND, pallet.getEntity(), null, null, 
					String.format("%s: Skipping remove not found in Scheme: %s", 
							getClass().getSimpleName(), pallet.getEntity().getId()));
		} else {
			final boolean original = scheme.addCrudPallet(pallet);
			reply = new Reply(original ? ReplyResult.SUCCESS : ReplyResult.SUCCESS_OPERATION_ALREADY_SUBMITTED, 
				pallet.getEntity(), PREPARED, REMOVE, null);
		}
		return reply;
	}

	public Reply savePallet(final ShardEntity p) {
		Reply reply = null;
		final ShardEntity already = scheme.getScheme().getPalletById(p.getEntity().getId());
		if (already==null) {
			if (logger.isInfoEnabled()) {
				logger.info("{}: Adding New Pallet: {} with Balancer: {}", classname, p.getPallet(), 
					p.getPallet().getMetadata());
			}
			final boolean added = scheme.addCrudPallet(p);
			reply = new Reply(added ? ReplyResult.SUCCESS : ReplyResult.SUCCESS_OPERATION_ALREADY_SUBMITTED, 
					p.getEntity(), null, EntityEvent.CREATE, 
					String.format("%s: Added %s: %s", classname, added ? "": "already", p.getPallet()));
		} else {
			reply = new Reply(ReplyResult.ERROR_ENTITY_ALREADY_EXISTS, p.getEntity(), null, EntityEvent.CREATE, 
					String.format("%s: Skipping creation already in Scheme: %s", 
							getClass().getSimpleName(), p.getEntity().getId()));
		}
		return reply;
	}
	
	private boolean presentInPartition(final ShardEntity duty) {
		final Shard shardLocation = scheme.getScheme().findDutyLocation(duty.getDuty());
		return shardLocation != null && shardLocation.getState().isAlive();
	}

	private boolean presentInPartition(final Duty<?> duty) {
		final Shard shardLocation = scheme.getScheme().findDutyLocation(duty);
		return shardLocation != null && shardLocation.getState().isAlive();
	}

}
