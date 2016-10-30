/**
 *  Copyright (c) 2011-2015 Zauber S.A.  -- All rights reserved
 */

package io.tilt.minka.core.leader.distributor;

import static io.tilt.minka.domain.ShardEntity.State.PREPARED;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.lang.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.tilt.minka.api.Duty;
import io.tilt.minka.api.Pallet;
import io.tilt.minka.core.leader.PartitionTable;
import io.tilt.minka.domain.EntityEvent;
import io.tilt.minka.domain.Shard;
import io.tilt.minka.domain.ShardEntity;
import jersey.repackaged.com.google.common.collect.Sets;

/**
 * Receives transfers of {@linkplain Duty} clusters by {@linkplain Balancer}'s 
 * to perform over the {@linkplain Reallocation} object. 
 * 
 * @author Cristian Gonzalez
 * @since Oct 29, 2016
 */
public class Migrator {

	private final Logger logger = LoggerFactory.getLogger(getClass());
	
	private final PartitionTable table;
	private final Reallocation realloc;
	private final List<Transfer> transfers;
	
	public Migrator(final PartitionTable table, final Reallocation realloc) {
		super();
		this.table = table;
		this.realloc = realloc;
		this.transfers = new ArrayList<>();
	}
	
	private static class Transfer {
		private final Pallet<?> pallet;
		private final Shard shard;
		private final Set<ShardEntity> entities;
		public Transfer(Pallet<?> pallet, Shard shard, Set<ShardEntity> entities) {
			super();
			this.pallet = pallet;
			this.shard = shard;
			this.entities = entities;
		}
		public Pallet<?> getPallet() {
			return this.pallet;
		}
		public Shard getShard() {
			return this.shard;
		}
		public Set<ShardEntity> getEntities() {
			return this.entities;
		}
	}
	
	public void transfer(final Pallet<?> pallet, final Shard shard, final Set<ShardEntity> cluster) {
		Validate.notNull(pallet);
		Validate.notNull(shard);
		transfers.add(new Transfer(pallet, shard, Sets.newHashSet(cluster)));
	}
	
	public void execute() {
		Validate.notNull(table);
		for (final Transfer tr: transfers) {
			register(tr.getEntities(), tr.getShard(), 
					table.getDutiesByShard(tr.getPallet(), tr.getShard()));
		}
	}

	// TODO ojo que estoy overrwriteando lo que el Arranger mando como BORRADO
	private void register(final Set<ShardEntity> clusterSet, final Shard shard, final Set<ShardEntity> currents) {

		if (logger.isDebugEnabled()) {
			logger.debug("{}: cluster built {}", getClass().getSimpleName(), clusterSet);
			logger.debug("{}: currents at shard {} ", getClass().getSimpleName(), currents);
		}

		List<ShardEntity> detaching = clusterSet != null
				? currents.stream().filter(i -> !clusterSet.contains(i)).collect(Collectors.toList())
				: new ArrayList<>(currents);

		if (detaching.isEmpty() && logger.isDebugEnabled()) {
			logger.info("{}: Shard: {} has no Detachings (calculated are all already attached)",
					getClass().getSimpleName(), shard);
		}

		StringBuilder log = new StringBuilder();
		for (ShardEntity detach : detaching) {
			// copy because in latter cycles this will be assigned
			// so they're traveling different places
			final ShardEntity copy = ShardEntity.copy(detach);
			copy.registerEvent(EntityEvent.DETACH, PREPARED);
			realloc.addChange(shard, copy);
			log.append(copy.getEntity().getId()).append(", ");
		}
		if (log.length() > 0) {
			logger.info("{}: Detaching to shard: {}, duties: (#{}) {}", getClass().getSimpleName(), shard.getShardID(),
				detaching.size(), log.toString());
		}

		if (clusterSet != null) {
			final List<ShardEntity> attaching = clusterSet.stream().filter(i -> !currents.contains(i))
					.collect(Collectors.toList());

			if (attaching.isEmpty() && logger.isDebugEnabled()) {
				logger.info("{}: Shard: {} has no New Attachments (calculated are all already attached)",
						getClass().getSimpleName(), shard);
			}
			log = new StringBuilder();
			for (ShardEntity attach : attaching) {
				// copy because in latter cycles this will be assigned
				// so they're traveling different places
				final ShardEntity copy = ShardEntity.copy(attach);
				copy.registerEvent(EntityEvent.ATTACH, PREPARED);
				realloc.addChange(shard, copy);
				log.append(copy.getEntity().getId()).append(", ");
			}
			if (log.length() > 0) {
				logger.info("{}: Attaching to shard: {}, duty: (#{}) {}", getClass().getSimpleName(), shard.getShardID(),
					attaching.size(), log.toString());
			}
		}
	}
}
