package io.tilt.minka.core.leader.distributor;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.Consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.tilt.minka.api.Config;
import io.tilt.minka.api.Duty;
import io.tilt.minka.api.DutyBuilder.Task;
import io.tilt.minka.api.Pallet;
import io.tilt.minka.api.Reply;
import io.tilt.minka.core.leader.data.CrudController;
import io.tilt.minka.core.leader.data.Scheme;
import io.tilt.minka.domain.DependencyPlaceholder;
import io.tilt.minka.domain.ShardEntity;
import io.tilt.minka.shard.Shard;

/**
 * Initialization of the distributor phase. One-time execution only, or configured to run frequently.
 * Load of known replicas (when in presence of a leader reelection) 
 * and initial user duties if any.
 */
public class PhaseLoader {

	private final Logger logger = LoggerFactory.getLogger(getClass());
	
	private final CrudController crudController;
	private final DependencyPlaceholder dependencyPlaceholder;
	private final Config config;
	private final Scheme scheme;
	
	private boolean delegateFirstCall = true;
	private int counterForReloads;

	PhaseLoader(
			final CrudController crudController, 
			final DependencyPlaceholder dependencyPlaceholder,
			final Config config, 
			final Scheme scheme) {
		super();
		this.crudController = crudController;
		this.dependencyPlaceholder = dependencyPlaceholder;
		this.config = config;
		this.scheme = scheme;
	}

	/** @return if distribution can continue, read from storage only first time */
	boolean loadDutiesOnClusterStable() {
	    final boolean reload = !delegateFirstCall && (
	    		config.getDistributor().isReloadDutiesFromStorage()
                && config.getDistributor().getDutiesReloadFromStoragePhaseFrequency() == counterForReloads++);
	    
	    boolean ret = true;
	    
		if (delegateFirstCall || reload) {
		    counterForReloads = 0;
			logger.info("{}: reloading duties from storage", getClass().getSimpleName());
			
			if (loadPallets()) {
				loadDuties();
				final int size = scheme.getDirty().getSize();
				if (size>0) {
					logger.warn("{}: Aborting first distribution (no CRUD duties)", getClass().getSimpleName());
					ret = false;
				} else {
					logger.info("{}: reported {} entities for sharding...", getClass().getSimpleName(), size);
				}
			}
			delegateFirstCall = false;
		} else {
			checkUnexistingDutiesFromStorage();
		}
		return ret;
	}

	private void loadDuties() {
		final Set<Duty> duties = reloadDutiesFromUser();
		if (duties == null || duties.isEmpty()) {
			logger.warn("{}: EventMapper user's supplier hasn't return any duties: {}",
					getClass().getSimpleName(), duties);
		} else {
			try {
				duties.forEach(d -> Task.validateBuiltParams(d));
			} catch (Exception e) {
				logger.error("{}: Distribution suspended - Duty Built construction problem: ", 
						getClass().getSimpleName(), e);
				delegateFirstCall = false;
			}
		}		
		for (final Map.Entry<Shard, Set<ShardEntity>> e: scheme.getLearningState().getReplicasByShard().entrySet()) {
			scheme.getCommitedState().loadReplicas(e.getKey(), e.getValue());
		}
		crudController.loadRawDuties(duties, logger("Duty"));
	}

	private boolean loadPallets() {
		Set<Pallet> pallets = reloadPalletsFromUser();
		if (!scheme.getLearningState().isEmpty()) {
			if (pallets == null || pallets.isEmpty()) {
				pallets = new HashSet<>();
			}
			// current ones have precedence over user delegates
			// just because I dont care about this right now.
			// pallets are not being treated as a domain-1stCitizen YET
			pallets.addAll(scheme.getLearningState().collectPallets());
		}
		if (pallets == null || pallets.isEmpty()) {
			logger.warn("{}: EventMapper user's supplier hasn't return any pallets {}",
					getClass().getSimpleName(), pallets);
			return false;
		} else {
			return crudController.loadRawPallets(pallets, logger("Pallet"));
		}
	}

	private Set<Duty> reloadDutiesFromUser() {
		Set<Duty> duties = null;
		try {
			duties = dependencyPlaceholder.getMaster().loadDuties();
		} catch (Exception e) {
			logger.error("{}: throwed an Exception", getClass().getSimpleName(), e);
		}
		return duties;
	}

	private Set<Pallet> reloadPalletsFromUser() {
		Set<Pallet> pallets = null;
		try {
			pallets = dependencyPlaceholder.getMaster().loadPallets();
		} catch (Exception e) {
			logger.error("{}: throwed an Exception", getClass().getSimpleName(), e);
		}
		return pallets;
	}
	private Consumer<Reply> logger(final String type) {
		return (reply)-> {
			if (!reply.isSuccess()) {
				logger.info("{}: Skipping {} CRUD {} cause: {}", getClass().getSimpleName(), type, reply.getEntity(), reply.getValue());
			}
		};
	}

	/** feed missing duties with storage/scheme diff. */
	private void checkUnexistingDutiesFromStorage() {
		if (config.getDistributor().isRunConsistencyCheck() && scheme.getCurrentPlan().dispatchesEmpty()) {
			// only warn in case there's no reallocation ahead
			final Set<ShardEntity> sorted = new TreeSet<>();
			for (Duty duty: reloadDutiesFromUser()) {
				final ShardEntity entity = ShardEntity.Builder.builder(duty).build();
				if (!scheme.getCommitedState().dutyExists(entity)) {
					sorted.add(entity);
				}
			}
			if (!sorted.isEmpty()) {
				logger.error("{}: Consistency check: Absent duties going as Missing [ {}]", getClass().getSimpleName(),
						ShardEntity.toStringIds(sorted));
				scheme.getDirty().addMissing(sorted);
			}
		}
	}

}
