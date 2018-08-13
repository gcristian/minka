package io.tilt.minka.core.leader.distributor;

import java.util.Collection;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.Consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.tilt.minka.api.Config;
import io.tilt.minka.api.Duty;
import io.tilt.minka.api.Pallet;
import io.tilt.minka.api.Reply;
import io.tilt.minka.api.DutyBuilder.Task;
import io.tilt.minka.core.leader.data.Scheme;
import io.tilt.minka.core.leader.data.UncommitedRepository;
import io.tilt.minka.domain.DependencyPlaceholder;
import io.tilt.minka.domain.ShardEntity;

/**
 * Initialization of the distributor phase. One-time execution only, or configured to run frequently.
 * Load of known replicas (when in presence of a leader reelection) 
 * and initial user duties if any.
 */
public class PhaseLoader {

	private final Logger logger = LoggerFactory.getLogger(getClass());
	
	private final UncommitedRepository uncommitedRepository;
	private final DependencyPlaceholder dependencyPlaceholder;
	private final Config config;
	private final Scheme scheme;
	
	private boolean delegateFirstCall = true;
	private int counterForReloads;

	PhaseLoader(
			final UncommitedRepository uncommitedRepository, 
			final DependencyPlaceholder dependencyPlaceholder,
			final Config config, 
			final Scheme scheme) {
		super();
		this.uncommitedRepository = uncommitedRepository;
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
			final Set<Pallet> pallets = reloadPalletsFromStorage();
			final Set<Duty> duties = reloadDutiesFromStorage();
						
			if (pallets == null || pallets.isEmpty()) {
				logger.warn("{}: EventMapper user's supplier hasn't return any pallets {}",
						getClass().getSimpleName(), pallets);
			} else {
				uncommitedRepository.loadRawPallets(pallets, logger("Pallet"));
			}
			
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
					return false;
				}
			}		
			scheme.getCommitedState().loadReplicas(scheme.getLearningState().getReplicasByShard());
			uncommitedRepository.loadRawDuties(duties, logger("Duty"));
			delegateFirstCall = false;

			final Collection<ShardEntity> crudReady = scheme.getUncommited().getDutiesCrud();
			if (crudReady.isEmpty()) {
				logger.warn("{}: Aborting first distribution (no CRUD duties)", getClass().getSimpleName());
				ret = false;
			} else {
				logger.info("{}: reported {} entities for sharding...", getClass().getSimpleName(), crudReady.size());
			}
		} else {
			checkUnexistingDutiesFromStorage();
		}
		return ret;
	}

	private Set<Duty> reloadDutiesFromStorage() {
		Set<Duty> duties = null;
		try {
			duties = dependencyPlaceholder.getMaster().loadDuties();
		} catch (Exception e) {
			logger.error("{}: throwed an Exception", getClass().getSimpleName(), e);
		}
		return duties;
	}

	private Set<Pallet> reloadPalletsFromStorage() {
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
		if (config.getDistributor().isRunConsistencyCheck() && scheme.getCurrentPlan().areShippingsEmpty()) {
			// only warn in case there's no reallocation ahead
			final Set<ShardEntity> sorted = new TreeSet<>();
			for (Duty duty: reloadDutiesFromStorage()) {
				final ShardEntity entity = ShardEntity.Builder.builder(duty).build();
				if (!scheme.getCommitedState().dutyExists(entity)) {
					sorted.add(entity);
				}
			}
			if (!sorted.isEmpty()) {
				logger.error("{}: Consistency check: Absent duties going as Missing [ {}]", getClass().getSimpleName(),
						ShardEntity.toStringIds(sorted));
				scheme.getUncommited().addMissing(sorted);
			}
		}
	}

}
